package com.temporal.sample

import com.temporal.sample.integration.*
import io.temporal.activity.Activity
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.failure.ApplicationFailure
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.testing.WorkflowReplayer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JobWorkflowTests {
    // A job saves results and timers, and replay never calls the task service again.
    @Test
    fun `successful job can replay without repeating outside calls`() {
        val calls = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // Record the task call instead of making a network request.
            override fun execute(call: TaskCall): TaskResult {
                calls.add(call.task)
                if (call.index == 1) assertEquals("receipt-0", call.previousReceipts["0"])
                return TaskResult("receipt-${call.index}", output = mapOf("task" to call.task), podName = "test-worker")
            }
            // A successful job must not undo its tasks.
            override fun undo(call: TaskCall): TaskResult = error("Unexpected undo")
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-success", activities)
            val result = workflow.run(JobInput("job-success", "owner", listOf(
                JobStep("check", waitSeconds = 20, sideEffect = false), JobStep("reserve", undo = true))))
            assertEquals("COMPLETED", result.status)
            assertEquals(listOf("check", "reserve"), calls.toList())
            assertTrue(result.steps.all { it.status == "COMPLETED" })
            assertEquals("reserve", result.steps[1].output["task"])
            val history = environment.workflowClient.fetchHistory("job-success")
            // Export a baseline for review; it is not silently written into source fixtures.
            val fixture = java.nio.file.Path.of("build/replay/JobWorkflowV1-success.json")
            java.nio.file.Files.createDirectories(fixture.parent)
            java.nio.file.Files.writeString(fixture, history.toJson(true))
            WorkflowReplayer.replayWorkflowExecution(history, JobWorkflowV1Impl::class.java)
            assertEquals(listOf("check", "reserve"), calls.toList())
        }
    }

    // Replay a reviewed V1 history so later code changes cannot silently break old jobs.
    @Test
    fun `reviewed V1 history still replays`() {
        val saved = javaClass.getResourceAsStream("/replay/JobWorkflowV1-success.json")
            ?: error("Reviewed V1 replay fixture is missing")
        val history = saved.bufferedReader().use { it.readText() }
        WorkflowReplayer.replayWorkflowExecution(
            io.temporal.common.WorkflowExecutionHistory.fromJson(history), JobWorkflowV1Impl::class.java)
    }

    // A temporary fault retries with the same business key rather than making a new order.
    @Test
    fun `retry keeps one stable business key`() {
        val keys = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // Lose the first reply and let Temporal retry the same logical task.
            override fun execute(call: TaskCall): TaskResult {
                keys.add(call.idempotencyKey)
                if (Activity.getExecutionContext().info.attempt == 1) {
                    throw ApplicationFailure.newFailure("Temporary outage", "TEMPORARY")
                }
                return TaskResult("saved-result")
            }
            // No undo is needed for this successful read-only task.
            override fun undo(call: TaskCall): TaskResult = error("Unexpected undo")
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-retry", activities)
            assertEquals("COMPLETED", workflow.run(JobInput("job-retry", "owner", listOf(JobStep("check", sideEffect = false)))).status)
            assertEquals(2, keys.size)
            assertEquals(1, keys.distinct().size)
            assertEquals(64, keys.first().length)
        }
    }

    // Registering cleanup first also handles a forward write whose reply was lost.
    @Test
    fun `failed task is undone before older tasks and cleanup failure needs attention`() {
        val calls = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // The second provider call fails after the outside write may have happened.
            override fun execute(call: TaskCall): TaskResult {
                calls.add("execute:${call.task}")
                if (call.task == "charge") throw ApplicationFailure.newNonRetryableFailure("Reply lost", "REJECTED")
                return TaskResult("receipt-${call.index}")
            }
            // Fail one undo so older dependent work stays untouched for review.
            override fun undo(call: TaskCall): TaskResult {
                calls.add("undo:${call.task}")
                if (call.task == "charge") throw ApplicationFailure.newNonRetryableFailure("Needs help", "UNDO_REJECTED")
                return TaskResult("undone-${call.index}")
            }
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-failed", activities)
            val result = workflow.run(JobInput("job-failed", "owner", listOf(
                JobStep("reserve", undo = true), JobStep("charge", undo = true), JobStep("ship"))))
            assertEquals("NEEDS_ATTENTION", result.status)
            assertEquals(listOf("execute:reserve", "execute:charge", "undo:charge"), calls.toList())
            assertEquals("COMPLETED", result.steps[0].status)
            assertEquals("NEEDS_ATTENTION", result.steps[1].status)
            assertEquals("PENDING", result.steps[2].status)
        }
    }

    // Clean up reversible tasks in the reverse order and keep their saved receipts.
    @Test
    fun `all reversible work is undone after a later read fails`() {
        val calls = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // A final read fails after two reversible writes have completed.
            override fun execute(call: TaskCall): TaskResult {
                calls.add("execute:${call.task}")
                if (call.task == "confirm") throw ApplicationFailure.newNonRetryableFailure("Rejected", "REJECTED")
                return TaskResult("receipt-${call.index}", output = mapOf("state" to "saved"))
            }
            // Require the current task's receipt before safely undoing it.
            override fun undo(call: TaskCall): TaskResult {
                assertEquals("receipt-${call.index}", call.previousReceipts[call.index.toString()])
                calls.add("undo:${call.task}")
                return TaskResult("undone-${call.index}")
            }
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-reverted", activities)
            val result = workflow.run(JobInput("job-reverted", "owner", listOf(
                JobStep("reserve", undo = true), JobStep("charge", undo = true), JobStep("confirm", sideEffect = false))))
            assertEquals("FAILED", result.status)
            assertEquals(listOf("execute:reserve", "execute:charge", "execute:confirm", "undo:charge", "undo:reserve"), calls.toList())
            assertEquals("UNDONE", result.steps[0].status)
            assertEquals("UNDONE", result.steps[1].status)
            assertEquals("saved", result.steps[1].output["state"])
        }
    }

    // Never refund or release older work when an irreversible task may have happened.
    @Test
    fun `irreversible task stops all automatic cleanup`() {
        val calls = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // Make a payment, ship, then fail a later read-only check.
            override fun execute(call: TaskCall): TaskResult {
                calls.add("execute:${call.task}")
                if (call.task == "confirm") throw ApplicationFailure.newNonRetryableFailure("No reply", "REJECTED")
                return TaskResult("receipt-${call.index}")
            }
            // A shipped item needs manual review before any refund is attempted.
            override fun undo(call: TaskCall): TaskResult {
                calls.add("undo:${call.task}")
                return TaskResult("undone")
            }
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-irreversible", activities)
            val result = workflow.run(JobInput("job-irreversible", "owner", listOf(
                JobStep("charge", undo = true), JobStep("ship"), JobStep("confirm", sideEffect = false))))
            assertEquals("NEEDS_ATTENTION", result.status)
            assertEquals(listOf("execute:charge", "execute:ship", "execute:confirm"), calls.toList())
            assertEquals("COMPLETED", result.steps[0].status)
            assertEquals("NEEDS_ATTENTION", result.steps[1].status)
        }
    }

    // A user cancel during a saved wait stops later work and undoes the first task.
    @Test
    fun `cancel signal runs cleanup outside the cancelled scope`() {
        val calls = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // Complete a fast task before the workflow enters its long wait.
            override fun execute(call: TaskCall): TaskResult {
                calls.add("execute:${call.task}")
                return TaskResult("receipt-${call.index}")
            }
            // Show that cleanup can still run after the work scope is cancelled.
            override fun undo(call: TaskCall): TaskResult {
                calls.add("undo:${call.task}")
                return TaskResult("undone-${call.index}")
            }
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-cancel", activities)
            WorkflowClient.start(workflow::run, JobInput("job-cancel", "owner", listOf(
                JobStep("reserve", waitSeconds = 3600, undo = true), JobStep("ship"))))
            environment.sleep(Duration.ofSeconds(1))
            workflow.cancel()
            val result = WorkflowStub.fromTyped(workflow).getResult(JobSnapshot::class.java)
            assertEquals("CANCELED", result.status)
            assertEquals(listOf("execute:reserve", "undo:reserve"), calls.toList())
        }
    }

    // A stop requested before the first task must not access a missing row.
    @Test
    fun `cancel before work starts returns a valid cancelled snapshot`() {
        val activities = object : JobActivities {
            // No outside work should be started for this early cancel.
            override fun execute(call: TaskCall): TaskResult = error("Unexpected execution")
            // No outside work means there is nothing to undo.
            override fun undo(call: TaskCall): TaskResult = error("Unexpected undo")
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-early-cancel", activities)
            val input = JobInput("job-early-cancel", "owner", listOf(JobStep("reserve", undo = true)))
            WorkflowStub.fromTyped(workflow).signalWithStart("cancel", emptyArray<Any>(), arrayOf(input))
            val result = WorkflowStub.fromTyped(workflow).getResult(JobSnapshot::class.java)
            assertEquals("CANCELED", result.status)
        }
    }

    // Heartbeats allow an activity to acknowledge cancellation before cleanup runs.
    @Test
    fun `cancel during an activity waits for cancellation then undoes it`() {
        val started = CountDownLatch(1)
        val calls = CopyOnWriteArrayList<String>()
        val activities = object : JobActivities {
            // Keep this attempt in flight until a heartbeat observes the cancel.
            override fun execute(call: TaskCall): TaskResult {
                calls.add("execute:${call.task}")
                started.countDown()
                while (true) {
                    Activity.getExecutionContext().heartbeat("running")
                    Thread.sleep(25)
                }
            }
            // The detached cleanup scope must still be able to call this activity.
            override fun undo(call: TaskCall): TaskResult {
                calls.add("undo:${call.task}")
                return TaskResult("undone")
            }
        }
        TestWorkflowEnvironment.newInstance().use { environment ->
            val workflow = worker(environment, "job-inflight-cancel", activities)
            WorkflowClient.start(workflow::run, JobInput("job-inflight-cancel", "owner", listOf(JobStep("reserve", undo = true))))
            assertTrue(started.await(5, TimeUnit.SECONDS), "Activity never started")
            workflow.cancel()
            val result = WorkflowStub.fromTyped(workflow).getResult(20, TimeUnit.SECONDS, JobSnapshot::class.java)
            assertEquals("CANCELED", result.status)
            assertEquals(listOf("execute:reserve", "undo:reserve"), calls.toList())
        }
    }

    // Use the actual workflow code with a local Temporal test server.
    private fun worker(environment: TestWorkflowEnvironment, id: String, activities: JobActivities): JobWorkflowV1 {
        val worker = environment.newWorker("integration-tests")
        worker.registerWorkflowImplementationTypes(JobWorkflowV1Impl::class.java)
        worker.registerActivitiesImplementations(activities)
        environment.start()
        return environment.workflowClient.newWorkflowStub(JobWorkflowV1::class.java,
            WorkflowOptions.newBuilder().setWorkflowId(id).setTaskQueue("integration-tests").build())
    }
}
