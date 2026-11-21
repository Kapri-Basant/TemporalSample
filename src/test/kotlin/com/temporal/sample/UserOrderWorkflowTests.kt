package com.temporal.sample

import com.temporal.sample.workflow.UserOrderActivities
import com.temporal.sample.workflow.UserOrderWorkflow
import com.temporal.sample.workflow.UserOrderWorkflowImpl
import io.temporal.activity.Activity
import io.temporal.api.enums.v1.EventType
import io.temporal.client.WorkflowFailedException
import io.temporal.client.WorkflowOptions
import io.temporal.failure.ApplicationFailure
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.testing.WorkflowReplayer
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class UserOrderWorkflowTests {
    private val stepNames = listOf(
        "STEP_1_VALIDATE_USER",
        "STEP_2_RESERVE_ITEM",
        "STEP_3_TAKE_PAYMENT",
        "STEP_4_PACK_BOX",
        "STEP_5_SEND_ORDER"
    )

    // Check the order and all five saved waits, then replay without repeating work.
    @Test
    fun `five steps and timers survive history replay`() {
        val calls = CopyOnWriteArrayList<String>()
        val activity = object : UserOrderActivities {
            // Record each call instead of contacting real order or payment services.
            override fun runStep(userId: String, stepName: String): String {
                calls.add(stepName)
                return "test-pod:test-namespace:$stepName:completed"
            }
        }
        val environment = TestWorkflowEnvironment.newInstance()
        try {
            val workflow = startWorker(environment, "test-order", activity)
            val before = environment.currentTimeMillis()
            assertEquals("done for user-1", workflow.run("user-1", List(5) { 20 }))
            assertEquals(stepNames, calls.toList())
            assertTrue(environment.currentTimeMillis() - before >= 100_000)
            val history = environment.workflowClient.fetchHistory("test-order")
            assertEquals(5, history.history.eventsList.count { it.eventType == EventType.EVENT_TYPE_TIMER_STARTED })
            assertEquals(5, history.history.eventsList.count { it.eventType == EventType.EVENT_TYPE_TIMER_FIRED })

            WorkflowReplayer.replayWorkflowExecution(history, UserOrderWorkflowImpl::class.java)
            assertEquals(stepNames, calls.toList())
        } finally {
            environment.close()
        }
    }

    // A short failure retries the same activity before moving to the next step.
    @Test
    fun `temporary failure retries with the same activity key`() {
        val attempts = CopyOnWriteArrayList<Pair<String, Int>>()
        val calls = CopyOnWriteArrayList<String>()
        val activity = object : UserOrderActivities {
            // Fail the first attempt and record the stable key used by both attempts.
            override fun runStep(userId: String, stepName: String): String {
                calls.add(stepName)
                if (stepName == stepNames.first()) {
                    val info = Activity.getExecutionContext().info
                    attempts.add("${info.workflowId}:${info.activityId}" to info.attempt)
                    if (info.attempt == 1) throw ApplicationFailure.newFailure("Try again", "TemporaryFailure")
                }
                return "test-pod:test-namespace:$stepName:completed"
            }
        }
        val environment = TestWorkflowEnvironment.newInstance()
        try {
            val workflow = startWorker(environment, "test-retry", activity)
            assertEquals("done for user-1", workflow.run("user-1", List(5) { 3 }))
            assertEquals(listOf(1, 2), attempts.map { it.second })
            assertEquals(1, attempts.map { it.first }.distinct().size)
            assertEquals(listOf(stepNames.first()) + stepNames, calls.toList())
        } finally {
            environment.close()
        }
    }

    // Bad business input stops the flow instead of retrying the same bad request.
    @Test
    fun `permanent failure is not retried and later steps do not run`() {
        val calls = AtomicInteger()
        val activity = object : UserOrderActivities {
            // Return a permanent error that another attempt cannot fix.
            override fun runStep(userId: String, stepName: String): String {
                calls.incrementAndGet()
                throw ApplicationFailure.newNonRetryableFailure("User was rejected", "UserRejected")
            }
        }
        val environment = TestWorkflowEnvironment.newInstance()
        try {
            val workflow = startWorker(environment, "test-rejected", activity)
            assertFailsWith<WorkflowFailedException> { workflow.run("user-1", List(5) { 3 }) }
            assertEquals(1, calls.get())
            val history = environment.workflowClient.fetchHistory("test-rejected")
            assertTrue(history.history.eventsList.any { it.eventType == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED })
            assertTrue(history.history.eventsList.none { it.eventType == EventType.EVENT_TYPE_TIMER_STARTED })
        } finally {
            environment.close()
        }
    }

    // Start the real workflow code with a local test service and a small fake activity.
    private fun startWorker(
        environment: TestWorkflowEnvironment,
        workflowId: String,
        activity: UserOrderActivities
    ): UserOrderWorkflow {
        val queue = "test-order-queue"
        val worker = environment.newWorker(queue)
        worker.registerWorkflowImplementationTypes(UserOrderWorkflowImpl::class.java)
        worker.registerActivitiesImplementations(activity)
        environment.start()
        return environment.workflowClient.newWorkflowStub(
            UserOrderWorkflow::class.java,
            WorkflowOptions.newBuilder().setTaskQueue(queue).setWorkflowId(workflowId).build()
        )
    }
}
