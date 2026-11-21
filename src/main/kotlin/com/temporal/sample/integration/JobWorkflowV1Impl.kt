package com.temporal.sample.integration

import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.CancellationScope
import io.temporal.workflow.Workflow
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration

class JobWorkflowV1Impl : JobWorkflowV1 {
    private val activities = Workflow.newActivityStub(JobActivities::class.java,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(30))
            .setScheduleToCloseTimeout(Duration.ofMinutes(5))
            .setHeartbeatTimeout(Duration.ofSeconds(10))
            .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
            .setRetryOptions(RetryOptions.newBuilder()
                .setInitialInterval(Duration.ofSeconds(1))
                .setMaximumInterval(Duration.ofSeconds(10))
                .setMaximumAttempts(5).build())
            .build())
    private val log = Workflow.getLogger(JobWorkflowV1Impl::class.java)
    private var state = JobSnapshot()
    private var cancelRequested = false
    private var work: CancellationScope? = null
    private val touched = mutableListOf<Pair<JobStep, TaskCall>>()

    // Keep outside work in activities and keep all workflow choices replay-safe.
    override fun run(input: JobInput): JobSnapshot {
        if (input.jobId.isBlank() || input.ownerId.isBlank() || input.steps.size !in 1..20 ||
            input.steps.any { it.task.isBlank() || it.waitSeconds !in 0..86400 }) {
            throw io.temporal.failure.ApplicationFailure.newNonRetryableFailure("Invalid job input", "INVALID_JOB")
        }
        state = JobSnapshot(input.jobId, input.ownerId, "RUNNING", steps = input.steps.mapIndexed { index, step ->
            JobStepState(index, step.task)
        })
        log.info("workflow={} action=JOB_STARTED status=RUNNING", Workflow.getInfo().workflowId)
        work = Workflow.newCancellationScope(Runnable {
            for ((index, step) in input.steps.withIndex()) {
                if (cancelRequested) throw CanceledFailure("Cancel requested")
                state = state.copy(currentStep = index, status = "RUNNING")
                val call = TaskCall(input.jobId, input.ownerId, index, step.task, step.input,
                    key(input, index, step.task, "execute"), receipts())
                // Register first: a failed reply does not prove that the provider did no work.
                touched.add(step to call)
                setStep(index, "RUNNING")
                val result = activities.execute(call)
                setStep(index, "ACTIVITY_COMPLETED", result)
                if (step.waitSeconds > 0) {
                    state = state.copy(status = "WAITING")
                    setStep(index, "WAITING")
                    Workflow.await(Duration.ofSeconds(step.waitSeconds.toLong())) { cancelRequested }
                    if (cancelRequested) throw CanceledFailure("Cancel requested")
                }
                setStep(index, "COMPLETED", result)
            }
        })
        return try {
            if (cancelRequested) work!!.cancel("Cancel requested")
            work!!.run()
            state = state.copy(status = "COMPLETED")
            finish()
        } catch (_: CanceledFailure) {
            setStep(state.currentStep, "CANCELED", error = "Cancel requested")
            compensate(input, "CANCELED", "Cancel requested")
        } catch (failure: ActivityFailure) {
            setStep(state.currentStep, "FAILED", error = "Task failed; inspect the saved activity failure")
            if (cancelRequested || hasCancelCause(failure)) compensate(input, "CANCELED", "Cancel requested")
            else compensate(input, "FAILED", "Task failed; inspect the saved activity failure")
        }
    }

    // Return one snapshot that callers can show without merging several queries.
    override fun snapshot(): JobSnapshot = state.copy(steps = state.steps.toList())

    // Stop current work; cleanup runs in a separate scope that cannot be cancelled.
    override fun cancel() {
        if (state.status in setOf("COMPLETED", "FAILED", "CANCELED", "NEEDS_ATTENTION")) return
        cancelRequested = true
        work?.cancel("Cancel requested")
    }

    // Stop cleanup at the first unsafe point; older refunds may depend on this undo.
    private fun compensate(input: JobInput, finalStatus: String, error: String): JobSnapshot {
        // An irreversible operation may still be in flight, even if its reply failed.
        val barriers = touched.filter { (step, _) -> step.sideEffect && !step.undo }
        if (barriers.isNotEmpty()) {
            for ((_, call) in barriers) {
                setStep(call.index, "NEEDS_ATTENTION", error = "Irreversible work may have happened; review before undoing earlier tasks")
            }
            state = state.copy(status = "NEEDS_ATTENTION", error = "Manual review required before undoing irreversible work")
            return finish()
        }
        var needsAttention = false
        state = state.copy(status = "COMPENSATING", error = error)
        log.info("workflow={} action=CLEANUP_STARTED status=COMPENSATING", Workflow.getInfo().workflowId)
        Workflow.newDetachedCancellationScope {
            for ((step, call) in touched.asReversed()) {
                if (!step.sideEffect) continue
                setStep(call.index, "UNDOING")
                try {
                    activities.undo(call.copy(idempotencyKey = key(input, call.index, step.task, "undo"), previousReceipts = receipts()))
                    setStep(call.index, "UNDONE")
                } catch (_: ActivityFailure) {
                    needsAttention = true
                    setStep(call.index, "NEEDS_ATTENTION", error = "Undo failed; older tasks are kept for manual review")
                    break
                }
            }
        }.run()
        state = state.copy(status = if (needsAttention) "NEEDS_ATTENTION" else finalStatus)
        return finish()
    }

    // Count business outcomes separately from Temporal execution status; replay skips this metric.
    private fun finish(): JobSnapshot {
        Workflow.getMetricsScope().tagged(mapOf("outcome" to state.status))
            .counter("job_business_result").inc(1)
        log.info("workflow={} action=JOB_FINISHED status={}", Workflow.getInfo().workflowId, state.status)
        return state
    }

    // Change just one row and preserve its saved receipt when undoing it.
    private fun setStep(index: Int, status: String, result: TaskResult? = null, error: String? = null) {
        if (index !in state.steps.indices) return
        val rows = state.steps.toMutableList()
        val before = rows[index]
        rows[index] = before.copy(status = status, receipt = result?.receipt ?: before.receipt,
            podName = result?.podName ?: before.podName, error = error, output = result?.output ?: before.output)
        state = state.copy(steps = rows)
        // The workflow logger skips replay; no task input or receipt is written here.
        log.info("workflow={} step={} task={} action=STEP_STATE status={}",
            Workflow.getInfo().workflowId, index, before.task, status)
    }

    // Give later steps and undo calls the small receipts saved by earlier work.
    private fun receipts(): Map<String, String> = state.steps.mapNotNull { step ->
        step.receipt?.let { step.index.toString() to it }
    }.toMap()

    // Use a business key that stays the same after retries, resets, or a new run.
    private fun key(input: JobInput, index: Int, task: String, direction: String): String {
        val parts = listOf(input.ownerId, input.jobId, index.toString(), task, direction)
        val value = parts.joinToString("") { "${it.length}:$it" }
        return MessageDigest.getInstance("SHA-256").digest(value.toByteArray(StandardCharsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
    }

    // A native Temporal cancel can reach an activity wrapped in ActivityFailure.
    private fun hasCancelCause(failure: Throwable): Boolean =
        generateSequence(failure) { it.cause }.any { it is CanceledFailure }
}
