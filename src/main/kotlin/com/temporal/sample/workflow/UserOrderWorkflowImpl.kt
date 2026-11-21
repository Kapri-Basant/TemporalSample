package com.temporal.sample.workflow

import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import java.time.Duration
import java.time.Instant

class UserOrderWorkflowImpl : UserOrderWorkflow {
    private val activity = Workflow.newActivityStub(
        UserOrderActivities::class.java,
        ActivityOptions.newBuilder()
            // One try gets 20 seconds; all tries and queue waits get five minutes.
            .setStartToCloseTimeout(Duration.ofSeconds(20))
            .setScheduleToCloseTimeout(Duration.ofMinutes(5))
            .setRetryOptions(
                RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofSeconds(1))
                    .setBackoffCoefficient(2.0)
                    .setMaximumInterval(Duration.ofSeconds(10))
                    .setMaximumAttempts(5)
                    .build()
            )
            .build()
    )

    private var nowAt = "NOT_STARTED"
    private val doneSteps = mutableListOf<String>()
    private val timeline = mutableListOf<String>()
    private val log = Workflow.getLogger(UserOrderWorkflowImpl::class.java)

    // Run five steps; replay reuses their saved results after a restart.
    override fun run(userId: String, stepTimersSec: List<Int>): String {
        val safeTimers = normalizeTimers(stepTimersSec)
        addEvent("WORKFLOW_STARTED", "workflow accepted for $userId")
        doStep(userId, "STEP_1_VALIDATE_USER", safeTimers[0])
        doStep(userId, "STEP_2_RESERVE_ITEM", safeTimers[1])
        doStep(userId, "STEP_3_TAKE_PAYMENT", safeTimers[2])
        doStep(userId, "STEP_4_PACK_BOX", safeTimers[3])
        doStep(userId, "STEP_5_SEND_ORDER", safeTimers[4])
        nowAt = "DONE"
        addEvent("WORKFLOW_COMPLETED", "all 5 steps completed for $userId")
        return "done for $userId"
    }

    // Return the current step to a workflow query.
    override fun currentStep(): String = nowAt

    // Return a copy of the fully finished steps.
    override fun completedSteps(): List<String> = doneSteps.toList()

    // Return a copy of the workflow messages.
    override fun timeline(): List<String> = timeline.toList()

    // Run one activity, then wait using a durable Temporal timer.
    private fun doStep(userId: String, stepName: String, delaySec: Int) {
        nowAt = stepName
        addEvent("STEP_STARTED", stepName)
        addEvent("ACTIVITY_DISPATCHED", "$stepName sent to activity queue")
        val result = activity.runStep(userId, stepName)
        addEvent("ACTIVITY_COMPLETED", "$stepName result=$result")
        addEvent("WORKFLOW_TIMER_STARTED", "${stepName} wait=$delaySec seconds")
        Workflow.sleep(Duration.ofSeconds(delaySec.toLong()))
        addEvent("WORKFLOW_TIMER_FIRED", "${stepName} timer completed")
        doneSteps.add(stepName)
    }

    // Use saved workflow time and avoid duplicate console logs during replay.
    private fun addEvent(action: String, detail: String) {
        val event = "${Instant.ofEpochMilli(Workflow.currentTimeMillis())}|action=$action|detail=$detail"
        timeline.add(event)
        log.info(event)
    }

    // Keep old workflow inputs safe, with 20-second defaults.
    private fun normalizeTimers(stepTimersSec: List<Int>): List<Int> {
        val defaults = listOf(20, 20, 20, 20, 20)
        return defaults.indices.map { index ->
            val value = stepTimersSec.getOrNull(index) ?: defaults[index]
            value.coerceIn(3, 120)
        }
    }
}
