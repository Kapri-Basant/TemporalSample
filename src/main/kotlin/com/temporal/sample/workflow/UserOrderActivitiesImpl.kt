package com.temporal.sample.workflow

import io.temporal.failure.ApplicationFailure
import io.temporal.activity.Activity
import org.slf4j.LoggerFactory

class UserOrderActivitiesImpl : UserOrderActivities {
    private val log = LoggerFactory.getLogger(javaClass)
    private val podName = System.getenv("POD_NAME") ?: "unknown-pod"
    private val podNamespace = System.getenv("POD_NAMESPACE") ?: "temporal-demo"

    // Imitate one order task and log the same key on every retry.
    override fun runStep(userId: String, stepName: String): String {
        val info = Activity.getExecutionContext().info
        // A real service must save this key with its result to prevent duplicate writes.
        val key = "${info.workflowId}:${info.runId}:$stepName"
        log.info("workflow={} run={} activity={} attempt={} key={} step={} simulated=true action=ACTIVITY_STARTED",
            info.workflowId, info.runId, info.activityId, info.attempt, key, stepName)
        log.info("pod={} namespace={} action=activity_started user={} step={} event=activity_received", podName, podNamespace, userId, stepName)
        if (userId.isBlank() || stepName !in setOf("STEP_1_VALIDATE_USER", "STEP_2_RESERVE_ITEM",
                "STEP_3_TAKE_PAYMENT", "STEP_4_PACK_BOX", "STEP_5_SEND_ORDER")) {
            throw ApplicationFailure.newNonRetryableFailure("Unknown step or empty user", "INVALID_ORDER")
        }
        // Only imitate work here. No payment or stock system is connected.
        Thread.sleep(3000)
        val result = "completed"
        log.info("pod={} namespace={} action=activity_result user={} step={} result={}", podName, podNamespace, userId, stepName, result)
        return "$podName:$podNamespace:$stepName:$result"
    }
}
