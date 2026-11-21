package com.temporal.sample

import io.temporal.api.history.v1.HistoryEvent
import io.temporal.common.converter.DefaultDataConverter
import java.time.Instant

// Read saved events without asking a worker to run a query.
object HistoryStatus {
    // Turn recorded events into row states and a full timeline.
    fun read(workflowId: String, events: List<HistoryEvent>): WorkflowStatusResponse {
        val steps = mutableMapOf<Long, String>()
        val done = mutableListOf<String>()
        val activitiesDone = mutableListOf<String>()
        val timeline = mutableListOf<String>()
        val stepPods = mutableMapOf<String, String>()
        val stepStates = mutableMapOf<String, String>()
        val timers = mutableMapOf<String, String>()
        val runId = events.firstOrNull { it.hasWorkflowExecutionStartedEventAttributes() }
            ?.workflowExecutionStartedEventAttributes?.originalExecutionRunId ?: ""
        var current = "NOT_STARTED"
        var pod = "unknown"
        val converter = DefaultDataConverter.STANDARD_INSTANCE
        for (event in events) {
            var detail = ""
            var eventPod = "not-applicable"
            when {
                event.hasWorkflowExecutionStartedEventAttributes() -> current = "STARTED"
                event.hasWorkflowTaskStartedEventAttributes() -> {
                    pod = event.workflowTaskStartedEventAttributes.identity.ifBlank { "unknown" }
                    eventPod = pod
                }
                event.hasActivityTaskScheduledEventAttributes() -> {
                    val input = event.activityTaskScheduledEventAttributes.input
                    val step = converter.fromPayload(input.getPayloads(1), String::class.java, String::class.java)
                    steps[event.eventId] = step
                    current = step
                    stepStates[step] = "SCHEDULED"
                    detail = "step=$step"
                }
                event.hasActivityTaskStartedEventAttributes() -> {
                    val activity = event.activityTaskStartedEventAttributes
                    pod = activity.identity.ifBlank { "unknown" }
                    eventPod = pod
                    steps[activity.scheduledEventId]?.let { stepPods[it] = pod; stepStates[it] = "ACTIVITY_RUNNING" }
                    detail = "step=${steps[activity.scheduledEventId]} attempt=${activity.attempt}"
                }
                event.hasActivityTaskCompletedEventAttributes() -> {
                    val activity = event.activityTaskCompletedEventAttributes
                    steps[activity.scheduledEventId]?.let { if (it !in activitiesDone) activitiesDone.add(it) }
                    steps[activity.scheduledEventId]?.let {
                        stepStates[it] = "ACTIVITY_COMPLETED"
                        // Activity results include the pod that did the work, even after retries.
                        if (activity.result.payloadsCount > 0) {
                            val result = converter.fromPayload(activity.result.getPayloads(0), String::class.java, String::class.java)
                            if (result.contains(":")) stepPods[it] = result.substringBefore(":")
                        }
                        eventPod = stepPods[it] ?: "unknown"
                    }
                    detail = "step=${steps[activity.scheduledEventId]} result saved"
                }
                event.hasActivityTaskFailedEventAttributes() -> {
                    val step = steps[event.activityTaskFailedEventAttributes.scheduledEventId]
                    step?.let { stepStates[it] = "FAILED" }
                    detail = "step=$step"
                }
                event.hasActivityTaskTimedOutEventAttributes() -> {
                    val step = steps[event.activityTaskTimedOutEventAttributes.scheduledEventId]
                    step?.let { stepStates[it] = "TIMED_OUT" }
                    detail = "step=$step"
                }
                event.hasTimerStartedEventAttributes() -> {
                    timers[event.timerStartedEventAttributes.timerId] = current
                    stepStates[current] = "WAITING_TIMER"
                    detail = "step=$current durable wait started"
                }
                event.hasTimerFiredEventAttributes() -> {
                    val step = timers[event.timerFiredEventAttributes.timerId] ?: current
                    stepStates[step] = "COMPLETED"
                    if (step !in done) done.add(step)
                    detail = "step=$step durable wait ended"
                }
                event.hasWorkflowExecutionCompletedEventAttributes() -> current = "DONE"
                event.hasWorkflowExecutionFailedEventAttributes() -> current = "FAILED"
                event.hasWorkflowExecutionCanceledEventAttributes() -> current = "CANCELED"
                event.hasWorkflowExecutionTerminatedEventAttributes() -> current = "TERMINATED"
                event.hasWorkflowExecutionTimedOutEventAttributes() -> current = "TIMED_OUT"
            }
            val time = Instant.ofEpochSecond(event.eventTime.seconds, event.eventTime.nanos.toLong())
            // A server event does not prove that a worker pod is running.
            timeline.add("$time|id=$runId:${event.eventId}|pod=$eventPod|action=${event.eventType.name.removePrefix("EVENT_TYPE_")}|detail=$detail")
        }
        return WorkflowStatusResponse(workflowId, current, done, "UNKNOWN", timeline, pod, runId, stepPods, stepStates, completedActivities = activitiesDone)
    }
}
