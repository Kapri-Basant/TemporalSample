package com.temporal.sample

import com.google.protobuf.Timestamp
import io.temporal.api.enums.v1.EventType
import io.temporal.api.history.v1.*
import io.temporal.common.converter.DefaultDataConverter
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HistoryStatusTests {
    // Check that recorded events rebuild progress with no running worker.
    @Test
    fun `saved activity progress stays readable without a worker`() {
        val input = DefaultDataConverter.STANDARD_INSTANCE.toPayloads("user-1", "STEP_1_VALIDATE_USER").get()
        val events = listOf(
            HistoryEvent.newBuilder().setEventId(1).setEventTime(Timestamp.newBuilder().setSeconds(100))
                .setEventType(EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                .setActivityTaskScheduledEventAttributes(ActivityTaskScheduledEventAttributes.newBuilder().setInput(input)).build(),
            HistoryEvent.newBuilder().setEventId(2).setEventTime(Timestamp.newBuilder().setSeconds(101))
                .setEventType(EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED)
                .setActivityTaskStartedEventAttributes(ActivityTaskStartedEventAttributes.newBuilder()
                    .setScheduledEventId(1).setIdentity("worker-pod-1").setAttempt(1)).build(),
            HistoryEvent.newBuilder().setEventId(3).setEventTime(Timestamp.newBuilder().setSeconds(102))
                .setEventType(EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
                .setActivityTaskCompletedEventAttributes(ActivityTaskCompletedEventAttributes.newBuilder().setScheduledEventId(1)).build()
        )
        val status = HistoryStatus.read("order-1", events)
        assertEquals(emptyList(), status.completedSteps)
        assertEquals(listOf("STEP_1_VALIDATE_USER"), status.completedActivities)
        assertEquals("worker-pod-1", status.podName)
        assertEquals(3, status.timeline.size)
        assertTrue(status.timeline.last().contains("ACTIVITY_TASK_COMPLETED"))
        assertEquals(status, HistoryStatus.read("order-1", events))
    }

    // Keep a business failure separate from worker health.
    @Test
    fun `workflow failure is not reported as worker restart`() {
        val event = HistoryEvent.newBuilder().setEventId(1)
            .setEventType(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
            .setWorkflowExecutionFailedEventAttributes(WorkflowExecutionFailedEventAttributes.getDefaultInstance()).build()
        assertEquals("FAILED", HistoryStatus.read("order-2", listOf(event)).currentStep)
    }
}
