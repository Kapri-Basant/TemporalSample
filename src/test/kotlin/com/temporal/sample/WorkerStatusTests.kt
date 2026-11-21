package com.temporal.sample

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkerStatusTests {
    private val json = ObjectMapper()

    // Use actual Kubernetes fields to show a restarted but not yet ready worker.
    @Test
    fun `restart and last exit come from container status`() {
        val pod = json.readTree("""{
          "metadata":{"name":"worker-1","uid":"pod-uid"},
          "status":{"phase":"Running","conditions":[{"type":"Ready","status":"False"}],
            "containerStatuses":[{"name":"worker","restartCount":2,
              "state":{"waiting":{"reason":"CrashLoopBackOff"}},
              "lastState":{"terminated":{"exitCode":1,"reason":"Error","finishedAt":"2026-11-21T07:00:00Z"}}}]}
        }""")
        val status = WorkerStatus.project(pod, emptyList())
        assertEquals("RESTARTING", status.workerStatus)
        assertEquals(2, status.restartCount)
        assertEquals("worker-1", status.podName)
        assertTrue(status.timeline.single().contains("CONTAINER_EXITED"))
        assertTrue(status.podStatus.contains("CrashLoopBackOff"))
    }

    // A restart counter stays nonzero after the container becomes ready again.
    @Test
    fun `ready pod after restart is up`() {
        val pod = json.readTree("""{
          "metadata":{"name":"worker-1","uid":"pod-uid"},
          "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}],
            "containerStatuses":[{"name":"worker","restartCount":2,
              "state":{"running":{"startedAt":"2026-11-21T07:01:00Z"}}}]}
        }""")
        val status = WorkerStatus.project(pod, emptyList())
        assertEquals("UP", status.workerStatus)
        assertEquals(2, status.restartCount)
        assertTrue(status.timeline.single().contains("CONTAINER_STARTED"))
    }
}
