package com.temporal.sample

import com.fasterxml.jackson.databind.ObjectMapper
import com.temporal.sample.api.*
import com.temporal.sample.integration.*
import io.temporal.client.WorkflowStub
import io.temporal.testing.TestWorkflowEnvironment
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter
import org.springframework.mock.env.MockEnvironment
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.time.Duration
import java.util.Optional
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JobApiTests {
    private val json = ObjectMapper()

    // JSON clients can submit several jobs and retry a request without starting duplicate work.
    @Test
    fun `create accepts separate jobs but rejects duplicate IDs and unknown tasks`() = withApi { api ->
        api.mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
            .content(request("first-job", "check"))).andExpect(status().isAccepted)
        api.mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
            .content(request("first-job", "check"))).andExpect(status().isConflict)
        api.mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
            .content(request("second-job", "check"))).andExpect(status().isAccepted)
        api.mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
            .content(request("bad-job", "not-registered"))).andExpect(status().isBadRequest)
        val first = api.workflow("first-job")
        assertEquals("COMPLETED", WorkflowStub.fromTyped(first).getResult(5, TimeUnit.SECONDS, JobSnapshot::class.java).status)
        assertEquals("COMPLETED", WorkflowStub.fromTyped(api.workflow("second-job"))
            .getResult(5, TimeUnit.SECONDS, JobSnapshot::class.java).status)
        val view = json.readTree(api.mvc.perform(get("/api/v1/jobs/first-job"))
            .andExpect(status().isOk).andReturn().response.contentAsString)
        assertEquals("COMPLETED", view.path("executionStatus").asText())
        assertEquals("COMPLETED", view.path("businessStatus").asText())
        api.mvc.perform(get("/api/v1/jobs/missing-job")).andExpect(status().isNotFound)
    }

    // Status and saved events must stay readable when task pollers have stopped.
    @Test
    fun `saved status and events do not depend on a running worker`() = withApi { api ->
        api.mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
            .content(request("waiting-job", "check", 3600))).andExpect(status().isAccepted)
        api.environment.sleep(Duration.ofSeconds(1))
        assertEquals("WAITING", api.workflow("waiting-job").snapshot().status)
        api.environment.workerFactory.suspendPolling()
        try {
            val view = json.readTree(api.mvc.perform(get("/api/v1/jobs/waiting-job"))
                .andExpect(status().isOk).andReturn().response.contentAsString)
            assertEquals("RUNNING", view.path("executionStatus").asText())
            val page = json.readTree(api.mvc.perform(get("/api/v1/jobs/waiting-job/events").param("pageSize", "2"))
                .andExpect(status().isOk).andReturn().response.contentAsString)
            assertTrue(page.path("events").isArray)
            assertTrue(page.path("events").size() > 0)
            assertTrue(page.path("runId").asText().isNotBlank())
            api.mvc.perform(get("/api/v1/jobs/waiting-job/events").param("pageSize", "101"))
                .andExpect(status().isBadRequest)
            api.mvc.perform(get("/api/v1/jobs/waiting-job/events").param("pageToken", "invalid")
                .param("runId", page.path("runId").asText())).andExpect(status().isBadRequest)
        } finally {
            api.environment.workerFactory.resumePolling()
        }
    }

    // A cancel reply means accepted; the final result records cleanup and the business outcome.
    @Test
    fun `cancel is accepted and later reports its finished cleanup`() = withApi { api ->
        api.mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
            .content(request("cancel-job", "reserve", 3600))).andExpect(status().isAccepted)
        api.environment.sleep(Duration.ofSeconds(1))
        assertEquals("WAITING", api.workflow("cancel-job").snapshot().status)
        api.mvc.perform(post("/api/v1/jobs/cancel-job/cancel")).andExpect(status().isAccepted)
        val result = WorkflowStub.fromTyped(api.workflow("cancel-job"))
            .getResult(5, TimeUnit.SECONDS, JobSnapshot::class.java)
        assertEquals("CANCELED", result.status)
        assertEquals("UNDONE", result.steps.single().status)
        assertEquals(listOf("execute:reserve", "undo:reserve"), api.calls.toList())
        val view = json.readTree(api.mvc.perform(get("/api/v1/jobs/cancel-job"))
            .andExpect(status().isOk).andReturn().response.contentAsString)
        assertEquals("COMPLETED", view.path("executionStatus").asText())
        assertEquals("CANCELED", view.path("businessStatus").asText())
    }

    // Keep requests in real JSON so Kotlin constructor binding is covered too.
    private fun request(jobId: String, task: String, waitSeconds: Int = 0): String =
        """{"jobId":"$jobId","steps":[{"task":"$task","input":{"reference":"ref-1"},"waitSeconds":$waitSeconds}]}"""

    // Use the real controller and service with an in-process Temporal server and fake outside work.
    private fun withApi(test: (Api) -> Unit) {
        TestWorkflowEnvironment.newInstance().use { environment ->
            val registry = IntegrationProperties().apply {
                tasks = mapOf(
                    "check" to TaskTarget(url = "https://service.example.com/check", readOnly = true),
                    "reserve" to TaskTarget(url = "https://service.example.com/reserve", undoUrl = "https://service.example.com/release")
                )
            }
            val calls = CopyOnWriteArrayList<String>()
            val activities = object : JobActivities {
                // Return a saved receipt in place of a real provider call.
                override fun execute(call: TaskCall): TaskResult {
                    calls.add("execute:${call.task}")
                    return TaskResult("receipt-${call.index}", podName = "test-worker")
                }
                // Record cleanup so cancellation can be checked through the HTTP API.
                override fun undo(call: TaskCall): TaskResult {
                    calls.add("undo:${call.task}")
                    return TaskResult("undone-${call.index}")
                }
            }
            val worker = environment.newWorker("api-tests")
            worker.registerWorkflowImplementationTypes(JobWorkflowV1Impl::class.java)
            worker.registerActivitiesImplementations(activities)
            environment.start()
            val local = MockEnvironment().apply { setActiveProfiles("local") }
            val requests = JobRequests(registry)
            val caller = Caller(local)
            val service = JobService(environment.workflowClient, requests, JobCursor("test-cursor-secret-at-least-32-bytes", local), "api-tests")
            val controller = JobController(service, requests, caller)
            val mvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(JacksonJsonHttpMessageConverter()).build()
            test(Api(environment, mvc, requests, caller.owner(null), calls))
        }
    }

    private data class Api(
        val environment: TestWorkflowEnvironment,
        val mvc: MockMvc,
        val requests: JobRequests,
        val owner: String,
        val calls: CopyOnWriteArrayList<String>
    ) {
        // Read a typed handle for setup checks without changing the public API behavior.
        fun workflow(jobId: String): JobWorkflowV1 = environment.workflowClient.newWorkflowStub(
            JobWorkflowV1::class.java, requests.workflowId(owner, jobId), Optional.empty())
    }
}
