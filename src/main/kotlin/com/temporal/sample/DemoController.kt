package com.temporal.sample

import java.util.concurrent.TimeUnit
import java.time.Instant
import io.grpc.Status
import org.springframework.web.server.ResponseStatusException
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.api.enums.v1.PendingActivityState
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.client.WorkflowServiceException
import io.temporal.client.WorkflowNotFoundException
import com.temporal.sample.workflow.UserOrderWorkflow
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowExecutionAlreadyStarted
import io.temporal.client.WorkflowOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

data class StartDemoRequest(
    val userId: String,
    val stepTimersSec: List<Int> = listOf(20, 20, 20, 20, 20)
)

data class WorkflowStatusResponse(
    val workflowId: String,
    val currentStep: String,
    val completedSteps: List<String>,
    val workerStatus: String,
    val timeline: List<String>,
    val podName: String,
    val runId: String = "",
    val stepPods: Map<String, String> = emptyMap(),
    val stepStates: Map<String, String> = emptyMap(),
    val observations: List<String> = emptyList(),
    val completedActivities: List<String> = emptyList()
)

@org.springframework.context.annotation.Profile("local")
@RestController
@RequestMapping("/demo")
@ConditionalOnProperty(name = ["app.mode"], havingValue = "api", matchIfMissing = true)
class DemoController(
    private val workflowClient: WorkflowClient,
    private val workerStatusReader: WorkerStatus,
    @Value("\${app.allow-crash:false}") private val allowCrash: Boolean,
    @Value("\${temporal.task-queue:$USER_DEMO_TASK_QUEUE}") private val taskQueue: String,
    @Value("\${worker.control.base-url:http://temporal-demo-worker-control:8080}") private val workerControlBaseUrl: String
) {
    private val httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    // Start an order with the five default waits.
    @PostMapping("/start/{userId}")
    fun start(@PathVariable userId: String): ResponseEntity<Map<String, String>> {
        return startWithBody(StartDemoRequest(userId = userId))
    }

    // Check the input and start one workflow for this user.
    @PostMapping("/start")
    fun startWithBody(@RequestBody request: StartDemoRequest): ResponseEntity<Map<String, String>> {
        if (!request.userId.matches(Regex("[A-Za-z0-9_-]{1,80}")) ||
            request.stepTimersSec.size != 5 || request.stepTimersSec.any { it !in 3..120 }) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST, "Use a user ID with letters, numbers, _ or - and five timers from 3 to 120 seconds"
            )
        }
        val workflowId = "$USER_DEMO_WORKFLOW_PREFIX${request.userId}"
        return try {
            val options = WorkflowOptions.newBuilder()
                .setTaskQueue(taskQueue)
                .setWorkflowId(workflowId)
                .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
                .build()
            val workflow = workflowClient.newWorkflowStub(UserOrderWorkflow::class.java, options)
            val execution = WorkflowClient.start(workflow::run, request.userId, request.stepTimersSec)
            ResponseEntity.ok(
                mapOf(
                    "workflowId" to workflowId,
                    "runId" to execution.runId,
                    "message" to "started"
                )
            )
        } catch (_: WorkflowExecutionAlreadyStarted) {
            ResponseEntity.status(HttpStatus.CONFLICT)
                .body(
                    mapOf(
                        "workflowId" to workflowId,
                        "message" to "already running or done"
                    )
                )
        } catch (ex: WorkflowServiceException) {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Temporal could not accept the workflow", ex)
        }
    }

    // Read saved progress even while every worker is down.
    @GetMapping("/status/{userId}")
    fun status(@PathVariable userId: String): WorkflowStatusResponse {
        val workflowId = "$USER_DEMO_WORKFLOW_PREFIX$userId"
        return try {
            // Pin both reads to one run so they cannot mix two executions.
            val description = workflowClient.workflowServiceStubs.blockingStub()
                .withDeadlineAfter(3, TimeUnit.SECONDS)
                .describeWorkflowExecution(DescribeWorkflowExecutionRequest.newBuilder()
                    .setNamespace(workflowClient.options.namespace)
                    .setExecution(WorkflowExecution.newBuilder().setWorkflowId(workflowId))
                    .build())
            val runId = description.workflowExecutionInfo.execution.runId
            val history = workflowClient.fetchHistory(workflowId, runId).history.eventsList
            val saved = HistoryStatus.read(workflowId, history).copy(runId = runId)
            // Pending attempts are live server state; they may not yet appear in saved history.
            val states = saved.stepStates.toMutableMap()
            val pods = saved.stepPods.toMutableMap()
            val observations = mutableListOf<String>()
            for (activity in description.pendingActivitiesList) {
                val scheduled = history.lastOrNull { it.hasActivityTaskScheduledEventAttributes() }
                if (scheduled?.activityTaskScheduledEventAttributes?.activityId != activity.activityId) continue
                val step = saved.currentStep
                // History was read after Describe; a finished activity is no longer pending.
                if (states[step] !in listOf("SCHEDULED", "ACTIVITY_RUNNING")) continue
                states[step] = when (activity.state) {
                    PendingActivityState.PENDING_ACTIVITY_STATE_STARTED -> "ACTIVITY_RUNNING"
                    else -> if (activity.attempt > 1) "RETRY_WAIT" else "SCHEDULED"
                }
                if (activity.lastWorkerIdentity.isNotBlank()) pods[step] = activity.lastWorkerIdentity
                val time = if (activity.hasLastStartedTime()) activity.lastStartedTime else activity.scheduledTime
                observations.add("${Instant.ofEpochSecond(time.seconds, time.nanos.toLong())}|id=$runId:pending:${activity.activityId}:${activity.attempt}:${activity.state}|pod=${activity.lastWorkerIdentity.ifBlank { "unknown" }}|action=ACTIVITY_ATTEMPT_OBSERVED|detail=step=$step attempt=${activity.attempt} state=${states[step]}")
            }
            saved.copy(stepStates = states, stepPods = pods, observations = observations)
        } catch (ex: WorkflowNotFoundException) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Workflow not found", ex)
        } catch (ex: Exception) {
            val status = if (Status.fromThrowable(ex).code == Status.Code.NOT_FOUND)
                HttpStatus.NOT_FOUND else HttpStatus.SERVICE_UNAVAILABLE
            throw ResponseStatusException(status, "Temporal history unavailable", ex)
        }
    }

    // Read actual pod state, or label a local HTTP check clearly.
    @GetMapping("/worker-status")
    fun workerStatus(): WorkerStatusResponse = workerStatusReader.read().copy(crashEnabled = allowCrash)

    // Send a test crash request only when this control is enabled.
    @PostMapping("/admin/crash-worker")
    fun crashWorker(): ResponseEntity<Map<String, String>> {
        if (!allowCrash) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(mapOf("message" to "Crash control is disabled"))
        }
        return try {
            val request = HttpRequest.newBuilder()
                .uri(URI.create("$workerControlBaseUrl/worker/control/crash"))
                .timeout(Duration.ofSeconds(4))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            ResponseEntity.status(response.statusCode())
                .body(mapOf("message" to if (response.statusCode() in 200..299) "worker accepted crash request" else "worker rejected crash request", "details" to response.body()))
        } catch (ex: Exception) {
            ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(mapOf("message" to "failed to call worker control", "error" to (ex.message ?: "unknown error")))
        }
    }
}
