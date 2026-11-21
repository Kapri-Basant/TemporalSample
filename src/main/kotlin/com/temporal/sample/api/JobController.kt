package com.temporal.sample.api

import com.temporal.sample.integration.JobInput
import com.temporal.sample.integration.JobSnapshot
import com.temporal.sample.integration.JobWorkflowV1
import io.grpc.Status
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.HistoryEventFilterType
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowExecutionAlreadyStarted
import io.temporal.client.WorkflowOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.Authentication
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.net.URI
import java.time.Instant
import java.util.Optional
import java.util.concurrent.TimeUnit

// Keep transport state separate from the result of the business steps.
data class JobView(
    val jobId: String, val runId: String, val executionStatus: String,
    val businessStatus: String? = null, val result: JobSnapshot? = null,
    val pendingTasks: List<PendingTask> = emptyList()
)
data class PendingTask(val activityId: String, val activityType: String, val state: String, val attempt: Int, val worker: String)
data class JobEvent(val id: Long, val time: String, val type: String)
data class EventPage(val runId: String, val events: List<JobEvent>, val nextPageToken: String?)

@Service
@ConditionalOnProperty(name = ["app.mode"], havingValue = "api", matchIfMissing = true)
class JobService(
    private val client: WorkflowClient,
    private val requests: JobRequests,
    private val cursor: JobCursor,
    @Value("\${integration.task-queue:integration-v1}") private val queue: String
) {
    // Start one saved job. Retrying an already used job ID returns a conflict.
    fun start(input: JobInput): Map<String, String> = temporalCall {
        val id = requests.workflowId(input.ownerId, input.jobId)
        val workflow = client.newWorkflowStub(JobWorkflowV1::class.java, WorkflowOptions.newBuilder()
            .setWorkflowId(id).setTaskQueue(queue)
            .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE).build())
        val execution = WorkflowClient.start(workflow::run, input)
        mapOf("jobId" to input.jobId, "runId" to execution.runId, "workflowId" to id)
    }

    // Read server state without needing any worker or downloading the full history.
    fun status(owner: String, jobId: String): JobView = temporalCall {
        val description = describe(owner, jobId)
        val execution = description.workflowExecutionInfo.execution
        val state = description.workflowExecutionInfo.status
        val result = if (state == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED) {
            client.newUntypedWorkflowStub(execution, Optional.empty()).getResult(2, TimeUnit.SECONDS, JobSnapshot::class.java)
        } else null
        JobView(jobId, execution.runId, state.name.removePrefix("WORKFLOW_EXECUTION_STATUS_"), result?.status, result,
            description.pendingActivitiesList.map {
                PendingTask(it.activityId, it.activityType.name, it.state.name.removePrefix("PENDING_ACTIVITY_STATE_"), it.attempt, it.lastWorkerIdentity)
            })
    }

    // Return one page of recorded events; an offline worker does not hide these logs.
    fun events(owner: String, jobId: String, token: String?, runId: String?, pageSize: Int): EventPage = temporalCall {
        if (pageSize !in 1..100) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "pageSize must be between 1 and 100")
        if (token != null && (token.length > 16_384 || runId.isNullOrBlank())) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "A page token needs its runId and must fit in 16 KiB")
        }
        val id = requests.workflowId(owner, jobId)
        val pinned = if (runId.isNullOrBlank()) describe(owner, jobId).workflowExecutionInfo.execution
            else WorkflowExecution.newBuilder().setWorkflowId(id).setRunId(runId).build()
        val builder = GetWorkflowExecutionHistoryRequest.newBuilder().setNamespace(client.options.namespace)
            .setExecution(pinned).setMaximumPageSize(pageSize).setWaitNewEvent(false)
            .setHistoryEventFilterType(HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
        if (token != null) {
            val bytes = cursor.decode(token, owner, jobId, pinned.runId)
            builder.nextPageToken = com.google.protobuf.ByteString.copyFrom(bytes)
        }
        val response = client.workflowServiceStubs.blockingStub().withDeadlineAfter(3, TimeUnit.SECONDS)
            .getWorkflowExecutionHistory(builder.build())
        EventPage(pinned.runId, response.history.eventsList.map {
            JobEvent(it.eventId, Instant.ofEpochSecond(it.eventTime.seconds, it.eventTime.nanos.toLong()).toString(),
                it.eventType.name.removePrefix("EVENT_TYPE_"))
        }, if (response.nextPageToken.isEmpty) null else cursor.encode(owner, jobId, pinned.runId, response.nextPageToken.toByteArray()))
    }

    // Ask the workflow to stop and undo work; never force-terminate business jobs here.
    fun cancel(owner: String, jobId: String): Map<String, String> = temporalCall {
        val info = describe(owner, jobId).workflowExecutionInfo
        if (info.status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
            mapOf("message" to "job already closed", "jobId" to jobId)
        } else {
            val workflow = client.newWorkflowStub(JobWorkflowV1::class.java, info.execution.workflowId, Optional.of(info.execution.runId))
            workflow.cancel()
            mapOf("message" to "cancellation requested; read status for the result", "jobId" to jobId)
        }
    }

    // Resolve an owner-scoped workflow and pin later reads to this exact run.
    private fun describe(owner: String, jobId: String) = client.workflowServiceStubs.blockingStub()
        .withDeadlineAfter(3, TimeUnit.SECONDS).describeWorkflowExecution(DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(client.options.namespace).setExecution(WorkflowExecution.newBuilder()
                .setWorkflowId(requests.workflowId(owner, jobId))).build())

    // Map connection problems and duplicate IDs to stable HTTP replies.
    private fun <T> temporalCall(action: () -> T): T = try {
        action()
    } catch (ex: ResponseStatusException) {
        throw ex
    } catch (ex: WorkflowExecutionAlreadyStarted) {
        throw ResponseStatusException(HttpStatus.CONFLICT, "jobId already exists; use GET to read it or use a new ID")
    } catch (ex: Exception) {
        val code = when (Status.fromThrowable(ex).code) {
            Status.Code.NOT_FOUND -> HttpStatus.NOT_FOUND
            Status.Code.INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST
            else -> HttpStatus.SERVICE_UNAVAILABLE
        }
        throw ResponseStatusException(code, if (code == HttpStatus.NOT_FOUND) "Job not found" else "Workflow request could not be completed", ex)
    }
}

@RestController
@RequestMapping("/api/v1/jobs")
@ConditionalOnProperty(name = ["app.mode"], havingValue = "api", matchIfMissing = true)
class JobController(private val jobs: JobService, private val requests: JobRequests, private val caller: Caller) {
    // Accept a job from any software that can call this JSON API.
    @PostMapping
    fun start(@RequestBody request: StartJobRequest, authentication: Authentication?): ResponseEntity<Map<String, String>> {
        val input = requests.validate(request, caller.owner(authentication))
        return ResponseEntity.accepted().location(URI.create("/api/v1/jobs/${request.jobId}")).body(jobs.start(input))
    }

    // Read only the signed-in caller's job.
    @GetMapping("/{jobId}")
    fun status(@PathVariable jobId: String, authentication: Authentication?): JobView = jobs.status(caller.owner(authentication), jobId)

    // Page through this caller's saved logs without exposing activity inputs or secrets.
    @GetMapping("/{jobId}/events")
    fun events(@PathVariable jobId: String, @RequestParam(required = false) pageToken: String?,
               @RequestParam(required = false) runId: String?, @RequestParam(defaultValue = "50") pageSize: Int,
               authentication: Authentication?): EventPage = jobs.events(caller.owner(authentication), jobId, pageToken, runId, pageSize)

    // Request safe cancellation rather than destroying workflow state.
    @PostMapping("/{jobId}/cancel")
    fun cancel(@PathVariable jobId: String, authentication: Authentication?): ResponseEntity<Map<String, String>> =
        ResponseEntity.accepted().body(jobs.cancel(caller.owner(authentication), jobId))
}
