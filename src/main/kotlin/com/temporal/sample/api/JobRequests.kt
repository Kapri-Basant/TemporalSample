package com.temporal.sample.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.temporal.sample.integration.IntegrationProperties
import com.temporal.sample.integration.JobInput
import com.temporal.sample.integration.JobStep
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import java.security.MessageDigest

data class StartJobRequest(val jobId: String, val steps: List<StepRequest>)
data class StepRequest(val task: String, val input: Map<String, String> = emptyMap(), val waitSeconds: Int = 0)

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = ["app.mode"], havingValue = "api", matchIfMissing = true)
class JobRequests(private val properties: IntegrationProperties) {
    private val json = ObjectMapper()

    // Check a public ID before putting it in a workflow ID or a URL.
    fun checkId(jobId: String) {
        if (!jobId.matches(Regex("[A-Za-z0-9_-]{1,80}"))) badRequest("jobId must use 1–80 letters, numbers, _ or -")
    }

    // Copy only known task names and safe limits into the saved workflow input.
    fun validate(request: StartJobRequest, ownerId: String): JobInput {
        checkId(request.jobId)
        if (request.steps.size !in 1..20) badRequest("Use between 1 and 20 steps")
        if (json.writeValueAsBytes(request).size > 16_384) badRequest("Job input must fit in 16 KiB; send data references for large files")
        val steps = request.steps.map { step ->
            val target = properties.tasks[step.task] ?: badRequest("Task is not registered: ${step.task.take(64)}")
            if (step.waitSeconds !in 0..86_400) badRequest("Each wait must be between 0 and 86400 seconds")
            if (step.input.size > 32 || step.input.any { it.key.length !in 1..64 || it.value.length > 2048 }) {
                badRequest("Use at most 32 input fields; keys up to 64 characters and values up to 2048")
            }
            JobStep(step.task, step.input.toMap(), step.waitSeconds, !target.undoUrl.isNullOrBlank(), !target.readOnly)
        }
        return JobInput(request.jobId, ownerId, steps)
    }

    // Use a stable owner hash so IDs do not reveal a login or cross user boundaries.
    fun workflowId(ownerId: String, jobId: String): String {
        checkId(jobId)
        return "job-v1:$ownerId:$jobId"
    }

    // Return a clear client error without exposing service credentials.
    private fun badRequest(message: String): Nothing = throw ResponseStatusException(HttpStatus.BAD_REQUEST, message)
}

// Hash a caller identity without depending on the machine or the current time.
fun ownerHash(issuer: String, subject: String): String = MessageDigest.getInstance("SHA-256")
    .digest("$issuer\u0000$subject".toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
