package com.temporal.sample.integration

import com.fasterxml.jackson.databind.ObjectMapper
import io.temporal.activity.Activity
import io.temporal.failure.ApplicationFailure
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Flow
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Component
class HttpJobActivities(private val properties: IntegrationProperties) : JobActivities {
    private val json = ObjectMapper()
    private val log = LoggerFactory.getLogger(javaClass)
    private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3))
        .followRedirects(HttpClient.Redirect.NEVER).build()

    // Send a task only to a URL chosen by the server operator.
    override fun execute(call: TaskCall): TaskResult = send(call, false)

    // Use a separate undo key; a repeated undo must return the same safe outcome.
    override fun undo(call: TaskCall): TaskResult = send(call, true)

    // Bound memory, time, retries, and logs around each outside call.
    private fun send(call: TaskCall, undo: Boolean): TaskResult {
        val target = properties.tasks[call.task]
            ?: throw ApplicationFailure.newNonRetryableFailure("Task is not configured", "UNKNOWN_TASK")
        val address = if (undo) target.undoUrl else target.url
        if (address.isNullOrBlank()) throw ApplicationFailure.newNonRetryableFailure("Task endpoint is missing", "INVALID_TASK")
        val uri = try { URI.create(address) } catch (_: IllegalArgumentException) {
            throw ApplicationFailure.newNonRetryableFailure("Task URL is invalid", "INVALID_TASK")
        }
        if (uri.host.isNullOrBlank() || uri.userInfo != null || uri.fragment != null ||
            (uri.scheme != "https" && !(properties.allowHttp && uri.scheme == "http"))) {
            throw ApplicationFailure.newNonRetryableFailure("Task URL must use HTTPS", "INVALID_TASK")
        }
        val body = json.writeValueAsBytes(call)
        if (body.size > 32768) throw ApplicationFailure.newNonRetryableFailure("Task input is too large", "INVALID_TASK")
        val request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("Idempotency-Key", call.idempotencyKey)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        target.token?.takeIf { it.isNotBlank() }?.let {
            if (it.length > 8192 || it.any { char -> char.code !in 33..126 }) {
                throw ApplicationFailure.newNonRetryableFailure("Task credential format is invalid", "INVALID_TASK")
            }
            request.header("Authorization", "Bearer $it")
        }
        val context = Activity.getExecutionContext()
        log.info("workflow={} activity={} attempt={} task={} direction={} key={} action=HTTP_TASK_STARTED",
            context.info.workflowId, context.info.activityId, context.info.attempt, call.task,
            if (undo) "undo" else "execute", call.idempotencyKey)
        val future = client.sendAsync(request.build(), HttpResponse.BodyHandler { LimitedBody(65536) })
        val response = try {
            awaitReply(future)
        } catch (failure: ApplicationFailure) {
            throw failure
        } catch (failure: io.temporal.client.ActivityCanceledException) {
            throw failure
        } catch (failure: Exception) {
            if (generateSequence<Throwable>(failure) { it.cause }.any { it is ReplyTooLarge }) {
                throw ApplicationFailure.newNonRetryableFailure("Task reply is too large", "TASK_BAD_REPLY")
            }
            throw ApplicationFailure.newFailure("Task transport failed", "TASK_TRANSPORT")
        } finally {
            if (!future.isDone) future.cancel(true)
        }
        val code = response.statusCode()
        // Accepted means pending work, not a finished task, even when a receipt is present.
        if (code == 202) {
            throw ApplicationFailure.newNonRetryableFailure("Task is asynchronous; a completion adapter is required", "TASK_ASYNC_UNSUPPORTED")
        }
        if (code in 200..299 && code !in setOf(200, 201)) {
            throw ApplicationFailure.newNonRetryableFailure("Task must return a final HTTP 200 or 201 reply", "TASK_BAD_REPLY")
        }
        if (code !in 200..299) {
            if (code in setOf(408, 425, 429) || code >= 500) {
                throw ApplicationFailure.newFailure("Task returned HTTP $code", "TASK_RETRYABLE")
            }
            throw ApplicationFailure.newNonRetryableFailure("Task returned HTTP $code", "TASK_REJECTED")
        }
        val result = try {
            val node = json.readTree(response.body())
            require(node.isObject && node.path("receipt").isTextual)
            val receipt = node.path("receipt").asText("")
            require(receipt.isNotBlank() && receipt.length <= 256)
            val output = node.path("output")
            require(output.isMissingNode || output.isObject)
            require(output.size() <= 16)
            val values = mutableMapOf<String, String>()
            output.fields().forEach { (name, value) ->
                require(name.length <= 80 && value.isTextual && value.asText().length <= 256)
                values[name] = value.asText()
            }
            require(json.writeValueAsBytes(values).size <= 8192)
            TaskResult(receipt, values, System.getenv("POD_NAME") ?: "local-worker")
        } catch (_: Exception) {
            throw ApplicationFailure.newNonRetryableFailure("Task reply must contain a small receipt and string output", "TASK_BAD_REPLY")
        }
        log.info("workflow={} activity={} task={} direction={} action=HTTP_TASK_COMPLETED",
            context.info.workflowId, context.info.activityId, call.task, if (undo) "undo" else "execute")
        return result
    }

    // Heartbeats allow cancellation while waiting, and the whole call has a time limit.
    private fun awaitReply(future: CompletableFuture<HttpResponse<ByteArray>>): HttpResponse<ByteArray> {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (true) {
            Activity.getExecutionContext().heartbeat("HTTP_REQUEST_IN_FLIGHT")
            try {
                return future.get(250, TimeUnit.MILLISECONDS)
            } catch (_: TimeoutException) {
                if (System.nanoTime() >= until) throw ApplicationFailure.newFailure("Task request timed out", "TASK_TIMEOUT")
            }
        }
    }

    private class ReplyTooLarge : RuntimeException()

    // Stop a large response before it can fill worker memory.
    private class LimitedBody(private val maximum: Long) : HttpResponse.BodySubscriber<ByteArray> {
        private val delegate = HttpResponse.BodySubscribers.ofByteArray()
        private var subscription: Flow.Subscription? = null
        private var size = 0L

        // Let the HTTP client wait for the bounded body.
        override fun getBody(): CompletionStage<ByteArray> = delegate.body

        // Keep the subscription so a large response can be stopped.
        override fun onSubscribe(value: Flow.Subscription) {
            subscription = value
            delegate.onSubscribe(value)
        }

        // Reject extra bytes before passing them to the byte-array collector.
        override fun onNext(items: List<ByteBuffer>) {
            size += items.sumOf { it.remaining().toLong() }
            if (size > maximum) {
                subscription?.cancel()
                delegate.onError(ReplyTooLarge())
            } else delegate.onNext(items)
        }

        // Pass a failed network read to the waiting activity.
        override fun onError(error: Throwable) = delegate.onError(error)

        // Finish collecting a response that stayed within the limit.
        override fun onComplete() = delegate.onComplete()
    }
}
