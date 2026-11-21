package com.temporal.sample

import com.sun.net.httpserver.HttpServer
import com.temporal.sample.integration.*
import io.temporal.failure.ActivityFailure
import io.temporal.failure.ApplicationFailure
import io.temporal.testing.TestActivityEnvironment
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HttpJobActivitiesTests {
    // Send only to configured endpoints and pass a key the provider can deduplicate.
    @Test
    fun `HTTP task sends stable key and reads a small receipt`() {
        val receivedKey = AtomicReference<String>()
        val receivedBody = AtomicReference<String>()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/execute") { exchange ->
            receivedKey.set(exchange.requestHeaders.getFirst("Idempotency-Key"))
            receivedBody.set(String(exchange.requestBody.readAllBytes(), StandardCharsets.UTF_8))
            val bytes = """{"receipt":"provider-123","output":{"state":"saved"}}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.start()
        try {
            val environment = TestActivityEnvironment.newInstance()
            try {
                val properties = properties("http://127.0.0.1:${server.address.port}/execute")
                environment.registerActivitiesImplementations(HttpJobActivities(properties))
                val activities = environment.newActivityStub(JobActivities::class.java)
                val result = activities.execute(TaskCall("job", "owner", 0, "reserve", mapOf("item" to "book"), "stable-key"))
                assertEquals("provider-123", result.receipt)
                assertEquals("saved", result.output["state"])
                assertEquals("stable-key", receivedKey.get())
                assertTrue(receivedBody.get().contains("book"))
            } finally { environment.close() }
        } finally { server.stop(0) }
    }

    // Redirects must not move a bearer token to a different endpoint.
    @Test
    fun `HTTP task never follows a redirect`() {
        val redirected = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/execute") { exchange ->
            exchange.responseHeaders.add("Location", "/other")
            exchange.sendResponseHeaders(302, -1)
            exchange.close()
        }
        server.createContext("/other") { exchange ->
            redirected.incrementAndGet()
            exchange.sendResponseHeaders(500, -1)
            exchange.close()
        }
        server.start()
        try {
            val environment = TestActivityEnvironment.newInstance()
            try {
                environment.registerActivitiesImplementations(HttpJobActivities(properties("http://127.0.0.1:${server.address.port}/execute")))
                val activities = environment.newActivityStub(JobActivities::class.java)
                val failure = assertFailsWith<ActivityFailure> { activities.execute(TaskCall(task = "reserve")) }
                assertTrue((failure.cause as ApplicationFailure).isNonRetryable)
                assertEquals(0, redirected.get())
            } finally { environment.close() }
        } finally { server.stop(0) }
    }

    // An accepted request can still be running and must never mark its task complete.
    @Test
    fun `HTTP 202 with a receipt is not a completed task`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/execute") { exchange ->
            val bytes = """{"receipt":"pending-provider-job"}""".toByteArray()
            exchange.sendResponseHeaders(202, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.start()
        try {
            val environment = TestActivityEnvironment.newInstance()
            try {
                environment.registerActivitiesImplementations(HttpJobActivities(properties("http://127.0.0.1:${server.address.port}/execute")))
                val activities = environment.newActivityStub(JobActivities::class.java)
                val failure = assertFailsWith<ActivityFailure> { activities.execute(TaskCall(task = "reserve")) }
                val cause = failure.cause as ApplicationFailure
                assertEquals("TASK_ASYNC_UNSUPPORTED", cause.type)
                assertTrue(cause.isNonRetryable)
            } finally { environment.close() }
        } finally { server.stop(0) }
    }

    // Reject a large reply instead of keeping unbounded bytes in worker memory.
    @Test
    fun `HTTP task bounds response size`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/execute") { exchange ->
            val bytes = ByteArray(70000) { 'a'.code.toByte() }
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.start()
        try {
            val environment = TestActivityEnvironment.newInstance()
            try {
                environment.registerActivitiesImplementations(HttpJobActivities(properties("http://127.0.0.1:${server.address.port}/execute")))
                val activities = environment.newActivityStub(JobActivities::class.java)
                val failure = assertFailsWith<ActivityFailure> { activities.execute(TaskCall(task = "reserve")) }
                assertEquals("TASK_BAD_REPLY", (failure.cause as ApplicationFailure).type)
            } finally { environment.close() }
        } finally { server.stop(0) }
    }

    // Allow plain HTTP only for these local test servers.
    private fun properties(url: String): IntegrationProperties = IntegrationProperties().apply {
        allowHttp = true
        tasks = mapOf("reserve" to TaskTarget(url = url))
    }
}
