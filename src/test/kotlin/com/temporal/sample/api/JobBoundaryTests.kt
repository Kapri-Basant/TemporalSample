package com.temporal.sample.api

import com.temporal.sample.integration.IntegrationProperties
import com.temporal.sample.integration.TaskTarget
import jakarta.servlet.FilterChain
import org.junit.jupiter.api.Test
import org.springframework.mock.env.MockEnvironment
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.web.server.ResponseStatusException
import kotlin.test.*

class JobBoundaryTests {
    // Keep duplicate protection per job so one owner can create many distinct jobs.
    @Test
    fun `different jobs and owners have different workflow ids`() {
        val requests = requests()
        assertNotEquals(requests.workflowId("owner", "job-1"), requests.workflowId("owner", "job-2"))
        assertNotEquals(requests.workflowId("owner-1", "job"), requests.workflowId("owner-2", "job"))
        assertEquals(requests.workflowId("owner", "job"), requests.workflowId("owner", "job"))
    }

    // Only registered tasks and bounded inputs may enter saved workflow history.
    @Test
    fun `unknown task and excessive wait or input are rejected`() {
        val requests = requests()
        for (step in listOf(StepRequest("unknown"), StepRequest("check", waitSeconds = 86401),
            StepRequest("check", mapOf("value" to "a".repeat(2049))))) {
            assertEquals(400, assertFailsWith<ResponseStatusException> {
                requests.validate(StartJobRequest("job", listOf(step)), "owner")
            }.statusCode.value())
        }
        val valid = requests.validate(StartJobRequest("job", listOf(StepRequest("check"))), "owner")
        assertEquals(0, valid.steps.single().waitSeconds)
        assertFalse(valid.steps.single().sideEffect)
    }

    // Derive ownership from signed token fields, not a user value supplied in JSON.
    @Test
    fun `owners are isolated by both issuer and subject`() {
        val caller = Caller(MockEnvironment().withProperty("spring.profiles.active", "prod"))
        val first = Jwt.withTokenValue("token").header("alg", "RS256").issuer("https://issuer.example")
            .subject("one").build()
        val second = Jwt.withTokenValue("token").header("alg", "RS256").issuer("https://issuer.example")
            .subject("two").build()
        assertNotEquals(caller.owner(JwtAuthenticationToken(first)), caller.owner(JwtAuthenticationToken(second)))
        assertEquals(401, assertFailsWith<ResponseStatusException> { caller.owner(null) }.statusCode.value())
    }

    // A signed cursor cannot be moved to another owner, job, or workflow run.
    @Test
    fun `event cursor rejects changed bindings and tampering`() {
        val cursor = JobCursor("a-long-test-signing-secret-for-cursors", MockEnvironment())
        val signed = cursor.encode("owner", "job", "run", byteArrayOf(1, 2, 3))
        assertContentEquals(byteArrayOf(1, 2, 3), cursor.decode(signed, "owner", "job", "run"))
        for (binding in listOf(Triple("other", "job", "run"), Triple("owner", "other", "run"), Triple("owner", "job", "other"))) {
            assertFailsWith<ResponseStatusException> { cursor.decode(signed, binding.first, binding.second, binding.third) }
        }
        assertFailsWith<ResponseStatusException> { cursor.decode("x$signed", "owner", "job", "run") }
    }

    // Stop large chunked requests before JSON parsing can allocate the whole body.
    @Test
    fun `request body limit covers chunked input`() {
        val request = object : MockHttpServletRequest("POST", "/api/v1/jobs") {
            // Imitate a transfer with no declared content length.
            override fun getContentLengthLong(): Long = -1
        }
        request.setContent(ByteArray(65_537))
        val response = MockHttpServletResponse()
        var reached = false
        RequestSizeFilter().doFilter(request, response, FilterChain { _, _ -> reached = true })
        assertEquals(413, response.status)
        assertFalse(reached)
    }

    // Leave an accepted small body intact for the normal JSON converter.
    @Test
    fun `small request body survives size check`() {
        val body = """{"jobId":"job"}"""
        val request = MockHttpServletRequest("POST", "/api/v1/jobs").apply { setContent(body.toByteArray()) }
        val response = MockHttpServletResponse()
        RequestSizeFilter().doFilter(request, response, FilterChain { value, _ ->
            assertEquals(body, value.inputStream.readAllBytes().toString(Charsets.UTF_8))
        })
        assertEquals("no-store", response.getHeader("Cache-Control"))
    }

    // Build a safe read-only registry for input-boundary checks.
    private fun requests(): JobRequests = JobRequests(IntegrationProperties().apply {
        tasks = mapOf("check" to TaskTarget(url = "https://example.test/check", readOnly = true))
    })
}
