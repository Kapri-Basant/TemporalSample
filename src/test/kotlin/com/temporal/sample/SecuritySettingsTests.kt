package com.temporal.sample

import com.temporal.sample.integration.TaskTarget
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.security.oauth2.jwt.Jwt
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SecuritySettingsTests {
    @TempDir
    lateinit var directory: Path

    // A valid token still needs the application's audience and a caller identity.
    @Test
    fun `tokens require the right issuer audience and subject`() {
        val checks = TokenChecks.forIssuer("https://identity.example.com", "workflow-api")
        assertFalse(checks.validate(token("https://identity.example.com", "workflow-api", "alice")).hasErrors())
        assertTrue(checks.validate(token("https://wrong.example.com", "workflow-api", "alice")).hasErrors())
        assertTrue(checks.validate(token("https://identity.example.com", "other-api", "alice")).hasErrors())
        assertTrue(checks.validate(token("https://identity.example.com", "workflow-api", "")).hasErrors())
        val expired = Jwt.withTokenValue("expired").header("alg", "RS256")
            .issuer("https://identity.example.com").audience(listOf("workflow-api")).subject("alice")
            .issuedAt(Instant.now().minusSeconds(300)).expiresAt(Instant.now().minusSeconds(120)).build()
        assertTrue(checks.validate(expired).hasErrors())
        val noExpiry = Jwt.withTokenValue("no-expiry").header("alg", "RS256")
            .issuer("https://identity.example.com").audience(listOf("workflow-api")).subject("alice").build()
        assertTrue(checks.validate(noExpiry).hasErrors())
    }

    // Startup must never guess whether it is a local demo or a production service.
    @Test
    fun `a profile must be chosen explicitly`() {
        ProductionGuard.validate(setOf("local"), { null })
        assertFailsWith<IllegalArgumentException> { ProductionGuard.validate(emptySet(), { null }) }
        assertFailsWith<IllegalArgumentException> { ProductionGuard.validate(setOf("local", "prod"), { null }) }
    }

    // Production refuses local crash controls, plain HTTP tasks, and missing undo support.
    @Test
    fun `production refuses unsafe settings`() {
        val certificate = Files.writeString(directory.resolve("client.pem"), "certificate is parsed when TLS starts")
        val key = Files.writeString(directory.resolve("client.key"), "key is parsed when TLS starts")
        val values = mapOf(
            "app.allow-crash" to "false", "temporal.tls.enabled" to "true",
            "temporal.target" to "temporal.example.com:7233", "temporal.namespace" to "orders",
            "temporal.tls.cert-file" to certificate.toString(), "temporal.tls.key-file" to key.toString(),
            "integration.cursor-secret" to "a-long-test-cursor-secret-at-least-32-bytes",
            "app.auth.issuer-uri" to "https://identity.example.com", "app.auth.audience" to "workflow-api"
        )
        val task = TaskTarget(url = "https://orders.example.com/check", readOnly = true)
        ProductionGuard.validate(setOf("prod"), values::get, mapOf("check" to task))
        ProductionGuard.validate(setOf("prod"), values::get,
            mapOf("send" to task.copy(readOnly = false, allowIrreversible = true)))
        ProductionGuard.validate(setOf("prod"), (values - "integration.cursor-secret" + ("app.mode" to "worker"))::get,
            mapOf("check" to task))
        assertFailsWith<IllegalArgumentException> {
            ProductionGuard.validate(setOf("prod"), (values + ("app.allow-crash" to "true"))::get, mapOf("check" to task))
        }
        assertFailsWith<IllegalArgumentException> {
            ProductionGuard.validate(setOf("prod"), values::get, mapOf("check" to task.copy(url = "http://orders.example.com/check")))
        }
        assertFailsWith<IllegalArgumentException> {
            ProductionGuard.validate(setOf("prod"), values::get, mapOf("charge" to task.copy(readOnly = false)))
        }
    }

    // Build a small signed-token model so validator tests need no identity server.
    private fun token(issuer: String, audience: String, subject: String): Jwt = Jwt.withTokenValue("test-token")
        .header("alg", "RS256").issuer(issuer).audience(listOf(audience)).subject(subject)
        .issuedAt(Instant.now().minusSeconds(5)).expiresAt(Instant.now().plusSeconds(60)).build()
}
