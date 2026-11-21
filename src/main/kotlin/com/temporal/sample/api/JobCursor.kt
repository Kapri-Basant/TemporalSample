package com.temporal.sample.api

import org.springframework.beans.factory.annotation.Value
import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import java.security.MessageDigest
import java.time.Clock
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = ["app.mode"], havingValue = "api", matchIfMissing = true)
class JobCursor(
    @Value("\${integration.cursor-secret:}") configuredSecret: String,
    environment: Environment
) {
    private val clock = Clock.systemUTC()
    private val encoder = Base64.getUrlEncoder().withoutPadding()
    private val decoder = Base64.getUrlDecoder()
    private val secret = (if (configuredSecret.isNotBlank()) configuredSecret else {
        check(environment.acceptsProfiles(Profiles.of("local"))) { "Set INTEGRATION_CURSOR_SECRET" }
        "local-only-cursor-key-not-for-production"
    }).toByteArray(Charsets.UTF_8)

    // Sign the next-page token together with its caller, job, run, and expiry.
    fun encode(owner: String, jobId: String, runId: String, token: ByteArray): String {
        val text = "${clock.instant().epochSecond + 900}|$owner|$jobId|$runId|${encoder.encodeToString(token)}"
        val payload = text.toByteArray(Charsets.UTF_8)
        return "${encoder.encodeToString(payload)}.${encoder.encodeToString(sign(payload))}"
    }

    // Reject changed, expired, or cross-job cursors before calling Temporal.
    fun decode(cursor: String, owner: String, jobId: String, runId: String): ByteArray = try {
        val parts = cursor.split('.')
        require(parts.size == 2 && cursor.length <= 16_384)
        val payload = decoder.decode(parts[0])
        require(MessageDigest.isEqual(sign(payload), decoder.decode(parts[1])))
        val values = payload.toString(Charsets.UTF_8).split('|')
        require(values.size == 5 && values[0].toLong() >= clock.instant().epochSecond)
        require(values[1] == owner && values[2] == jobId && values[3] == runId)
        decoder.decode(values[4])
    } catch (_: Exception) {
        throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid or expired page token; start reading events again")
    }

    // Make one keyed signature without sharing a mutable Mac between requests.
    private fun sign(payload: ByteArray): ByteArray = Mac.getInstance("HmacSHA256").run {
        init(SecretKeySpec(secret, "HmacSHA256"))
        doFinal(payload)
    }
}
