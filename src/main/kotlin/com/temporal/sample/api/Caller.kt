package com.temporal.sample.api

import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles
import org.springframework.http.HttpStatus
import org.springframework.security.core.Authentication
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = ["app.mode"], havingValue = "api", matchIfMissing = true)
class Caller(private val environment: Environment) {
    // Take ownership from a verified token, never from the request body.
    fun owner(authentication: Authentication?): String {
        if (environment.acceptsProfiles(Profiles.of("local"))) return ownerHash("local", "local-user")
        val jwt = (authentication as? JwtAuthenticationToken)?.token
            ?: throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "A verified access token is required")
        val issuer = jwt.issuer?.toString().orEmpty()
        val subject = jwt.subject.orEmpty()
        if (issuer.isBlank() || subject.isBlank()) throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Token needs issuer and subject")
        return ownerHash(issuer, subject)
    }
}
