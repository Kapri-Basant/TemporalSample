package com.temporal.sample

import jakarta.servlet.DispatcherType
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.http.HttpMethod
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator
import org.springframework.security.oauth2.core.OAuth2Error
import org.springframework.security.oauth2.core.OAuth2TokenValidator
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtValidators
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder
import org.springframework.security.web.SecurityFilterChain
import java.net.URI

@Configuration
class SecurityConfig {
    // Keep the visual recovery exercise open only in the explicit local profile.
    @Bean
    @Profile("local")
    fun localSecurity(http: HttpSecurity): SecurityFilterChain = http
        .csrf { it.disable() }
        .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
        .authorizeHttpRequests { it.anyRequest().permitAll() }
        .build()

    // Accept signed bearer tokens and close all routes outside the public API and health checks.
    @Bean
    @Profile("prod")
    fun productionSecurity(http: HttpSecurity): SecurityFilterChain = http
        .csrf { it.disable() }
        .requestCache { it.disable() }
        .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
        .formLogin { it.disable() }
        .httpBasic { it.disable() }
        .authorizeHttpRequests {
            it.dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                .requestMatchers(HttpMethod.GET, "/actuator/health", "/actuator/health/**").permitAll()
                .requestMatchers(HttpMethod.GET, "/actuator/prometheus").hasAuthority("SCOPE_workflow.metrics")
                .requestMatchers(HttpMethod.GET, "/api/**").hasAuthority("SCOPE_workflow.read")
                .requestMatchers("/api/**").hasAuthority("SCOPE_workflow.write")
                .anyRequest().denyAll()
        }
        .oauth2ResourceServer { it.jwt {} }
        .build()

    // Verify the issuer's signatures, token dates, expected audience, and caller identity.
    @Bean
    @Profile("prod")
    fun jwtDecoder(
        guard: ProductionGuard,
        @Value("\${app.auth.issuer-uri:}") issuer: String,
        @Value("\${app.auth.audience:}") audience: String
    ): JwtDecoder {
        require(URI.create(issuer).scheme == "https") { "AUTH_ISSUER_URI must use HTTPS" }
        require(audience.isNotBlank()) { "AUTH_AUDIENCE is required" }
        return NimbusJwtDecoder.withIssuerLocation(issuer).build().apply {
            setJwtValidator(TokenChecks.forIssuer(issuer, audience))
        }
    }
}

object TokenChecks {
    // Keep date and issuer checks together with the application's audience check.
    fun forIssuer(issuer: String, audience: String): OAuth2TokenValidator<Jwt> =
        DelegatingOAuth2TokenValidator(JwtValidators.createDefaultWithIssuer(issuer), AudienceAndSubjectValidator(audience))
}

class AudienceAndSubjectValidator(private val audience: String) : OAuth2TokenValidator<Jwt> {
    // Reject tokens for another service, anonymous tokens, and tokens without an expiry.
    override fun validate(token: Jwt): OAuth2TokenValidatorResult =
        if (audience in token.audience.orEmpty() && !token.subject.isNullOrBlank() && token.expiresAt != null)
            OAuth2TokenValidatorResult.success()
        else OAuth2TokenValidatorResult.failure(OAuth2Error("invalid_token", "Required audience, subject, or expiry is missing", null))
}
