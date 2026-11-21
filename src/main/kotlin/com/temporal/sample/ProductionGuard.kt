package com.temporal.sample

import com.temporal.sample.integration.IntegrationProperties
import com.temporal.sample.integration.TaskTarget
import org.springframework.beans.factory.InitializingBean
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path

@Component
class ProductionGuard(private val environment: Environment, private val integration: IntegrationProperties) : InitializingBean {
    // Refuse unsafe startup instead of silently using the local settings in production.
    override fun afterPropertiesSet() {
        validate(environment.activeProfiles.toSet(), environment::getProperty, integration.tasks, integration.allowHttp)
    }

    companion object {
        // Check settings before opening connections or handling requests.
        fun validate(
            profiles: Set<String>,
            property: (String) -> String?,
            tasks: Map<String, TaskTarget> = emptyMap(),
            allowHttp: Boolean = false
        ) {
            require(("local" in profiles) xor ("prod" in profiles)) { "Choose exactly one profile: local or prod" }
            if ("prod" !in profiles) return
            require(property("app.allow-crash") != "true") { "Crash control cannot be enabled in production" }
            require(property("temporal.tls.enabled") == "true") { "Production Temporal requires TLS" }
            require(!property("temporal.namespace").isNullOrBlank()) { "TEMPORAL_NAMESPACE is required" }
            require(!property("temporal.target").isNullOrBlank()) { "TEMPORAL_TARGET is required" }
            for (name in listOf("temporal.tls.cert-file", "temporal.tls.key-file")) {
                val file = property(name).orEmpty()
                require(file.isNotBlank() && Files.isReadable(Path.of(file))) { "$name must point to a readable PEM file" }
            }
            val mode = property("app.mode") ?: "api"
            require(mode in setOf("api", "worker")) { "APP_MODE must be api or worker" }
            if (mode == "api") {
                require(property("integration.cursor-secret").orEmpty().toByteArray(Charsets.UTF_8).size >= 32) {
                    "INTEGRATION_CURSOR_SECRET must contain at least 32 bytes"
                }
            }
            require(tasks.isNotEmpty()) { "Configure at least one integration.tasks entry" }
            require(!allowHttp) { "Plain HTTP adapters cannot be enabled in production" }
            for ((name, target) in tasks) {
                require(name.matches(Regex("[A-Za-z][A-Za-z0-9._-]{0,63}"))) { "Task names must use 1-64 simple letters, numbers, dots, _ or -" }
                require(target.readOnly || !target.undoUrl.isNullOrBlank() || target.allowIrreversible) {
                    "A write task needs an undo URL or explicit allow-irreversible=true"
                }
                for (url in listOfNotNull(target.url, target.undoUrl)) {
                    val uri = URI.create(url)
                    require(uri.scheme == "https" && !uri.host.isNullOrBlank() && uri.userInfo == null && uri.fragment == null) {
                        "Production adapter URLs must use HTTPS with no embedded credentials or fragments"
                    }
                }
            }
            val issuer = property("app.auth.issuer-uri").orEmpty()
            require(issuer.isNotBlank() && URI.create(issuer).scheme == "https") { "AUTH_ISSUER_URI must use HTTPS" }
            require(!property("app.auth.audience").isNullOrBlank()) { "AUTH_AUDIENCE is required" }
        }
    }
}
