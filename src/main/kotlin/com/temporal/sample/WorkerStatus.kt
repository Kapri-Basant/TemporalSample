package com.temporal.sample

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.Duration
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

data class WorkerStatusResponse(
    val workerStatus: String,
    val podStatus: String,
    val podName: String,
    val restartCount: Int = 0,
    val source: String,
    val timeline: List<String> = emptyList(),
    val details: String = "",
    val crashEnabled: Boolean = false
)

@Component
class WorkerStatus(
    @Value("\${worker.control.base-url:http://temporal-demo-worker-control:8080}") private val controlUrl: String
) {
    private val json = ObjectMapper()
    private val serviceAccount = Path.of("/var/run/secrets/kubernetes.io/serviceaccount")
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    private val clusterHttp by lazy {
        val certificate = Files.newInputStream(serviceAccount.resolve("ca.crt")).use {
            CertificateFactory.getInstance("X.509").generateCertificate(it)
        }
        val store = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null, null)
            setCertificateEntry("cluster", certificate)
        }
        val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(store) }
        val tls = SSLContext.getInstance("TLS").apply { init(null, trust.trustManagers, null) }
        HttpClient.newBuilder().sslContext(tls).connectTimeout(Duration.ofSeconds(2)).build()
    }

    // Read pod state and recent Kubernetes events with read-only access.
    fun read(): WorkerStatusResponse {
        if (!Files.exists(serviceAccount.resolve("token"))) return readHttp()
        return try {
            val namespace = Files.readString(serviceAccount.resolve("namespace")).trim()
            val base = "/api/v1/namespaces/$namespace"
            val pods = get("$base/pods?labelSelector=app%3Dtemporal-demo-worker").path("items")
            // Prefer the current pod during a rolling update.
            val pod = pods.filter { it.path("metadata").path("deletionTimestamp").isMissingNode }
                .maxByOrNull { it.path("metadata").path("creationTimestamp").asText() }
                ?: return WorkerStatusResponse("UNAVAILABLE", "No worker pod", "unknown", source = "KUBERNETES")
            val events = try {
                get("$base/events?fieldSelector=involvedObject.uid%3D${pod.path("metadata").path("uid").asText()}").path("items").toList()
            } catch (_: Exception) { emptyList() }
            project(pod, events)
        } catch (ex: Exception) {
            WorkerStatusResponse("UNKNOWN", "Kubernetes status unavailable", "unknown", source = "KUBERNETES",
                details = "Could not read pod status; check API access and namespace permissions")
        }
    }

    // Read one Kubernetes API resource using the cluster certificate.
    private fun get(path: String): JsonNode {
        // Read the token again because Kubernetes rotates it.
        val token = Files.readString(serviceAccount.resolve("token")).trim()
        val request = HttpRequest.newBuilder(URI.create("https://kubernetes.default.svc$path"))
            .timeout(Duration.ofSeconds(3)).header("Authorization", "Bearer $token").GET().build()
        val response = clusterHttp.send(request, HttpResponse.BodyHandlers.ofString())
        check(response.statusCode() == 200) { "Kubernetes API status ${response.statusCode()}" }
        return json.readTree(response.body())
    }

    // Check worker reachability when running outside Kubernetes.
    private fun readHttp(): WorkerStatusResponse = try {
        val response = http.send(HttpRequest.newBuilder(URI.create("$controlUrl/worker/control/status"))
            .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString())
        val body = json.readTree(response.body())
        WorkerStatusResponse(if (response.statusCode() == 200) "UP" else "UNAVAILABLE", "HTTP reachability only",
            body.path("podName").asText("unknown"), source = "HTTP")
    } catch (_: Exception) {
        WorkerStatusResponse("UNAVAILABLE", "HTTP unreachable; pod state unknown", "unknown", source = "HTTP")
    }

    companion object {
        // Turn real pod fields into status and restart events.
        fun project(pod: JsonNode, events: List<JsonNode>): WorkerStatusResponse {
            val name = pod.path("metadata").path("name").asText("unknown")
            val uid = pod.path("metadata").path("uid").asText()
            val status = pod.path("status")
            val container = status.path("containerStatuses").firstOrNull { it.path("name").asText() == "worker" }
            val restarts = container?.path("restartCount")?.asInt() ?: 0
            val ready = status.path("conditions").any { it.path("type").asText() == "Ready" && it.path("status").asText() == "True" }
            val waiting = container?.path("state")?.path("waiting")?.path("reason")?.asText("") ?: ""
            val phase = status.path("phase").asText("Unknown")
            val state = when {
                ready -> "UP"
                restarts > 0 && phase !in listOf("Failed", "Succeeded") -> "RESTARTING"
                phase in listOf("Pending", "Running") -> "STARTING"
                else -> "UNAVAILABLE"
            }
            val timeline = mutableListOf<String>()
            val previous = container?.path("lastState")?.path("terminated")
            if (previous != null && !previous.isMissingNode) {
                timeline.add("${previous.path("finishedAt").asText()}|id=$uid:exit:$restarts|pod=$name|action=CONTAINER_EXITED|detail=exitCode=${previous.path("exitCode").asInt()} reason=${previous.path("reason").asText()}")
            }
            val running = container?.path("state")?.path("running")
            if (running != null && !running.isMissingNode) {
                timeline.add("${running.path("startedAt").asText()}|id=$uid:start:$restarts|pod=$name|action=CONTAINER_STARTED|detail=restartCount=$restarts")
            }
            for (event in events) {
                val time = event.path("lastTimestamp").asText("").ifBlank {
                    event.path("eventTime").asText("").ifBlank { event.path("metadata").path("creationTimestamp").asText() }
                }
                val id = event.path("metadata").path("uid").asText()
                timeline.add("$time|id=$id:${event.path("count").asInt()}|pod=$name|action=${event.path("reason").asText()}|detail=${event.path("message").asText()}")
            }
            return WorkerStatusResponse(state, "$phase${if (waiting.isNotEmpty()) " / $waiting" else ""} / Ready=$ready",
                name, restarts, "KUBERNETES", timeline.sorted())
        }
    }
}
