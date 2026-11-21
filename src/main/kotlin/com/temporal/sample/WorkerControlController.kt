package com.temporal.sample

import org.springframework.context.annotation.Profile
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.slf4j.LoggerFactory

@RestController
@Profile("local")
@RequestMapping("/worker/control")
@ConditionalOnProperty(name = ["app.mode"], havingValue = "worker")
class WorkerControlController(
    private val worker: WorkerModeRunner,
    @Value("\${app.allow-crash:false}") private val allowCrash: Boolean
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // Tell readiness probes whether this worker started its pollers.
    @GetMapping("/status")
    fun status(): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(if (worker.isReady()) 200 else 503).body(
            mapOf(
                "message" to "worker is running",
                "podName" to (System.getenv("POD_NAME") ?: "unknown-pod"),
                "podNamespace" to (System.getenv("POD_NAMESPACE") ?: "temporal-demo"),
                "pollersStarted" to worker.isReady().toString()
            )
        )

    // Accept a test crash; Kubernetes can then restart this container.
    @PostMapping("/crash")
    fun crash(): ResponseEntity<Map<String, String>> {
        if (!allowCrash) {
            return ResponseEntity.status(403).body(mapOf("message" to "Crash control is disabled"))
        }
        val podName = System.getenv("POD_NAME") ?: "unknown-pod"
        log.info("pod={} action=POD_CRASH_REQUESTED event=process_will_exit", podName)
        log.info("pod={} action=POD_RESTART_EXPECTED event=kubernetes_restart_policy", podName)
        Thread {
            Thread.sleep(500)
            // Stop at once so this test really mimics a process crash.
            Runtime.getRuntime().halt(1)
        }.start()
        return ResponseEntity.accepted().body(mapOf("message" to "worker accepted crash request"))
    }
}
