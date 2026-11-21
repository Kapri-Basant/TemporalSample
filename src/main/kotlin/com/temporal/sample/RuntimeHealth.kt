package com.temporal.sample

import io.temporal.api.workflowservice.v1.GetSystemInfoRequest
import io.temporal.serviceclient.WorkflowServiceStubs
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component("temporalReadiness")
class TemporalReadiness(private val service: WorkflowServiceStubs) : HealthIndicator {
    // Stop routing new work here if the secured Temporal connection is not available.
    override fun health(): Health = try {
        service.blockingStub().withDeadlineAfter(2, TimeUnit.SECONDS).getSystemInfo(GetSystemInfoRequest.getDefaultInstance())
        Health.up().build()
    } catch (_: Exception) {
        Health.down().build()
    }
}

@Component("workerReadiness")
@ConditionalOnProperty(name = ["app.mode"], havingValue = "worker")
class WorkerReadiness(private val worker: WorkerModeRunner) : HealthIndicator {
    // Show ready only after this process starts its registered task pollers.
    override fun health(): Health = if (worker.isReady()) Health.up().build() else Health.down().build()
}
