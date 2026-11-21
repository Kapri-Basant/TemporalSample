package com.temporal.sample

import com.uber.m3.tally.RootScopeBuilder
import com.uber.m3.tally.Scope
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.micrometer.core.instrument.MeterRegistry
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.common.reporter.MicrometerClientStatsReporter
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.File
import java.time.Duration

const val USER_DEMO_TASK_QUEUE = "user-demo-queue"
const val USER_DEMO_WORKFLOW_PREFIX = "user-demo-"

@Configuration
class TemporalConfig {
    // Publish SDK task polls, retries, and latency through the application's meter registry.
    @Bean(destroyMethod = "close")
    fun temporalMetrics(registry: MeterRegistry): Scope = RootScopeBuilder()
        .reporter(MicrometerClientStatsReporter(registry))
        .reportEvery(com.uber.m3.util.Duration.ofSeconds(10.0))

    // Share one verified Temporal connection and close it when the app stops.
    @Bean(destroyMethod = "shutdown")
    fun workflowServiceStubs(
        metrics: Scope,
        guard: ProductionGuard,
        @Value("\${temporal.target}") target: String,
        @Value("\${temporal.tls.enabled:false}") tls: Boolean,
        @Value("\${temporal.tls.cert-file:}") certFile: String,
        @Value("\${temporal.tls.key-file:}") keyFile: String,
        @Value("\${temporal.tls.ca-file:}") caFile: String
    ): WorkflowServiceStubs {
        val options = WorkflowServiceStubsOptions.newBuilder()
            .setTarget(target)
            .setRpcTimeout(Duration.ofSeconds(3))
            .setRpcQueryTimeout(Duration.ofSeconds(3))
            .setMetricsScope(metrics)
        if (tls) {
            val ssl = GrpcSslContexts.forClient()
            if (certFile.isNotBlank() || keyFile.isNotBlank()) {
                require(certFile.isNotBlank() && keyFile.isNotBlank()) { "Both Temporal client certificate and key are required" }
                ssl.keyManager(File(certFile), File(keyFile))
            }
            if (caFile.isNotBlank()) ssl.trustManager(File(caFile))
            options.setSslContext(ssl.build())
        }
        return WorkflowServiceStubs.newServiceStubs(options.build())
    }

    // Use one client and record this pod's name with the tasks it handles.
    @Bean
    fun workflowClient(
        workflowServiceStubs: WorkflowServiceStubs,
        @Value("\${temporal.namespace:default}") namespace: String
    ): WorkflowClient {
        val options = WorkflowClientOptions.newBuilder()
            .setIdentity(System.getenv("POD_NAME") ?: "local-worker")
            .setNamespace(namespace)
            .build()
        return WorkflowClient.newInstance(workflowServiceStubs, options)
    }
}
