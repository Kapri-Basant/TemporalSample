package com.temporal.sample

import java.util.concurrent.TimeUnit
import com.temporal.sample.integration.HttpJobActivities
import com.temporal.sample.integration.JobWorkflowV1Impl
import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles
import io.temporal.worker.WorkerOptions
import com.temporal.sample.workflow.UserOrderActivitiesImpl
import com.temporal.sample.workflow.UserOrderWorkflowImpl
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(name = ["app.mode"], havingValue = "worker")
class WorkerModeRunner(
    private val workflowClient: WorkflowClient,
    private val jobActivities: HttpJobActivities,
    private val environment: Environment,
    @Value("\${integration.task-queue:integration-v1}") private val integrationQueue: String,
    @Value("\${temporal.task-queue:$USER_DEMO_TASK_QUEUE}") private val taskQueue: String
) : CommandLineRunner {
    private val log = LoggerFactory.getLogger(javaClass)
    private lateinit var factory: WorkerFactory

    // Register the workflow and activities, then start task polling.
    override fun run(vararg args: String) {
        val podName = System.getenv("POD_NAME") ?: "unknown-pod"
        val namespace = System.getenv("POD_NAMESPACE") ?: "temporal-demo"
        log.info("pod={} namespace={} action=WORKER_START event=process_started", podName, namespace)
        factory = WorkerFactory.newInstance(workflowClient)
        val options = WorkerOptions.newBuilder()
            .setMaxConcurrentActivityExecutionSize(10)
            .setMaxConcurrentWorkflowTaskExecutionSize(10)
            .build()
        val integrationWorker = factory.newWorker(integrationQueue, options)
        integrationWorker.registerWorkflowImplementationTypes(JobWorkflowV1Impl::class.java)
        integrationWorker.registerActivitiesImplementations(jobActivities)
        // Keep the old workflow type unchanged so existing local runs can still replay.
        if (environment.acceptsProfiles(Profiles.of("local"))) {
            check(integrationQueue != taskQueue) { "Use separate queues for the local exercise and the integration API" }
            val worker = factory.newWorker(taskQueue, options)
            worker.registerWorkflowImplementationTypes(UserOrderWorkflowImpl::class.java)
            worker.registerActivitiesImplementations(UserOrderActivitiesImpl())
        }
        log.info("pod={} namespace={} action=WORKER_REGISTERED taskQueue={} event=waiting_for_workflow_tasks", podName, namespace, taskQueue)
        factory.start()
        log.info("pod={} namespace={} action=WORKER_POLL_STARTED taskQueue={} event=pollers_started", podName, namespace, integrationQueue)
    }

    // Report whether this process has started its task pollers.
    fun isReady(): Boolean = ::factory.isInitialized && factory.isStarted && !factory.isShutdown

    // Stop new work and give running tasks time to finish.
    @PreDestroy
    fun closeFactory() {
        if (::factory.isInitialized) {
            factory.shutdown()
            factory.awaitTermination(25, TimeUnit.SECONDS)
            if (!factory.isTerminated) factory.shutdownNow()
        }
    }
}
