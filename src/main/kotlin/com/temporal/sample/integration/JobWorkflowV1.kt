package com.temporal.sample.integration

import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

@WorkflowInterface
interface JobWorkflowV1 {
    // Run a saved set of named tasks without trusting caller-supplied URLs.
    @WorkflowMethod(name = "JobWorkflowV1")
    fun run(input: JobInput): JobSnapshot

    // Read the latest state without changing the job.
    @QueryMethod
    fun snapshot(): JobSnapshot

    // Ask the job to stop and undo safe tasks in reverse order.
    @SignalMethod
    fun cancel()
}
