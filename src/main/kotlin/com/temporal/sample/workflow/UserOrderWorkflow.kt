package com.temporal.sample.workflow

import io.temporal.workflow.QueryMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

@WorkflowInterface
interface UserOrderWorkflow {
    // Keep the five order steps in order, with a saved wait after each.
    @WorkflowMethod
    fun run(userId: String, stepTimersSec: List<Int>): String

    // Read the current step without changing workflow state.
    @QueryMethod
    fun currentStep(): String

    // Read steps whose activity and wait have both finished.
    @QueryMethod
    fun completedSteps(): List<String>

    // Read messages rebuilt from the saved workflow history.
    @QueryMethod
    fun timeline(): List<String>
}
