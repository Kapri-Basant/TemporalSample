package com.temporal.sample.integration

import io.temporal.activity.ActivityInterface

@ActivityInterface
interface JobActivities {
    // Run one configured task using a stable key for every attempt.
    fun execute(call: TaskCall): TaskResult

    // Undo a task; the provider must also handle a forward call with a lost reply.
    fun undo(call: TaskCall): TaskResult
}
