package com.temporal.sample.workflow

import io.temporal.activity.ActivityInterface

@ActivityInterface
interface UserOrderActivities {
    // Do one outside task; Temporal may call it again after a failure.
    fun runStep(userId: String, stepName: String): String
}
