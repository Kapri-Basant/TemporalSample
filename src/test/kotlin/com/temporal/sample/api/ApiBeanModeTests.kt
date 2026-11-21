package com.temporal.sample.api

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertFalse
import kotlin.test.assertNull

class ApiBeanModeTests {
    // A production worker must not need API cursor keys or create API-only services.
    @Test
    fun `worker does not create API components`() {
        ApplicationContextRunner().withPropertyValues("app.mode=worker", "spring.profiles.active=prod")
            .withUserConfiguration(JobCursor::class.java, JobRequests::class.java, JobService::class.java, Caller::class.java)
            .run { context ->
                assertNull(context.startupFailure)
                assertFalse(context.containsBean("jobCursor"))
                assertFalse(context.containsBean("jobRequests"))
                assertFalse(context.containsBean("jobService"))
                assertFalse(context.containsBean("caller"))
            }
    }
}
