package com.temporal.sample

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@org.springframework.test.context.ActiveProfiles("local")
@SpringBootTest
class TemporalApplicationTests {

    // Check that the API can start without connecting to Temporal.
    @Test
    fun contextLoads() {
    }

}
