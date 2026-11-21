package com.temporal.sample

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TemporalApplication

// Start the backend service.
fun main(args: Array<String>) {
    runApplication<TemporalApplication>(*args)
}
