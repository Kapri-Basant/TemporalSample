package com.temporal.sample.integration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "integration")
class IntegrationProperties {
    var tasks: Map<String, TaskTarget> = emptyMap()
    var allowHttp: Boolean = false
}

data class TaskTarget(
    var url: String = "",
    var undoUrl: String? = null,
    var token: String? = null,
    var readOnly: Boolean = false,
    var allowIrreversible: Boolean = false
)
