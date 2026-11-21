package com.temporal.sample.integration

// Defaults let Temporal read old payloads when optional fields are added later.
data class JobInput(
    val jobId: String = "",
    val ownerId: String = "",
    val steps: List<JobStep> = emptyList()
)

data class JobStep(
    val task: String = "",
    val input: Map<String, String> = emptyMap(),
    val waitSeconds: Int = 0,
    val undo: Boolean = false,
    val sideEffect: Boolean = true
)

data class TaskCall(
    val jobId: String = "",
    val ownerId: String = "",
    val index: Int = 0,
    val task: String = "",
    val input: Map<String, String> = emptyMap(),
    val idempotencyKey: String = "",
    val previousReceipts: Map<String, String> = emptyMap()
)

data class TaskResult(
    val receipt: String = "",
    val output: Map<String, String> = emptyMap(),
    val podName: String? = null
)

data class JobStepState(
    val index: Int = 0,
    val task: String = "",
    val status: String = "PENDING",
    val receipt: String? = null,
    val podName: String? = null,
    val error: String? = null,
    val output: Map<String, String> = emptyMap()
)

data class JobSnapshot(
    val jobId: String = "",
    val ownerId: String = "",
    val status: String = "NOT_STARTED",
    val currentStep: Int = -1,
    val steps: List<JobStepState> = emptyList(),
    val error: String? = null
)
