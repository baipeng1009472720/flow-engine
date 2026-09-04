package com.playrewards.flowengine.contract.execution;

/** Lifecycle state shared by execution results, events and durable checkpoints. */
public enum ExecutionStatus {
    CREATED,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    SKIPPED,
    RETRYING,
    WAITING_RETRY,
    WAITING_MANUAL
}
