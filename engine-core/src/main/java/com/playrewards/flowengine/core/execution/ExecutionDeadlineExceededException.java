package com.playrewards.flowengine.core.execution;

/** Signals that scheduling, permit acquisition or a node invocation crossed its execution deadline. */
public final class ExecutionDeadlineExceededException extends RuntimeException {

    /** Creates a deadline failure at a scheduling or invocation boundary. */
    public ExecutionDeadlineExceededException(String message) {
        super(message);
    }

    /** Creates a deadline failure while retaining its internal timeout cause for diagnostics. */
    public ExecutionDeadlineExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
