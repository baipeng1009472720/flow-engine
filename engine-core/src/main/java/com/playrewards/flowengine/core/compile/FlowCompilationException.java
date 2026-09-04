package com.playrewards.flowengine.core.compile;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Reports every deterministic issue found in a compile attempt.
 *
 * <p>The compiler orders issues before constructing this exception, so callers may safely expose the
 * list in diagnostics and compare repeated attempts.</p>
 */
public final class FlowCompilationException extends IllegalArgumentException {

    private final List<CompilationIssue> issues;

    /**
     * Creates a compilation failure from a non-empty ordered issue list.
     *
     * @param issues immutable diagnostic source
     */
    public FlowCompilationException(List<CompilationIssue> issues) {
        super(message(issues));
        this.issues = List.copyOf(issues);
        if (this.issues.isEmpty()) {
            throw new IllegalArgumentException("compilation issues must not be empty");
        }
    }

    /** @return ordered immutable compilation issues */
    public List<CompilationIssue> issues() {
        return issues;
    }

    private static String message(List<CompilationIssue> issues) {
        Objects.requireNonNull(issues, "compilation issues must not be null");
        return issues.stream().map(CompilationIssue::message).collect(Collectors.joining("; "));
    }
}
