package com.playrewards.flowengine.core.compile;

/** A stable, machine-readable problem found while compiling one executable bundle. */
public record CompilationIssue(String code, String nodeId, String path, String message) {
}
