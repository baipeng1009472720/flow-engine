package com.playrewards.flowengine.core.execution;

import java.util.Objects;

/** Immutable execution-scope identity used to separate branch, loop and child-flow node writes. */
public record ScopePath(String value) {

    private static final ScopePath ROOT = new ScopePath("root");

    public ScopePath {
        Objects.requireNonNull(value, "scope path must not be null");
        value = value.strip();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("scope path must not be blank");
        }
    }

    /** @return the single root scope used by a non-structured EPHEMERAL flow */
    public static ScopePath root() {
        return ROOT;
    }

    /**
     * Creates a stable descendant path without mutating the parent.
     *
     * @param segment non-blank branch, loop or child-execution segment
     * @return descendant path
     */
    public ScopePath child(String segment) {
        Objects.requireNonNull(segment, "scope segment must not be null");
        String normalized = segment.strip();
        if (normalized.isEmpty() || normalized.contains("/")) {
            throw new IllegalArgumentException("scope segment must be non-blank and must not contain '/'");
        }
        return new ScopePath(value + "/" + normalized);
    }

    /** @return true when this path is equal to or below the supplied ancestor */
    public boolean isWithin(ScopePath ancestor) {
        Objects.requireNonNull(ancestor, "scope ancestor must not be null");
        return value.equals(ancestor.value) || value.startsWith(ancestor.value + "/");
    }

    /** Finds the deepest stable ancestor shared with another active scope. */
    public ScopePath commonAncestor(ScopePath other) {
        Objects.requireNonNull(other, "other scope path must not be null");
        String[] left = value.split("/");
        String[] right = other.value.split("/");
        int shared = 0;
        while (shared < left.length && shared < right.length && left[shared].equals(right[shared])) {
            shared++;
        }
        if (shared == 0) {
            throw new IllegalArgumentException("scope paths do not share a root");
        }
        if (shared > 1 && (left[shared - 1].startsWith("parallel:")
                || left[shared - 1].startsWith("loop:")
                || left[shared - 1].startsWith("subflow:"))) {
            shared--;
        }
        return new ScopePath(String.join("/", java.util.Arrays.copyOf(left, shared)));
    }
}
