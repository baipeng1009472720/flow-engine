package com.playrewards.flowengine.core.fixture;

import com.playrewards.flowengine.adapter.groovy.SandboxedGroovyRuntime;

/** Test-only fixture proving core code cannot depend on an adapter implementation. */
public final class ProhibitedCoreToAdapterDependency {
    private SandboxedGroovyRuntime runtime;
}
