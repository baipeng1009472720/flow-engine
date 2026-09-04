package com.playrewards.flowengine.adapter.fixture;

import com.playrewards.flowengine.core.EngineCoreMarker;

/** Test-only fixture proving adapter code cannot depend on core. */
public final class ProhibitedAdapterToCoreDependency {
    private EngineCoreMarker marker;
}
