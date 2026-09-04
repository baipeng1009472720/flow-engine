package com.playrewards.flowengine.adapters.fixture;

import com.playrewards.flowengine.core.EngineCoreMarker;

/** Test-only fixture proving adapters cannot depend on core. */
public final class ProhibitedAdapterToCoreDependency {
    private EngineCoreMarker marker;
}
