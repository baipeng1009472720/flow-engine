package com.playrewards.flowengine.contract.fixture;

import com.playrewards.flowengine.core.EngineCoreMarker;

/** Test-only fixture proving contract code cannot depend on an implementation module. */
public final class ProhibitedContractToCoreDependency {
    private EngineCoreMarker marker;
}
