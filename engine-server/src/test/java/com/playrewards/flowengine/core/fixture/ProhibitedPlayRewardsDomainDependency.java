package com.playrewards.flowengine.core.fixture;

import com.playrewards.domain.fixture.LegacyDomainFixture;

/** Test-only fixture proving protected layers reject legacy PlayRewards domain dependencies. */
public final class ProhibitedPlayRewardsDomainDependency {
    private LegacyDomainFixture legacyDomainFixture;
}
