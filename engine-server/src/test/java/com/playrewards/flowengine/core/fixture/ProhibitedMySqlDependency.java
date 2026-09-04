package com.playrewards.flowengine.core.fixture;

import com.mysql.cj.jdbc.Driver;

/** Test-only fixture proving protected layers reject MySQL driver dependencies. */
public final class ProhibitedMySqlDependency {
    private Driver driver;
}
