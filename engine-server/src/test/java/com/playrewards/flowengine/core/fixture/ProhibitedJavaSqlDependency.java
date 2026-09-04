package com.playrewards.flowengine.core.fixture;

import java.sql.Connection;

/** Test-only fixture proving protected layers reject JDBC dependencies. */
public final class ProhibitedJavaSqlDependency {
    private Connection connection;
}
