package com.playrewards.flowengine.adapter.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

/** V1 stays immutable: an existing database upgrades to latest without rewriting runtime data. */
class MySqlMigrationUpgradeTest {
    static final TestMySql MYSQL = TestMySql.start();

    @Test
    void upgradesExistingV1AndAddsPayloadExecutionIndexWithoutLosingCheckpoint() {
        var source = new MysqlDataSource();
        source.setURL(MYSQL.getJdbcUrl());
        source.setUser(MYSQL.getUsername());
        source.setPassword(MYSQL.getPassword());
        Flyway.configure()
                .dataSource(source)
                .locations("classpath:db/migration")
                .target("1")
                .load()
                .migrate();
        var db = new MySqlRuntimeDatabase(source);
        db.transaction(
                tx ->
                        tx.update(
                                "INSERT INTO"
                                    + " engine_execution_state(tenant_id,execution_id,revision,status,checkpoint)"
                                    + " VALUES('upgrade','execution',1,'RUNNING','{}')"));
        db.migrate();
        assertEquals(
                1,
                new MySqlExecutionStateStore(db, "upgrade")
                        .find("execution")
                        .orElseThrow()
                        .revision());
        long count =
                db.transaction(
                        tx ->
                                MySqlRuntimeDatabase.number(
                                        tx.row("SELECT COUNT(*) AS n FROM engine_business_lock")
                                                .orElseThrow(),
                                        "n"));
        assertEquals(0L, count);
        long payloadExecutionIndexColumns =
                db.transaction(
                        tx ->
                                MySqlRuntimeDatabase.number(
                                        tx.row(
                                                        "SELECT COUNT(*) AS n FROM"
                                                            + " information_schema.statistics WHERE"
                                                            + " table_schema=DATABASE() AND"
                                                            + " table_name='engine_payload' AND"
                                                            + " index_name='ix_payload_execution'")
                                                .orElseThrow(),
                                        "n"));
        assertEquals(4L, payloadExecutionIndexColumns);
    }
}
