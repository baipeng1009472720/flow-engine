package com.playrewards.flowengine.adapter.mysql;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;

import org.apache.ibatis.annotations.*;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.flywaydb.core.Flyway;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import javax.sql.DataSource;

/**
 * MyBatis-Plus transaction boundary for runtime adapters. SQL templates are adapter-owned
 * constants; all tenant identifiers and values are bound parameters. Exceptions never expose SQL
 * values.
 */
public final class MySqlRuntimeDatabase {
    private static final int CAPACITY_STRIPES = 64;
    private static final int CAPACITY_ADMISSION_LIMIT = 256;

    private final DataSource source;
    private final SqlSessionFactory sessions;
    private final SqlSessionFactory controlSessions;
    private final ReentrantLock[] capacityTransactions = new ReentrantLock[CAPACITY_STRIPES];
    private final Semaphore capacityAdmissions = new Semaphore(CAPACITY_ADMISSION_LIMIT, true);

    public MySqlRuntimeDatabase(DataSource source) {
        this.source = Objects.requireNonNull(source);
        Arrays.setAll(capacityTransactions, ignored -> new ReentrantLock(true));
        this.sessions = sessions("runtime", source);
        this.controlSessions =
                source instanceof org.apache.ibatis.datasource.pooled.PooledDataSource pool
                        ? sessions("runtime-control", unpooled(pool))
                        : sessions;
    }

    private static SqlSessionFactory sessions(String name, DataSource source) {
        var config = new MybatisConfiguration();
        config.setEnvironment(new Environment(name, new JdbcTransactionFactory(), source));
        config.setLocalCacheScope(LocalCacheScope.STATEMENT);
        config.addMapper(RuntimeMapper.class);
        return new MybatisSqlSessionFactoryBuilder().build(config);
    }

    private static DataSource unpooled(
            org.apache.ibatis.datasource.pooled.PooledDataSource pool) {
        return new org.apache.ibatis.datasource.unpooled.UnpooledDataSource(
                pool.getDriver(), pool.getUrl(), pool.getUsername(), pool.getPassword());
    }

    /**
     * Applies only versioned engine-owned migrations, independent from any host business schema.
     */
    public void migrate() {
        // Flyway can inspect a connection after closing it; a MyBatis pooled proxy rejects that
        // call.
        DataSource migrationSource = source;
        if (source instanceof org.apache.ibatis.datasource.pooled.PooledDataSource pool)
            migrationSource = unpooled(pool);
        Flyway.configure()
                .dataSource(migrationSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    /** Commit on successful return; any exception rolls the whole operation back. */
    <T> T transaction(Function<Tx, T> action) {
        return transaction(sessions, action);
    }

    /**
     * 以 READ COMMITTED 运行一个短事务，供分批保留清理使用。
     *
     * <p>RC 下范围 DELETE 只锁定实际命中的过期行而不加间隙锁，避免清理阻塞或死锁正在插入新观察记录
     * 的写事务；调用方仍需自行限制单批行数。
     */
    <T> T readCommittedTransaction(Function<Tx, T> action) {
        return transaction(
                sessions,
                tx -> {
                    // 必须是事务内第一条语句：SET TRANSACTION 只作用于随后开启的这一个事务。
                    tx.update("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    return action.apply(tx);
                });
    }

    /**
     * Bypasses a saturated workload pool for lease renewal. The coordinator must keep this control
     * path bounded independently; it is intentionally unavailable outside the MySQL adapter.
     */
    <T> T controlTransaction(Function<Tx, T> action) {
        return transaction(controlSessions, action);
    }

    /**
     * Queues same-tenant capacity writers before they borrow a workload connection. The fixed
     * stripe set is memory-bounded; the database row lock remains authoritative across processes.
     */
    <T> T capacityTransaction(String tenant, Function<Tx, T> action) {
        String requiredTenant = Objects.requireNonNull(tenant);
        if (!capacityAdmissions.tryAcquire())
            throw new IllegalStateException("RUNTIME_STORAGE_CAPACITY_BUSY");
        int stripe = Math.floorMod(requiredTenant.hashCode(), CAPACITY_STRIPES);
        ReentrantLock lock = capacityTransactions[stripe];
        boolean locked = false;
        try {
            try {
                lock.lockInterruptibly();
                locked = true;
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("RUNTIME_STORAGE_CAPACITY_INTERRUPTED");
            }
            return transaction(action);
        } finally {
            if (locked) lock.unlock();
            capacityAdmissions.release();
        }
    }

    /**
     * Returns identity-free process-local admission use for capacity monitoring.
     *
     * @return current admitted capacity transactions and their fixed upper bound
     */
    public CapacitySnapshot capacitySnapshot() {
        return new CapacitySnapshot(
                CAPACITY_ADMISSION_LIMIT - capacityAdmissions.availablePermits(),
                CAPACITY_ADMISSION_LIMIT);
    }

    /** Process-local storage admission values; no tenant or payload identity is exposed. */
    public record CapacitySnapshot(int inFlight, int limit) {}

    private <T> T transaction(SqlSessionFactory factory, Function<Tx, T> action) {
        try (var session = factory.openSession(false)) {
            try {
                T value = action.apply(new Tx(this, session.getMapper(RuntimeMapper.class)));
                session.commit();
                return value;
            } catch (RuntimeException failure) {
                session.rollback();
                if (failure instanceof IllegalArgumentException
                        || failure instanceof IllegalStateException) throw failure;
                throw new IllegalStateException("RUNTIME_STORAGE_TRANSACTION_FAILED");
            }
        }
    }

    static final class Tx {
        private final MySqlRuntimeDatabase database;
        private final RuntimeMapper mapper;

        Tx(MySqlRuntimeDatabase database, RuntimeMapper mapper) {
            this.database = database;
            this.mapper = mapper;
        }

        /** 当前事务所属的数据库，仅供适配器内需要独立短事务的边界（如容量计数播种）使用。 */
        MySqlRuntimeDatabase database() {
            return database;
        }

        int update(String sql, Object... args) {
            return mapper.update(Map.of("statement", sql, "args", args));
        }

        List<Map<String, Object>> rows(String sql, Object... args) {
            return mapper.rows(Map.of("statement", sql, "args", args));
        }

        Optional<Map<String, Object>> row(String sql, Object... args) {
            return rows(sql, args).stream().findFirst();
        }
    }

    /** Private adapter SQL bridge. No public API accepts SQL or caller-provided identifiers. */
    interface RuntimeMapper {
        @Update("${statement}")
        int update(Map<String, Object> parameters);

        @Select("${statement}")
        List<Map<String, Object>> rows(Map<String, Object> parameters);
    }

    static long number(Map<String, Object> row, String name) {
        return ((Number) row.get(name)).longValue();
    }

    static String text(Map<String, Object> row, String name) {
        return Objects.toString(row.get(name), null);
    }
}
