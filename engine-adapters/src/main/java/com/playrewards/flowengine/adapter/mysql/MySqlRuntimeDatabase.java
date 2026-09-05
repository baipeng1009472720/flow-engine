package com.playrewards.flowengine.adapter.mysql;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;

import org.apache.ibatis.annotations.*;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.flywaydb.core.Flyway;

import java.util.*;
import java.util.function.Function;

import javax.sql.DataSource;

/**
 * MyBatis-Plus transaction boundary for runtime adapters. SQL templates are adapter-owned
 * constants; all tenant identifiers and values are bound parameters. Exceptions never expose SQL
 * values.
 */
public final class MySqlRuntimeDatabase {
    private final DataSource source;
    private final SqlSessionFactory sessions;

    public MySqlRuntimeDatabase(DataSource source) {
        this.source = Objects.requireNonNull(source);
        var config = new MybatisConfiguration();
        config.setEnvironment(new Environment("runtime", new JdbcTransactionFactory(), source));
        config.setLocalCacheScope(LocalCacheScope.STATEMENT);
        config.addMapper(RuntimeMapper.class);
        sessions = new MybatisSqlSessionFactoryBuilder().build(config);
    }

    /**
     * Applies only versioned engine-owned migrations, independent from any host business schema.
     */
    public void migrate() {
        // Flyway can inspect a connection after closing it; a MyBatis pooled proxy rejects that
        // call.
        DataSource migrationSource = source;
        if (source instanceof org.apache.ibatis.datasource.pooled.PooledDataSource pool)
            migrationSource =
                    new org.apache.ibatis.datasource.unpooled.UnpooledDataSource(
                            pool.getDriver(),
                            pool.getUrl(),
                            pool.getUsername(),
                            pool.getPassword());
        Flyway.configure()
                .dataSource(migrationSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    /** Commit on successful return; any exception rolls the whole operation back. */
    <T> T transaction(Function<Tx, T> action) {
        try (var session = sessions.openSession(false)) {
            try {
                T value = action.apply(new Tx(session.getMapper(RuntimeMapper.class)));
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
        private final RuntimeMapper mapper;

        Tx(RuntimeMapper mapper) {
            this.mapper = mapper;
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
