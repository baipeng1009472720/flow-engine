package com.playrewards.flowengine.server;

import org.testcontainers.containers.MySQLContainer;

import java.sql.DriverManager;
import java.util.UUID;

/**
 * 真实 MySQL 测试连接来源：优先使用环境变量指定的外部 MySQL，否则启动一次性 Testcontainers 容器。
 *
 * <p>外部模式为每个测试类创建独立随机库，避免与本机业务库或其它测试互相污染；不负责删除该库， 便于失败后排查。凭据只从环境读取，不写入日志或断言消息。
 */
public final class TestMySql {
    private static final String EXTERNAL_URL = "FLOW_ENGINE_TEST_MYSQL_URL";
    private static final String EXTERNAL_USER = "FLOW_ENGINE_TEST_MYSQL_USER";
    private static final String EXTERNAL_PASSWORD = "FLOW_ENGINE_TEST_MYSQL_PASSWORD";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private TestMySql(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * 返回一个干净数据库的连接参数。
     *
     * @return 外部 MySQL 上的新库，或已启动容器的默认库
     * @throws IllegalStateException 外部库无法创建，或 Docker 不可用
     */
    public static TestMySql start() {
        String base = System.getenv(EXTERNAL_URL);
        if (base == null || base.isBlank()) return container();
        String user = System.getenv().getOrDefault(EXTERNAL_USER, "root");
        String secret = System.getenv().getOrDefault(EXTERNAL_PASSWORD, "");
        String schema = "flow_engine_test_" + UUID.randomUUID().toString().replace("-", "");
        String server = base.endsWith("/") ? base : base + "/";
        try (var connection = DriverManager.getConnection(server, user, secret);
                var statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + schema);
        } catch (java.sql.SQLException failure) {
            throw new IllegalStateException("external test MySQL unavailable", failure);
        }
        return new TestMySql(
                server + schema + "?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true",
                user,
                secret);
    }

    @SuppressWarnings("resource")
    private static TestMySql container() {
        var mysql = new MySQLContainer<>("mysql:8.4");
        mysql.start();
        return new TestMySql(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
