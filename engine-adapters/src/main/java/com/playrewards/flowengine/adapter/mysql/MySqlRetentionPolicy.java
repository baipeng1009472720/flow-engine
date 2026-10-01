package com.playrewards.flowengine.adapter.mysql;

import java.time.Duration;
import java.util.Objects;

/**
 * MySQL 观察记录保留策略。
 *
 * <p>执行记录在进入可过期状态时写入 {@code expires_at}：DURABLE 终态使用 {@code durableExecutions}，
 * EPHEMERAL 终态使用更短的 {@code ephemeralExecutions}；EPHEMERAL 非终态记录每次更新都会续期
 * {@code staleExecutions}，用于回收进程崩溃后永远不会结束的试运行记录。DURABLE 非终态与
 * WAITING_MANUAL 永不过期，避免删除仍需恢复或人工处置的现场。事件、日志和内容流仍按各自创建时间
 * 独立过期，并随所属执行一起级联删除。
 *
 * @param durableExecutions DURABLE 终态执行及其节点、事件、日志、非固定 Payload 的保留期
 * @param ephemeralExecutions EPHEMERAL 终态执行的保留期
 * @param staleExecutions EPHEMERAL 非终态执行自最后一次更新起的保留期，必须远大于最大执行时长
 * @param events 生命周期事件按创建时间的保留期
 * @param logs 节点日志按创建时间的保留期
 * @param content 内容流按创建时间的保留期
 * @param batchSize 单个清理事务处理的最大执行数或行数，限制长事务与锁范围
 */
public record MySqlRetentionPolicy(
        Duration durableExecutions,
        Duration ephemeralExecutions,
        Duration staleExecutions,
        Duration events,
        Duration logs,
        Duration content,
        int batchSize) {

    public MySqlRetentionPolicy {
        for (Duration value :
                new Duration[] {
                    durableExecutions, ephemeralExecutions, staleExecutions, events, logs, content
                }) {
            Objects.requireNonNull(value, "retention must not be null");
            if (value.isZero() || value.isNegative())
                throw new IllegalArgumentException("retention must be positive");
        }
        if (batchSize < 1 || batchSize > 5000)
            throw new IllegalArgumentException("cleanup batch size out of bounds");
    }

    /** 默认：DURABLE 终态 7 天、EPHEMERAL 终态 1 天、孤儿 EPHEMERAL 1 天，事件/日志/内容 1 小时。 */
    public static MySqlRetentionPolicy defaults() {
        return new MySqlRetentionPolicy(
                Duration.ofDays(7),
                Duration.ofDays(1),
                Duration.ofDays(1),
                Duration.ofHours(1),
                Duration.ofHours(1),
                Duration.ofHours(1),
                500);
    }

    /** 返回只调整批量大小的新策略。 */
    public MySqlRetentionPolicy withBatchSize(int size) {
        return new MySqlRetentionPolicy(
                durableExecutions, ephemeralExecutions, staleExecutions, events, logs, content, size);
    }

    /** 返回同一策略但统一替换事件、日志、内容的行级保留期，兼容旧构造参数。 */
    MySqlRetentionPolicy withRowRetention(Duration events, Duration logs, Duration content) {
        return new MySqlRetentionPolicy(
                durableExecutions, ephemeralExecutions, staleExecutions, events, logs, content,
                batchSize);
    }

    /** 按表名返回行级保留期；表名为适配器内部常量。 */
    Duration rows(String table) {
        return switch (table) {
            case "engine_event" -> events;
            case "engine_log" -> logs;
            case "engine_content" -> content;
            default -> throw new IllegalArgumentException("table has no row retention");
        };
    }
}
