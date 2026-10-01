package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.adapter.memory.InMemoryObservationStore;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.core.compile.FlowCompiler;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.core.execution.ScopePath;
import com.playrewards.flowengine.core.observation.ExecutionRecorder;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 观察边界不得在每个节点排队时回读节点历史：异步写入的存储会因此被迫逐节点同步刷盘。
 * 尝试序号由活动执行的内存状态维护，只有恢复接管时才读取一次历史。
 */
class ObservationReadAmplificationTest {

    @Test
    void queuedAttemptsUseInMemoryHistoryAndRecoverReadsItOnce() {
        var store = new InMemoryObservationStore();
        var reads = new AtomicInteger();
        ExecutionRecordStore counting =
                (ExecutionRecordStore)
                        java.lang.reflect.Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {ExecutionRecordStore.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("nodes")) reads.incrementAndGet();
                                    try {
                                        return method.invoke(store, args);
                                    } catch (java.lang.reflect.InvocationTargetException failure) {
                                        throw failure.getCause();
                                    }
                                });
        var recorder = recorder(counting, store);
        var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
        recorder.created("attempts", plan, request(), null);
        var start = plan.nodesById().get("start");
        recorder.queued("attempts", start, ScopePath.root());
        recorder.queued("attempts", start, ScopePath.root());
        recorder.queued("attempts", start, ScopePath.root().child("b"));
        assertEquals(0, reads.get());
        assertEquals(List.of(1, 2, 1), attempts(store));

        var takeover = recorder(counting, store);
        assertTrue(takeover.recover("attempts"));
        takeover.queued("attempts", start, ScopePath.root());
        assertEquals(1, reads.get());
        assertEquals(List.of(1, 2, 1, 3), attempts(store));
    }

    /** 直接读取底层存储（不计入回读次数），按排队顺序返回尝试序号。 */
    private static List<Integer> attempts(InMemoryObservationStore store) {
        return store.nodes("attempts").stream().map(NodeExecutionRecord::attempt).toList();
    }

    private static ExecutionRecorder recorder(
            ExecutionRecordStore records, InMemoryObservationStore payloads) {
        return new ExecutionRecorder(
                records,
                payloads,
                new BoundedExecutionEventBus(records, 2, 64),
                Clock.systemUTC(),
                false);
    }

    private static ExecutionRequest request() {
        return new ExecutionRequest(
                "test:flow",
                ExecutionMode.EPHEMERAL,
                ExecutionPurpose.TRIAL,
                Map.of(),
                Map.of(),
                null,
                "key",
                "tenant",
                Instant.now().plusSeconds(5),
                Map.of());
    }
}
