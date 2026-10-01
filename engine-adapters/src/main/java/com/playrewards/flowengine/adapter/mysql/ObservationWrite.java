package com.playrewards.flowengine.adapter.mysql;

import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

/**
 * 写后缓冲中的一条观察写入，按提交顺序归属于一个执行通道。
 *
 * <p>记录本身是不可变契约值；Payload 在调用线程完成序列化与大小校验，缓冲中只保存定长文本，
 * 避免调用方之后修改原始对象影响落库内容。
 */
sealed interface ObservationWrite {
    /** @return 所属执行 ID，用于选择通道与读前刷新 */
    String executionId();

    record SaveExecution(ExecutionRecord record) implements ObservationWrite {
        public String executionId() {
            return record.executionId();
        }
    }

    record AppendEvent(ExecutionEvent event) implements ObservationWrite {
        public String executionId() {
            return event.executionId();
        }
    }

    record SaveNode(NodeExecutionRecord record) implements ObservationWrite {
        public String executionId() {
            return record.executionId();
        }
    }

    record AppendLog(ExecutionLogRecord record) implements ObservationWrite {
        public String executionId() {
            return record.executionId();
        }
    }

    record AppendContent(String nodeExecutionId, ContentChunk chunk) implements ObservationWrite {
        public String executionId() {
            return chunk.executionId();
        }
    }

    /** 非固定观察 Payload；引用在调用线程生成并立即返回给记录器。 */
    record StorePayload(String reference, String executionId, String body, int size)
            implements ObservationWrite {}
}
