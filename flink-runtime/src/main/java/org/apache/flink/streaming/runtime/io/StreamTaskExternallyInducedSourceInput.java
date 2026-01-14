/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.streaming.api.operators.SourceOperator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
// 适配外部触发 Checkpoint 的 Source： 专门用于封装实现了 ExternallyInducedSourceReader 接口的 Source Reader
// 触发 Checkpoint 机制： 在 Source Reader 暂时没有数据可读时，它会询问 Source Reader 是否应该触发一次 Checkpoint，并将触发逻辑（checkpointTriggeringHook）注入到 Flink 的运行时。
// Flink 内部支持具有外部 Checkpoint 机制的 Source 的桥梁，负责将外部的 Checkpoint 信号转换为 Flink 内部的 Checkpoint 触发流程。
/** A subclass of {@link StreamTaskSourceInput} for {@link ExternallyInducedSourceReader}. */
public class StreamTaskExternallyInducedSourceInput<T> extends StreamTaskSourceInput<T> {
    // Checkpoint 触发钩子
    // 用于实际向 JobManager 触发 Checkpoint 的逻辑。
    // 它接收 Checkpoint ID 作为参数。当 Source Reader 判断需要触发 Checkpoint 时，会调用此 Hook
    private final Consumer<Long> checkpointTriggeringHook;
    // 外部触发 Source Reader 实例
    private final ExternallyInducedSourceReader<T, ?> sourceReader;
    // 阻塞 Future。
    // 用于实现外部控制的 I/O 阻塞机制。如果此 Future 不为 null 且未完成，emitNext 方法将返回 NOTHING_AVAILABLE，直到 Future 完成。
    private CompletableFuture<?> blockFuture;

    @SuppressWarnings("unchecked")
    public StreamTaskExternallyInducedSourceInput(
            SourceOperator<T, ?> operator,
            Consumer<Long> checkpointTriggeringHook,
            int inputGateIndex,
            int inputIndex) {
        super(operator, inputGateIndex, inputIndex);
        this.checkpointTriggeringHook = checkpointTriggeringHook;
        this.sourceReader = (ExternallyInducedSourceReader<T, ?>) operator.getSourceReader();
    }

    public void blockUntil(CompletableFuture<?> blockFuture) {
        this.blockFuture = blockFuture;
        // assume that the future is completed in mailbox thread
        blockFuture.whenComplete((v, e) -> unblock());
    }

    private void unblock() {
        this.blockFuture = null;
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        if (blockFuture != null) {
            return DataInputStatus.NOTHING_AVAILABLE;
        }

        DataInputStatus status = super.emitNext(output);
        if (status == DataInputStatus.NOTHING_AVAILABLE) {
            sourceReader.shouldTriggerCheckpoint().ifPresent(checkpointTriggeringHook);
        }
        return status;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (blockFuture != null) {
            return blockFuture;
        }
        return super.getAvailableFuture();
    }
}
