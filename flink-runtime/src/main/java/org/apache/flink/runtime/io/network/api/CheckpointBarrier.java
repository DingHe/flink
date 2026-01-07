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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Checkpoint barriers are used to align checkpoints throughout the streaming topology. The barriers
 * are emitted by the sources when instructed to do so by the JobManager. When operators receive a
 * CheckpointBarrier on one of its inputs, it knows that this is the point between the
 * pre-checkpoint and post-checkpoint data.
 *
 * <p>Once an operator has received a checkpoint barrier from all its input channels, it knows that
 * a certain checkpoint is complete. It can trigger the operator specific checkpoint behavior and
 * broadcast the barrier to downstream operators.
 *
 * <p>Depending on the semantic guarantees, may hold off post-checkpoint data until the checkpoint
 * is complete (exactly once).
 *
 * <p>The checkpoint barrier IDs are strictly monotonous increasing.
 */
// CheckpointBarrier（检查点屏障）在 Flink 流式数据处理中扮演着 “水位线”或“信号” 的角色，是实现 分布式快照（即检查点）的核心机制。
// 数据分隔： 屏障作为特殊事件，在数据流中流动，它将流数据清晰地分隔为**“屏障之前的数据”（Pre-checkpoint Data）和“屏障之后的数据”（Post-checkpoint Data）**。
// 触发对齐（Exactly-Once）：
// 当一个 Operator 接收到输入通道上的屏障时，它必须等待所有输入通道都收到相同 ID 的屏障。
// 在等待过程中，它会阻塞（或缓存）屏障之后的数据（Post-checkpoint Data），直到所有输入都对齐。
// 一旦对齐完成，Operator 知道它已经处理完所有属于上一个检查点的数据，可以安全地触发自身的状态快照（例如，将 Keyed State 写入存储），然后将屏障广播到下游所有输出通道，继续向下游传递。
// 非对齐机制（Unaligned Checkpoint）： Flink 也支持非对齐检查点，在这种模式下，屏障不再阻塞数据，而是允许数据流继续通过，同时将在途数据（In-flight Data）的状态也作为检查点的一部分进行保存。

public class CheckpointBarrier extends RuntimeEvent {
    // 检查点 ID。
    // 唯一且严格单调递增的标识符，用于标识是哪一次检查点。
    // 这是屏障对齐的核心依据。
    private final long id;
    // 检查点时间戳。
    // JobManager 发起检查点时的系统时间，用于元数据记录和审计。
    private final long timestamp;
    // 检查点选项。
    // 封装了检查点的类型和行为配置（例如：是否是 Savepoint、是否为非对齐检查点 Unaligned Checkpoint 等）。
    private final CheckpointOptions checkpointOptions;

    public CheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) {
        this.id = id;
        this.timestamp = timestamp;
        this.checkpointOptions = checkNotNull(checkpointOptions);
    }

    public long getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public CheckpointOptions getCheckpointOptions() {
        return checkpointOptions;
    }

    public CheckpointBarrier withOptions(CheckpointOptions checkpointOptions) {
        return this.checkpointOptions == checkpointOptions
                ? this
                : new CheckpointBarrier(id, timestamp, checkpointOptions);
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    //
    //  These methods are inherited form the generic serialization of AbstractEvent
    //  but would require the CheckpointBarrier to be mutable. Since all serialization
    //  for events goes through the EventSerializer class, which has special serialization
    //  for the CheckpointBarrier, we don't need these methods
    //

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32) ^ timestamp ^ (timestamp >>> 32));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != CheckpointBarrier.class) {
            return false;
        } else {
            CheckpointBarrier that = (CheckpointBarrier) other;
            return that.id == this.id
                    && that.timestamp == this.timestamp
                    && this.checkpointOptions.equals(that.checkpointOptions);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "CheckpointBarrier %d @ %d Options: %s", id, timestamp, checkpointOptions);
    }

    public boolean isCheckpoint() {
        return !checkpointOptions.getCheckpointType().isSavepoint();
    }

    public CheckpointBarrier asUnaligned() {
        return checkpointOptions.isUnalignedCheckpoint()
                ? this
                : new CheckpointBarrier(
                        getId(), getTimestamp(), getCheckpointOptions().toUnaligned());
    }
}
