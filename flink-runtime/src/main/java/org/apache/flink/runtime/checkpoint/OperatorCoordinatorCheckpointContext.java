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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * This context is the interface through which the {@link CheckpointCoordinator} interacts with an
 * {@link OperatorCoordinator} during checkpointing and checkpoint restoring.
 */
// 定义了 Flink 检查点协调器 (CheckpointCoordinator) 在执行检查点（Checkpoint）和恢复操作时，与算子协调器 (OperatorCoordinator) 之间进行通信和交互的标准契约
// 充当了检查点机制和自定义算子协调器之间的桥梁。算子协调器通过实现这个接口，使得 Flink 的核心检查点系统能够统一管理和协调其状态的保存与恢复。
// OperatorInfo 算子信息。 允许检查点协调器获取关于该算子协调器所管理的算子的元数据，例如 JobVertexID、并行度等。

public interface OperatorCoordinatorCheckpointContext extends OperatorInfo, CheckpointListener {
    // 触发协调器检查点
    // checkpointId: 当前检查点的唯一 ID
    // result: 一个 CompletableFuture，算子协调器需要将它保存的状态数据（序列化后的 byte[]）填充到这个 Future 中。
    // 当 Future 完成时，表示协调器的状态已成功保存。
    void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception;

    // 中止当前正在进行的检查点触发
    // 当检查点协调器决定中止当前的检查点尝试时调用此方法。
    void abortCurrentTriggering();

    /**
     * We override the method here to remove the checked exception. Please check the Java docs of
     * {@link CheckpointListener#notifyCheckpointComplete(long)} for more detail semantic of the
     * method.
     */
    // 通知检查点已完成
    @Override
    void notifyCheckpointComplete(long checkpointId);

    /**
     * We override the method here to remove the checked exception. Please check the Java docs of
     * {@link CheckpointListener#notifyCheckpointAborted(long)} for more detail semantic of the
     * method.
     */
    // 通知检查点已中止。 这是从 CheckpointListener 接口重写的默认方法
    @Override
    default void notifyCheckpointAborted(long checkpointId) {}

    /**
     * Resets the coordinator to the checkpoint with the given state.
     *
     * <p>This method is called with a null state argument in the following situations:
     *
     * <ul>
     *   <li>There is a recovery and there was no completed checkpoint yet.
     *   <li>There is a recovery from a completed checkpoint/savepoint but it contained no state for
     *       the coordinator.
     * </ul>
     *
     * <p>In both cases, the coordinator should reset to an empty (new) state.
     */
    // 重置到检查点状态（全局恢复）。
    // 在发生全局故障恢复（如 JobManager 恢复或 Savepoint 恢复）时调用。协调器必须使用提供的状态数据恢复其内部状态
    void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception;

    /**
     * Called if a task is recovered as part of a <i>partial failover</i>, meaning a failover
     * handled by the scheduler's failover strategy (by default recovering a pipelined region). The
     * method is invoked for each subtask involved in that partial failover.
     *
     * <p>In contrast to this method, the {@link #resetToCheckpoint(long, byte[])} method is called
     * in the case of a global failover, which is the case when the coordinator (JobManager) is
     * recovered.
     */
    // 子任务重置（局部恢复）。
    // 在发生局部故障恢复（Partial Failover，通常由调度器的故障转移策略处理，只恢复受影响的流水线区域）时调用
    void subtaskReset(int subtask, long checkpointId);
}
