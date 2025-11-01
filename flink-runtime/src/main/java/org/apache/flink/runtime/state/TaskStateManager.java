/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;

/**
 * This interface provides methods to report and retrieve state for a task.
 *
 * <p>When a checkpoint or savepoint is triggered on a task, it will create snapshots for all stream
 * operator instances it owns. All operator snapshots from the task are then reported via this
 * interface. A typical implementation will dispatch and forward the reported state information to
 * interested parties such as the checkpoint coordinator or a local state store.
 *
 * <p>This interface also offers the complementary method that provides access to previously saved
 * state of operator instances in the task for restore purposes.
 */
// Flink **任务执行（Task Execution）**与 **状态管理（State Management）**之间的核心桥梁。它在 Flink 的容错机制中扮演着至关重要的角色。
// 主要作用是为 Flink 任务子任务（Task Subtask）提供一个统一的状态接入点（Unified State Access Point）
// 当 TaskManager 上的一个子任务（如 Map 算子的一个并行实例）运行时，它内部的各个算子（Operator）需要执行状态的保存（Snapshot）和恢复（Restore）。TaskStateManager 负责：
// 状态报告（Checkpointing）： 接收子任务中所有算子的状态快照数据（包括快照元数据和指标），并将这些信息报告给 JobManager（Checkpoint Coordinator）进行持久化和确认。
// 状态恢复（Restoring）： 在任务启动时或从故障中恢复时，为子任务中每个算子提供其需要恢复的优先级化状态（Prioritized State）。
// 通道状态和 Changelog 集成： 管理与流式数据通道状态（Channel State）和状态变更日志（State Changelog）相关的组件和配置。
// TaskStateManager 是 Task 与 Checkpoint 协调器、状态后端（State Backend）以及本地恢复（Local Recovery）机制之间的通信和数据交换的门面
public interface TaskStateManager extends CheckpointListener, AutoCloseable {
    // 用于报告子任务在初始化阶段（例如恢复状态）的性能指标和耗时。
    void reportInitializationMetrics(SubTaskInitializationMetrics subTaskInitializationMetrics);

    /**
     * Report the state snapshots for the operator instances running in the owning task.
     *
     * @param checkpointMetaData meta data from the checkpoint request.
     * @param checkpointMetrics task level metrics for the checkpoint.
     * @param acknowledgedState the reported states to acknowledge to the job manager.
     * @param localState the reported states for local recovery.
     */
    // 任务保存状态的核心方法。
    // 它接收所有算子（Operator）的状态快照：
    // acknowledgedState 是要发送给 JobManager 进行持久化的全局状态；
    // localState 是用于本地恢复（Local Recovery）的本地状态。同时报告检查点的元数据和指标。
    void reportTaskStateSnapshots(
            @Nonnull CheckpointMetaData checkpointMetaData,
            @Nonnull CheckpointMetrics checkpointMetrics,
            @Nullable TaskStateSnapshot acknowledgedState,
            @Nullable TaskStateSnapshot localState);

    // 获取输入数据重扩容描述符。
    // 返回一个描述符，其中包含在任务恢复时，如何处理上游输入通道中**飞行中（inflight）**数据的重扩容（Rescaling）信息的逻辑。
    InflightDataRescalingDescriptor getInputRescalingDescriptor();
    // 获取输出数据重扩容描述符。
    // 返回一个描述符，其中包含在任务恢复时，如何处理本任务输出通道中飞行中数据的重扩容信息的逻辑。
    InflightDataRescalingDescriptor getOutputRescalingDescriptor();

    /**
     * Report the stats for state snapshots for an aborted checkpoint.
     *
     * @param checkpointMetaData meta data from the checkpoint request.
     * @param checkpointMetrics task level metrics for the checkpoint.
     */
    // 报告未完成（中止）的快照。
    // 用于报告那些已发起但最终中止或失败的检查点的统计数据。
    // 这通常用于指标记录和调试。
    void reportIncompleteTaskStateSnapshots(
            CheckpointMetaData checkpointMetaData, CheckpointMetrics checkpointMetrics);

    /** Whether all the operators of the task are finished on restore. */
    // 查询任务是否已完成部署。
    // 检查在恢复时，此任务是否被标记为“已完成”，这在 Flink 批处理模式或作业完成逻辑中可能用到。
    boolean isTaskDeployedAsFinished();

    /** Acquires the checkpoint id to restore from. */
    // 获取恢复检查点 ID。
    // 返回任务应该从哪个检查点 ID 开始恢复状态。如果任务是首次启动或从头开始，则可能返回空。
    Optional<Long> getRestoreCheckpointId();

    /**
     * Returns means to restore previously reported state of an operator running in the owning task.
     *
     * @param operatorID the id of the operator for which we request state.
     * @return Previous state for the operator. The previous state can be empty if the operator had
     *     no previous state.
     */
    // 获取优先级化的算子子任务状态。
    // 任务中每个算子在启动时调用此方法来获取其恢复所需的状态。
    // 返回的 PrioritizedOperatorSubtaskState 包含一个有序的状态句柄列表，状态管理器将尝试从最高优先级的（例如本地状态）开始恢复。
    @Nonnull
    PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID);

    /**
     * Get the restored state from jobManager which belongs to an operator running in the owning
     * task.
     *
     * @param operatorID the id of the operator for which we request state.
     * @return the subtask restored state from jobManager.
     */
    // 获取 JobManager 恢复状态。
    // 返回从 JobManager 获得的，特定算子子任务需要恢复的全局状态（通常用于调试或特定状态后端逻辑）。
    Optional<OperatorSubtaskState> getSubtaskJobManagerRestoredState(OperatorID operatorID);

    /**
     * Returns the configuration for local recovery, i.e. the base directories for all file-based
     * local state of the owning subtask and the general mode for local recovery.
     */
    // 创建本地恢复配置。
    // 返回一个配置对象，包含进行本地恢复所需的目录路径和模式等信息。
    @Nonnull
    LocalRecoveryConfig createLocalRecoveryConfig();
    // 获取顺序通道状态读取器。
    // 用于顺序读取 Task 恢复所需的所有通道状态（Channel State），即流式数据在 Checkpoint 时截断并保存下来的状态
    SequentialChannelStateReader getSequentialChannelStateReader();

    /** Returns the configured state changelog storage for this task. */
    // 获取状态变更日志存储。
    // 返回为此任务配置的状态变更日志存储组件。这是 Flink 3.0 快速检查点机制中的关键组件。
    @Nullable
    StateChangelogStorage<?> getStateChangelogStorage();

    /**
     * Returns the state changelog storage view of given {@link ChangelogStateHandle} for this task.
     */
    // 获取状态变更日志存储视图。
    // 根据给定的配置和状态变更日志句柄，返回一个视图，用于读取和应用状态变更日志数据。
    @Nullable
    StateChangelogStorageView<?> getStateChangelogStorageView(
            Configuration configuration, ChangelogStateHandle changelogStateHandle);
    // 获取文件合并快照管理器。
    // 返回用于管理文件合并快照（例如，某些状态后端可以合并小文件以优化存储）的组件。
    @Nullable
    FileMergingSnapshotManager getFileMergingSnapshotManager();
}
