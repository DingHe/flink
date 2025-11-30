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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * Common interface for the runtime {@link DefaultExecutionGraph} and {@link
 * ArchivedExecutionGraph}.
 */
// AccessExecutionGraph 接口是 Flink 中用于提供作业状态和执行图信息访问的通用接口，它扮演着门面 (Facade) 的角色
// 接口旨在提供对两种核心执行图类型（实现）的统一访问视图：
// DefaultExecutionGraph： 正在内存中运行的实时执行图。
// ArchivedExecutionGraph： 作业完成后或停止后，被归档（Archived）的历史执行图快照。
// 通过这个接口，像 Flink Web UI、REST API 或其他监控组件就能以一致的方式获取作业的核心元数据、拓扑结构、运行时状态、检查点信息和累计器结果，
// 而无需区分它是当前正在运行的作业还是已完成的归档作业。

public interface AccessExecutionGraph extends JobStatusProvider {
    /**
     * Returns the job plan as a JSON string.
     *
     * @return job plan as a JSON string
     */
    // 获取 JSON 计划。
    // 返回作业计划的 JSON 字符串表示，用于可视化和调试。
    String getJsonPlan();

    /**
     * Returns the {@link JobID} for this execution graph.
     *
     * @return job ID for this execution graph
     */
    // 获取作业 ID。
    // 返回作业的唯一标识符。
    JobID getJobID();

    /**
     * Returns the job name for the execution graph.
     *
     * @return job name for this execution graph
     */
    // 获取作业名称。
    // 返回用户定义的作业名称。
    String getJobName();

    /**
     * Returns the current {@link JobStatus} for this execution graph.
     *
     * @return job status for this execution graph
     */
    // 获取当前作业状态。
    // 返回作业当前的执行状态（继承自 JobStatusProvider）。
    JobStatus getState();

    /**
     * Returns the {@link JobType} for this execution graph.
     *
     * @return job type for this execution graph. It may be null when an exception occurs.
     */
    // 获取作业类型。
    // 返回作业是 STREAMING（流式）还是 BATCH（批式）。
    // 如果发生异常，可能返回 null。
    @Nullable
    JobType getJobType();

    /**
     * Returns the exception that caused the job to fail. This is the first root exception that was
     * not recoverable and triggered job failure.
     *
     * @return failure causing exception, or null
     */
    // 获取故障信息。
    // 如果作业失败，返回导致作业失败的根异常和时间戳。如果作业未失败，则返回 null。
    @Nullable
    ErrorInfo getFailureInfo();

    /**
     * Returns the job vertex for the given {@link JobVertexID}.
     *
     * @param id id of job vertex to be returned
     * @return job vertex for the given id, or {@code null}
     */
    // 通过 ID 获取作业顶点。
    // 根据 JobVertex ID 查找并返回对应的 ExecutionJobVertex 访问接口。
    @Nullable
    AccessExecutionJobVertex getJobVertex(JobVertexID id);

    /**
     * Returns a map containing all job vertices for this execution graph.
     * 获取所有的执行顶点
     * @return map containing all job vertices for this execution graph
     */
    // 获取所有作业顶点。
    // 返回一个 Map，包含所有 JobVertexID 及其对应的 ExecutionJobVertex 访问接口。
    Map<JobVertexID, ? extends AccessExecutionJobVertex> getAllVertices();

    /**
     * Returns an iterable containing all job vertices for this execution graph in the order they
     * were created.
     *
     * @return iterable containing all job vertices for this execution graph in the order they were
     *     created
     */
    // 按拓扑顺序获取作业顶点。
    // 返回一个迭代器，按作业顶点创建的顺序（通常也是拓扑顺序）遍历所有 ExecutionJobVertex 访问接口。
    Iterable<? extends AccessExecutionJobVertex> getVerticesTopologically();

    /**
     * Returns an iterable containing all execution vertices for this execution graph.
     *
     * @return iterable containing all execution vertices for this execution graph
     */
    // 获取所有执行顶点。
    // 返回一个迭代器，遍历执行图中的所有并行任务实例（ExecutionVertex）的访问接口。
    Iterable<? extends AccessExecutionVertex> getAllExecutionVertices();

    /**
     * Returns the timestamp for the given {@link JobStatus}.
     *
     * @param status status for which the timestamp should be returned
     * @return timestamp for the given job status
     */
    // 获取状态时间戳。
    // 返回作业进入给定状态的时间点（继承自 JobStatusProvider）。
    long getStatusTimestamp(JobStatus status);

    /**
     * Returns the {@link CheckpointCoordinatorConfiguration} or <code>null</code> if checkpointing
     * is disabled.
     * @return JobCheckpointingConfiguration for this execution graph
     */
    // 获取检查点协调器配置。
    // 返回作业的 Checkpoint 配置，如果未启用 Checkpoint 则返回 null。
    @Nullable
    CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration();

    /**
     * Returns a snapshot of the checkpoint statistics or <code>null</code> if checkpointing is
     * disabled.
     *
     * @return Snapshot of the checkpoint statistics for this execution graph
     */
    // 获取检查点统计快照。
    // 返回关于最近 Checkpoint 状态的统计信息快照，用于监控。
    // 如果未启用 Checkpoint 则返回 null。
    @Nullable
    CheckpointStatsSnapshot getCheckpointStatsSnapshot();

    /**
     * Returns the {@link ArchivedExecutionConfig} for this execution graph.
     *
     * @return execution config summary for this execution graph, or null in case of errors
     */
    // 获取归档执行配置。
    // 返回作业启动时使用的执行配置（如并行度、重启策略等）的快照。
    @Nullable
    ArchivedExecutionConfig getArchivedExecutionConfig();

    /**
     * Returns whether the job for this execution graph is stoppable.
     *
     * @return true, if all sources tasks are stoppable, false otherwise
     */
    // 检查作业是否可停止。
    // 返回作业是否可以被安全地停止 (STOP 操作)，这通常取决于所有 Source 任务是否支持该操作。
    boolean isStoppable();

    /**
     * Returns the aggregated user-defined accumulators as strings.
     *
     * @return aggregated user-defined accumulators as strings.
     */
    // 获取累计器字符串结果。
    // 返回用户定义累计器的最终聚合结果，格式化为字符串数组。
    StringifiedAccumulatorResult[] getAccumulatorResultsStringified();

    /**
     * Returns a map containing the serialized values of user-defined accumulators.
     *
     * @return map containing serialized values of user-defined accumulators
     */
    // 获取序列化累计器。
    // 返回用户定义累计器的原始序列化值，用于更精确的数值分析。
    Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized();

    /**
     * Returns the state backend name for this ExecutionGraph.
     *
     * @return The state backend name, or an empty Optional in the case of batch jobs
     */
    // 获取状态后端名称。
    // 返回作业使用的状态后端名称（例如 "filesystem" 或 "rocksdb"）。批处理作业可能返回空 Optional。
    Optional<String> getStateBackendName();

    /**
     * Returns the checkpoint storage name for this ExecutionGraph.
     *
     * @return The checkpoint storage name, or an empty Optional in the case of batch jobs
     */
    // 获取检查点存储名称。
    // 返回检查点数据的存储位置名称。批处理作业可能返回空 Optional。
    Optional<String> getCheckpointStorageName();

    /**
     * Returns whether the state changelog is enabled for this ExecutionGraph.
     *
     * @return true, if state changelog enabled, false otherwise.
     */
    // 检查是否启用了状态变更日志 (Changelog)。
    // 返回一个三值布尔值 (TRUE, FALSE, UNDEFINED)，指示是否启用了 Changelog 状态后端。
    TernaryBoolean isChangelogStateBackendEnabled();

    /**
     * Returns the changelog storage name for this ExecutionGraph.
     *
     * @return The changelog storage name, or an empty Optional in the case of batch jobs
     */
    // 获取 Changelog 存储名称。
    // 返回 Changelog 数据的存储位置名称。
    // 批处理作业可能返回空 Optional。
    Optional<String> getChangelogStorageName();
}
