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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.failover.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The execution graph is the central data structure that coordinates the distributed execution of a
 * data flow. It keeps representations of each parallel task, each intermediate stream, and the
 * communication between them.
 * <p>The execution graph consists of the following constructs:
 *
 * <ul>
 *   <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one
 *       operation like "map" or "join") during execution. It holds the aggregated state of all
 *       parallel subtasks. The ExecutionJobVertex is identified inside the graph by the {@link
 *       JobVertexID}, which it takes from the JobGraph's corresponding JobVertex.
 *   <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex,
 *       there are as many ExecutionVertices as the parallelism. The ExecutionVertex is identified
 *       by the ExecutionJobVertex and the index of the parallel subtask
 *   <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple
 *       Executions for the ExecutionVertex, in case of a failure, or in the case where some data
 *       needs to be recomputed because it is no longer available when requested by later
 *       operations. An Execution is always identified by an {@link ExecutionAttemptID}. All
 *       messages between the JobManager and the TaskManager about deployment of tasks and updates
 *       in the task status always use the ExecutionAttemptID to address the message receiver.
 * </ul>
 */
// ExecutionGraph 接口是 Flink 中执行图的核心抽象，它是一个 Job 在运行时在 JobMaster 端的数据结构，负责协调作业的分布式执行。
// 它将用户提交的逻辑作业图 (JobGraph) 转换成一个可执行的物理图，并管理作业的生命周期、状态转换、故障恢复、检查点以及资源协调。
// ExecutionJobVertex (作业顶点)： 对应 JobGraph 中的一个算子 (JobVertex)。它代表了一个逻辑操作，并包含该操作的所有并行子任务的聚合状态。
// ExecutionVertex (执行顶点)： 对应 ExecutionJobVertex 的一个并行子任务实例（Subtask）。它是调度的基本单元。
// Execution (执行尝试)： 对应 ExecutionVertex 的一次具体运行尝试。每次任务失败或重启时，都会创建一个新的 Execution，它通过 ExecutionAttemptID 唯一标识。这是 JobManager 和 TaskManager 之间通信的最小单元。
// ExecutionGraph 是 Flink 作业的实时大脑。
public interface ExecutionGraph extends AccessExecutionGraph {
    // 启动执行图。
    // 在调度器准备好后调用，将执行图与 JobMaster 的主线程执行器关联，并开始作业的分布式执行过程。
    void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor);
    //获取调度拓扑结构，
    // 这表示任务之间的依赖关系以及如何安排它们的执行
    SchedulingTopology getSchedulingTopology();
   // 启用检查点。
   // 初始化并配置作业的 Checkpoint 组件，包括 CheckpointCoordinator、存储、ID 计数器等。
    void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner,
            String changelogStorage);


    // 获取检查点协调器。
    // 返回已启用 Checkpoint 后的 CheckpointCoordinator 实例。
    @Nullable
    CheckpointCoordinator getCheckpointCoordinator();

    // 获取 Key/Value 状态位置注册表。
    // 用于注册和查询 Key/Value 状态的位置，以便外部查询（如 Queryable State）。
    KvStateLocationRegistry getKvStateLocationRegistry();
    // 设置 JSON 计划。
    // 用于设置作业的 JSON 计划字符串。
    void setJsonPlan(String jsonPlan);
    // 获取作业配置。
    // 返回作业启动时的 Flink 配置。
    Configuration getJobConfiguration();
    // 获取故障原因。
    // 返回导致作业失败的异常对象。
    Throwable getFailureCause();

    @Override
    Iterable<ExecutionJobVertex> getVerticesTopologically();

    @Override
    Iterable<ExecutionVertex> getAllExecutionVertices();

    @Override
    ExecutionJobVertex getJobVertex(JobVertexID id);

    @Override
    Map<JobVertexID, ExecutionJobVertex> getAllVertices();

    /**
     * Gets the number of restarts, including full restarts and fine grained restarts. If a recovery
     * is currently pending, this recovery is included in the count.
     *
     * @return The number of restarts so far
     */
    long getNumberOfRestarts();
    // 获取所有中间结果。
    // 返回作业中所有中间数据集及其对应的 IntermediateResult 实例。
    Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults();

    /**
     * Gets the intermediate result partition by the given partition ID, or throw an exception if
     * the partition is not found.
     *
     * @param id of the intermediate result partition
     * @return intermediate result partition
     */
    // 获取结果分区。
    // 通过 ID 获取对应的 IntermediateResultPartition 实例，如果找不到则抛出异常。
    IntermediateResultPartition getResultPartitionOrThrow(final IntermediateResultPartitionID id);

    /**
     * Merges all accumulator results from the tasks previously executed in the Executions.
     *
     * @return The accumulator map
     */
    // 聚合用户累计器。
    // 聚合所有已完成任务的用户定义累计器结果。
    Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators();

    /**
     * Updates the accumulators during the runtime of a job. Final accumulator results are
     * transferred through the UpdateTaskExecutionState message.
     *
     * @param accumulatorSnapshot The serialized flink and user-defined accumulators
     */
    // 更新累计器。
    // 接收任务报告的累计器快照，用于更新作业的运行时指标。
    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);
    // 设置内部故障监听器。
    // 设置一个监听器，用于处理由 ExecutionGraph 内部逻辑（而非外部 TaskManager）触发的任务失败事件。
    void setInternalTaskFailuresListener(InternalFailuresListener internalTaskFailuresListener);
    // 附加 JobGraph。
    // 在 ExecutionGraph 构造后，将逻辑 JobGraph 的信息附加到图上，
    // 并根据 JobVertex 列表创建 ExecutionJobVertex 和 IntermediateResult 结构。
    void attachJobGraph(
            List<JobVertex> topologicallySorted, JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException;
    // 状态转换到 RUNNING。
    // 将作业状态从 CREATED 或 RESTARTING 等过渡状态转换为 RUNNING 状态。
    void transitionToRunning();
    // 取消作业。
    // 尝试将作业状态转换为 CANCELLING，然后将所有正在运行的任务标记为取消。
    void cancel();

    /**
     * Suspends the current ExecutionGraph.
     *
     * <p>The JobStatus will be directly set to {@link JobStatus#SUSPENDED} iff the current state is
     * not a terminal state. All ExecutionJobVertices will be canceled and the onTerminalState() is
     * executed.
     *
     * <p>The {@link JobStatus#SUSPENDED} state is a local terminal state which stops the execution
     * of the job but does not remove the job from the HA job store so that it can be recovered by
     * another JobManager.
     *
     * @param suspensionCause Cause of the suspension
     */
    // 挂起作业。
    // 将作业状态设置为 SUSPENDED。
    // 这是一种本地终止状态，它会停止执行，但不从 HA 存储中删除作业，允许 JobManager 故障转移后恢复。
    void suspend(Throwable suspensionCause);
    // 作业失败。
    // 触发作业状态向 FAILING 或 FAILED 的转换，记录故障原因 (cause) 和时间 (timestamp)。
    void failJob(Throwable cause, long timestamp);

    /**
     * Returns the termination future of this {@link ExecutionGraph}. The termination future is
     * completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this terminal
     * state and all {@link Execution} have been terminated.
     *
     * @return Termination future of this {@link ExecutionGraph}.
     */
    // 获取终止 Future。
    // 返回一个 Future，当 ExecutionGraph 达到最终状态（FINISHED, CANCELED, FAILED, SUSPENDED）时，该 Future 会完成并返回该最终状态。
    CompletableFuture<JobStatus> getTerminationFuture();
    // 等待作业到达终止状态。
    // 这是一个供测试使用的阻塞方法，等待 ExecutionGraph 达到任何最终状态。
    @VisibleForTesting
    JobStatus waitUntilTerminal() throws InterruptedException;
    //用于状态转换，
    // 确保作业的状态转换符合逻辑，如从 RUNNING 到 FINISHED
    boolean transitionState(JobStatus current, JobStatus newState);
    // 增加重启计数。
    // 当作业经历一次重启（无论是完全重启还是细粒度重启）时，调用此方法增加重启计数。
    void incrementRestarts();
    // 初始化故障原因。
    // 在作业发生故障的初期记录故障的根源 (t) 和时间戳。
    void initFailureCause(Throwable t, long timestamp);

    /**
     * Updates the state of one of the ExecutionVertex's Execution attempts. If the new status if
     * "FINISHED", this also updates the accumulators.
     *
     * @param state The state update.
     * @return True, if the task update was properly applied, false, if the execution attempt was
     *     not found.
     */
    // 更新任务执行状态。
    // 接收 TaskManager 发送的 TaskExecutionStateTransition 消息，更新相应 Execution 尝试的状态（如 RUNNING 到 FINISHED）。
    boolean updateState(TaskExecutionStateTransition state);
    // 获取已注册的执行尝试。
    // 返回所有当前或历史已注册的 Execution 实例。
    Map<ExecutionAttemptID, Execution> getRegisteredExecutions();

    // 注册作业状态监听器。
    // 允许外部组件注册一个监听器，以便在作业状态发生转换时接收通知。
    void registerJobStatusListener(JobStatusListener listener);
    // 获取结果分区可用性检查器。
    // 返回用于检查给定结果分区是否可用的组件，这对调度器判断任务是否可以启动至关重要。
    ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker();
    // 获取已完成顶点数量。
    // 返回所有 ExecutionJobVertex 中已完成的并行任务（ExecutionVertex）的数量。
    int getNumFinishedVertices();

    // 获取 JobMaster 主线程执行器。
    // 再次提供对主线程执行器的访问，用于在 Flink 的主调度循环中执行操作。
    @Nonnull
    ComponentMainThreadExecutor getJobMasterMainThreadExecutor();
    // 初始化 JobVertex。
    // 负责根据并行度创建 ExecutionVertex，并根据输入信息连接到上游的 IntermediateResult。
    default void initializeJobVertex(ExecutionJobVertex ejv, long createTimestamp)
            throws JobException {
        initializeJobVertex(
                ejv,
                createTimestamp,
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        ejv, getAllIntermediateResults()::get));
    }

    /**
     * Initialize the given execution job vertex, mainly includes creating execution vertices
     * according to the parallelism, and connecting to the predecessors.
     *
     * @param ejv The execution job vertex that needs to be initialized.
     * @param createTimestamp The timestamp for creating execution vertices, used to initialize the
     *     first Execution with.
     * @param jobVertexInputInfos The input infos of this job vertex.
     */
    void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos)
            throws JobException;

    /**
     * Notify that some job vertices have been newly initialized, execution graph will try to update
     * scheduling topology.
     *
     * @param vertices The execution job vertices that are newly initialized.
     */
    void notifyNewlyInitializedJobVertices(List<ExecutionJobVertex> vertices);
    // 通过尝试 ID 查找顶点。
    // 通过 ExecutionAttemptID 查找该尝试所属的 ExecutionVertex 的名称。
    Optional<String> findVertexWithAttempt(final ExecutionAttemptID attemptId);
    // 查找执行实例。
    // 通过 ExecutionAttemptID 查找对应的 Execution 实例。
    Optional<AccessExecution> findExecution(final ExecutionAttemptID attemptId);
}
