/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for scheduling Flink jobs.
 * <p>Instances are created via {@link SchedulerNGFactory}, and receive a {@link JobGraph} when
 * instantiated.
 * <p>Implementations can expect that methods will not be invoked concurrently. In fact, all
 * invocations will originate from a thread in the {@link ComponentMainThreadExecutor}.
 */
// SchedulerNG 接口定义了 Flink **JobMaster（作业管理器）**中调度组件的所有核心功能和交互点。
// 它充当了 JobMaster 的核心大脑，负责将逻辑作业图 (JobGraph) 转化为可执行的物理执行图 (ExecutionGraph)，并管理其整个生命周期。
// 作业生命周期管理：控制作业的启动、取消、停止，并提供作业终止状态的 Future。
// 任务状态处理：处理来自 TaskManager 的任务执行状态更新（如 RUNNING, FAILED, FINISHED），并据此驱动 ExecutionGraph 的状态变迁和故障恢复。
// 数据流和资源协调：处理任务所需的数据分片请求（InputSplit）和结果分区状态查询（Partition State），以确保数据流的正确连接。
// 检查点和状态管理：负责协调和管理分布式检查点（Checkpoint）和保存点（Savepoint）的触发、确认和拒绝，是 Flink 容错机制的中心。
// 操作符协调：处理与 Operator Coordinator（操作符协调器，例如 Source Coordinator）之间的事件和请求，支持复杂的 Source 和自定义协调逻辑。
// 明确要求所有方法调用都必须来自 ComponentMainThreadExecutor 线程池，从而确保所有调度操作是顺序执行的，避免并发问题，简化了内部状态管理。
public interface SchedulerNG extends GlobalFailureHandler, AutoCloseableAsync {
    // 启动调度过程。
    // 该方法触发 Flink 作业的执行流程开始，包括资源请求、任务部署等。
    void startScheduling();
    // 取消作业执行。
    // 向所有正在运行的任务发送取消指令，并清理作业资源
    void cancel();
    // 获取作业终止 Future。
    // 提供一个异步机制，让调用者可以跟踪作业最终是成功、失败还是被取消。
    CompletableFuture<JobStatus> getJobTerminationFuture();
    // 将 TaskExecutionState 包装为 TaskExecutionStateTransition 后调用重载方法。
    default boolean updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        return updateTaskExecutionState(new TaskExecutionStateTransition(taskExecutionState));
    }
    // 更新任务执行状态。
    // 接收来自 TaskManager 的任务状态报告（FINISHED, FAILED, RUNNING 等），是 JobMaster 驱动 ExecutionGraph 状态机的核心入口。
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState);
   // 请求下一个输入分片。
   // 在批处理或 Source 任务启动时，TaskManager 会调用此方法向 JobMaster 请求它要处理的下一个数据分片。
    SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException;
    // 请求结果分区状态。
    // 当下游任务（Consumer）准备消费数据时，会请求上游结果分区（Producer Partition）的状态，确认数据是否已准备好。
    ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException;
    // 请求作业执行图信息。
    // 返回作业的完整执行图（ExecutionGraph）快照及相关信息，用于 Web UI 或外部查询。
    ExecutionGraphInfo requestJob();

    /**
     * Returns the checkpoint statistics for a given job. Although the {@link
     * CheckpointStatsSnapshot} is included in the {@link ExecutionGraphInfo}, this method is
     * preferred to {@link SchedulerNG#requestJob()} because it is less expensive.
     * @return checkpoint statistics snapshot for job graph
     */
    // 请求检查点统计。
    // 获取当前作业检查点的详细统计信息快照。
    CheckpointStatsSnapshot requestCheckpointStats();
    // 请求作业当前状态。
    // 返回作业的当前状态（如 RUNNING、FAILED、FINISHED 等）。
    JobStatus requestJobStatus();

    // ------------------------------------------------------------------------------------
    // Methods below do not belong to Scheduler but are included due to historical reasons
    // ------------------------------------------------------------------------------------
    // 请求 Keyed State 的位置。
    // 用于 Queryable State 功能，查询特定 Keyed State 在集群中的存储位置。
    KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException;
    // 通知 KvState 注册。
    // 当 TaskManager 注册了一个可查询状态（KvState）时，通知 JobMaster 记录其位置。
    void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException;
    // 通知 KvState 注销。
    // 当 KvState 被移除时通知 JobMaster。
    void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException;

    // ------------------------------------------------------------------------
    // 更新累加器。
    // 接收来自任务的累加器数据更新（通常用于进度展示或调试）。
    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);

    // ------------------------------------------------------------------------
    // 触发保存点。
    // 用于将作业状态持久化到外部存储。可以选定目标路径和是否在完成后取消作业。
    CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob, SavepointFormatType formatType);

    // 触发检查点。
    // 在流式作业中触发一次定期的检查点操作。
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType);
    // 确认检查点。
    // Task Sinks 成功完成检查点写入后，通知 JobMaster 确认该检查点已经完成，并携带检查点指标和任务状态快照。
    void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState);
    // 汇报检查点指标。
    // 任务在处理检查点时，将耗时等指标汇报给 JobMaster。
    void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics);

    // 拒绝检查点。
    // 当某个任务无法完成检查点时（例如，I/O 错误），调用此方法通知 JobMaster，检查点将失败。
    void declineCheckpoint(DeclineCheckpoint decline);
    // 汇报初始化指标。
    // 任务（特别是从状态恢复的任务）在启动阶段将初始化耗时等指标汇报给 JobMaster。
    void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics);
    // 优雅停止作业并保存状态。
    // 先触发保存点，然后等待保存点完成后再安全地终止作业。
    CompletableFuture<String> stopWithSavepoint(
            String targetDirectory, boolean terminate, SavepointFormatType formatType);

    // ------------------------------------------------------------------------
    //  Operator Coordinator related methods
    //
    //  These are necessary as long as the Operator Coordinators are part of the
    //  scheduler. There are good reasons to pull them out of the Scheduler and
    //  make them directly a part of the JobMaster. However, we would need to
    //  rework the complete CheckpointCoordinator initialization before we can
    //  do that, because the CheckpointCoordinator is initialized (and restores
    //  savepoint) in the scheduler constructor, which requires the coordinators
    //  to be there as well.
    // ------------------------------------------------------------------------

    /**
     * Delivers the given OperatorEvent to the {@link OperatorCoordinator} with the given {@link
     * OperatorID}.
     *
     * <p>Failure semantics: If the task manager sends an event for a non-running task or a
     * non-existing operator coordinator, then respond with an exception to the call. If task and
     * coordinator exist, then we assume that the call from the TaskManager was valid, and any
     * bubbling exception needs to cause a job failure
     * @throws FlinkException Thrown, if the task is not running or no operator/coordinator exists
     *     for the given ID.
     */
    // 发送操作符事件。
    // 将来自 TaskManager（通常是 Source Task）的事件发送给指定的 Operator Coordinator 进行处理。
    void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException;

    /**
     * Delivers a coordination request to the {@link OperatorCoordinator} with the given {@link
     * OperatorID} and returns the coordinator's response.
     *
     * @return A future containing the response.
     * @throws FlinkException Thrown, if the task is not running, or no operator/coordinator exists
     *     for the given ID, or the coordinator cannot handle client events.
     */
    // 发送协调请求。
    // 处理来自外部客户端或 TaskManager 的请求，并等待 Operator Coordinator 返回协调响应。
    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException;

    /**
     * Notifies that the task has reached the end of data.
     *
     * @param executionAttemptID The execution attempt id.
     */
    // 通知数据结束。
    // 当某个任务实例（Execution Attempt）处理完所有数据（End-of-data）时通知调度器。
    void notifyEndOfData(ExecutionAttemptID executionAttemptID);

    /**
     * Read current {@link JobResourceRequirements job resource requirements}.
     *
     * @return Current resource requirements.
     */
    // 请求作业资源需求。
    // 返回作业当前所需的资源配置（例如并发度）。
    default JobResourceRequirements requestJobResourceRequirements() {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }

    /**
     * Update {@link JobResourceRequirements job resource requirements}.
     *
     * @param jobResourceRequirements new resource requirements
     */
    // 更新作业资源需求。
    // 允许外部组件动态地更改作业的资源需求（例如调整并发度）。
    default void updateJobResourceRequirements(JobResourceRequirements jobResourceRequirements) {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }
}
