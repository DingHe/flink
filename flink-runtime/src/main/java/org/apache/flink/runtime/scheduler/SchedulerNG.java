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
 *  SchedulerNG 接口定义了 Flink 作业的调度和执行方式。它涵盖了作业状态管理、检查点处理、操作符事件处理等功能
 * <p>Instances are created via {@link SchedulerNGFactory}, and receive a {@link JobGraph} when
 * instantiated.
 * 所有的方法调用都由 ComponentMainThreadExecutor 线程池发起，这意味着所有方法调用是顺序执行的，而不是并发的
 * <p>Implementations can expect that methods will not be invoked concurrently. In fact, all
 * invocations will originate from a thread in the {@link ComponentMainThreadExecutor}.
 */
public interface SchedulerNG extends GlobalFailureHandler, AutoCloseableAsync {
    //启动 Flink 作业的调度过程，该方法会触发作业执行生命周期的开始，通常在作业准备好执行时调用
    void startScheduling();
    //取消 Flink 作业的执行
    void cancel();
    //允许用户异步跟踪作业的终止状态
    CompletableFuture<JobStatus> getJobTerminationFuture();
    //用于更新任务的状态，例如将任务状态从 RUNNING 更新为 FAILED 或 FINISHED
    default boolean updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        return updateTaskExecutionState(new TaskExecutionStateTransition(taskExecutionState));
    }

    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState);
   //在流式或批处理作业中，用于请求任务执行的下一批数据分片
    SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException;
    //请求一个结果分区的当前状态，用于检查任务是否已经成功写入分区数据，或者分区是否仍然处于处理中
    ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException;
    //请求作业的执行图信息
    ExecutionGraphInfo requestJob();

    /**
     * Returns the checkpoint statistics for a given job. Although the {@link
     * CheckpointStatsSnapshot} is included in the {@link ExecutionGraphInfo}, this method is
     * preferred to {@link SchedulerNG#requestJob()} because it is less expensive.
     * 用于获取作业的检查点统计信息，便于监控和优化检查点的执行
     * @return checkpoint statistics snapshot for job graph
     */
    CheckpointStatsSnapshot requestCheckpointStats();
    //请求作业的当前状态（如 RUNNING、FAILED）
    JobStatus requestJobStatus();

    // ------------------------------------------------------------------------------------
    // Methods below do not belong to Scheduler but are included due to historical reasons
    // ------------------------------------------------------------------------------------

    KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException;

    void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException;

    void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException;

    // ------------------------------------------------------------------------

    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);

    // ------------------------------------------------------------------------
    //触发保存点操作，即将作业的状态持久化
    CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob, SavepointFormatType formatType);

    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType);
    //任务管理器在完成检查点后会调用此方法，通知调度器该检查点已成功完成
    void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState);

    void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics);
    //当某个任务无法完成检查点时，会调用该方法拒绝检查点
    void declineCheckpoint(DeclineCheckpoint decline);

    void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics);

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
     * 将事件发送到指定的操作符协调器
     * @throws FlinkException Thrown, if the task is not running or no operator/coordinator exists
     *     for the given ID.
     */
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
    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException;

    /**
     * Notifies that the task has reached the end of data.
     *
     * @param executionAttemptID The execution attempt id.
     */
    void notifyEndOfData(ExecutionAttemptID executionAttemptID);

    /**
     * Read current {@link JobResourceRequirements job resource requirements}.
     *
     * @return Current resource requirements.
     */
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
    default void updateJobResourceRequirements(JobResourceRequirements jobResourceRequirements) {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }
}
