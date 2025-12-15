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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been acknowledged by all
 * tasks that need to acknowledge it. Once all tasks have acknowledged it, it becomes a {@link
 * CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the state handles
 * always as serialized values, never as actual values.
 */
// PendingCheckpoint 类代表一个正在进行中（Pending）的检查点。
// CheckpointCoordinator 的内部状态容器，负责在检查点从触发到完成的过程中，追踪所有必需任务的进度、收集它们报告的子任务状态，并最终将所有信息整合成一个完整的、可持久化的检查点元数据。
// 一旦所有任务、算子协调器和 Master 状态都已确认，PendingCheckpoint 就会被终结（finalize），转换为一个不可变的 CompletedCheckpoint 对象，并存储起来。
// 生命周期总结：
// 创建： 当 CheckpointCoordinator 触发检查点时创建。
// 追踪： 记录哪些任务（ExecutionAttemptID）、算子协调器（OperatorID）和 Master 状态（Identifier）尚未确认。
// 收集： 接收任务的 AcknowledgeCheckpoint 消息，收集 TaskStateSnapshot 并聚合到 OperatorState 中。
// 完成： 当所有部分都确认后，调用 finalizeCheckpoint()，将自身转换为 CompletedCheckpoint。
// 清理/丢弃： 如果超时或被新的检查点取代 (Subsumed)，则被 abort() 或 dispose()，并释放状态资源。
@NotThreadSafe
public class PendingCheckpoint implements Checkpoint {

    /** Result of the {@link PendingCheckpoint#acknowledgedTasks} method. */
    public enum TaskAcknowledgeResult {
        SUCCESS, // successful acknowledge of the task
        DUPLICATE, // acknowledge message is a duplicate
        UNKNOWN, // unknown task acknowledged
        DISCARDED // pending checkpoint has been discarded
    }

    // ------------------------------------------------------------------------

    /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);
    // 同步锁。
    // 用于保护对内部状态（如 notYetAcknowledgedTasks 和 operatorStates）的并发访问。
    private final Object lock = new Object();
    // 作业 ID。
    // 当前检查点所属 Job 的唯一标识符。
    private final JobID jobId;
    // 检查点 ID。
    // 当前检查点的唯一、递增 ID。
    private final long checkpointId;
    // 检查点触发时间戳。
    // 检查点在 JobMaster 上被触发时的系统时间。
    private final long checkpointTimestamp;
    // 算子状态集合。
    // 存储已收集到的所有算子状态（由子任务报告）。
    // Key 是 OperatorID，Value 是聚合后的 OperatorState。
    private final Map<OperatorID, OperatorState> operatorStates;
    // 检查点计划。
    // 包含哪些任务需要参与此检查点的触发、等待和提交。
    private final CheckpointPlan checkpointPlan;
    // 未确认任务列表。
    // 存储尚未确认此检查点的任务执行尝试 ID (ExecutionAttemptID) 及其对应的顶点 (ExecutionVertex)。
    private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;
    // 未确认协调器集合。
    // 存储尚未提交状态的算子协调器 ID。
    private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;
    // Master 状态列表。
    // 存储由 Master Hook 或其他 Master 组件报告的状态数据。
    private final List<MasterState> masterStates;
    // 未确认 Master 状态集合。
    // 存储尚未提交状态的 Master Hook 标识符。
    private final Set<String> notYetAcknowledgedMasterStates;

    /** Set of acknowledged tasks. */
    // 已确认任务集合。
    // 存储已确认此检查点的任务执行尝试 ID，用于处理重复的 ACK 消息。
    private final Set<ExecutionAttemptID> acknowledgedTasks;

    /** The checkpoint properties. */
    // 检查点属性。
    // 包含检查点的配置信息，如是否是保存点 (isSavepoint)、是否是增量检查点等。
    private final CheckpointProperties props;

    /**
     * The promise to fulfill once the checkpoint has been completed. Note that it will be completed
     * only after the checkpoint is successfully added to CompletedCheckpointStore.
     */
    // 完成承诺。
    // 当检查点成功完成后，此 Future 会带着 CompletedCheckpoint 结果完成。
    // 这是 CheckpointCoordinator 等待检查点完成的机制。
    private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;
    // 挂起检查点统计。
    // 用于收集并保存检查点过程中子任务的统计数据（如耗时、状态大小）。
    @Nullable private final PendingCheckpointStats pendingCheckpointStats;
    // Master 触发完成承诺。
    // 用于等待所有 Master 状态的触发和收集操作完成。
    private final CompletableFuture<Void> masterTriggerCompletionPromise;

    /** Target storage location to persist the checkpoint metadata to. */
    // 目标存储位置。
    // 检查点元数据（Metadata）将要存储的位置信息。
    @Nullable private CheckpointStorageLocation targetLocation;
    // 已确认任务数量。
    // 已确认检查点的任务计数器。
    private int numAcknowledgedTasks;
    // 已处理标志。
    // 标记此 PendingCheckpoint 实例是否已经被处理（无论是成功完成还是失败/取消），不再接受新的 ACK 消息。
    private boolean disposed;
    // 已丢弃标志。
    // 标记检查点是否已经释放了其关联的所有状态资源（即调用了 discard() 方法）。
    private boolean discarded;
    // 取消句柄。
    // 由定时器调度的任务句柄，用于在检查点超时时取消检查点。
    private volatile ScheduledFuture<?> cancellerHandle;
    // 失败原因。
    // 如果检查点失败，存储导致失败的异常信息。
    private CheckpointException failureCause;

    // --------------------------------------------------------------------------------------------

    public PendingCheckpoint(
            JobID jobId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointPlan checkpointPlan,
            Collection<OperatorID> operatorCoordinatorsToConfirm,
            Collection<String> masterStateIdentifiers,
            CheckpointProperties props,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            CompletableFuture<Void> masterTriggerCompletionPromise) {
        checkArgument(
                checkpointPlan.getTasksToWaitFor().size() > 0,
                "Checkpoint needs at least one vertex that commits the checkpoint");

        this.jobId = checkNotNull(jobId);
        this.checkpointId = checkpointId;
        this.checkpointTimestamp = checkpointTimestamp;
        this.checkpointPlan = checkNotNull(checkpointPlan);

        this.notYetAcknowledgedTasks =
                CollectionUtil.newHashMapWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        for (Execution execution : checkpointPlan.getTasksToWaitFor()) {
            notYetAcknowledgedTasks.put(execution.getAttemptId(), execution.getVertex());
        }

        this.props = checkNotNull(props);

        this.operatorStates = new HashMap<>();
        this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
        this.notYetAcknowledgedMasterStates =
                masterStateIdentifiers.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(masterStateIdentifiers);
        this.notYetAcknowledgedOperatorCoordinators =
                operatorCoordinatorsToConfirm.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(operatorCoordinatorsToConfirm);
        this.acknowledgedTasks =
                CollectionUtil.newHashSetWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        this.onCompletionPromise = checkNotNull(onCompletionPromise);
        this.pendingCheckpointStats = pendingCheckpointStats;
        this.masterTriggerCompletionPromise = checkNotNull(masterTriggerCompletionPromise);
    }

    // --------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return jobId;
    }

    @Override
    public long getCheckpointID() {
        return checkpointId;
    }

    public void setCheckpointTargetLocation(CheckpointStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    public CheckpointStorageLocation getCheckpointStorageLocation() {
        return targetLocation;
    }

    public long getCheckpointTimestamp() {
        return checkpointTimestamp;
    }

    public int getNumberOfNonAcknowledgedTasks() {
        return notYetAcknowledgedTasks.size();
    }

    public int getNumberOfNonAcknowledgedOperatorCoordinators() {
        return notYetAcknowledgedOperatorCoordinators.size();
    }

    public CheckpointPlan getCheckpointPlan() {
        return checkpointPlan;
    }

    public int getNumberOfAcknowledgedTasks() {
        return numAcknowledgedTasks;
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public List<MasterState> getMasterStates() {
        return masterStates;
    }

    public boolean isFullyAcknowledged() {
        return areTasksFullyAcknowledged()
                && areCoordinatorsFullyAcknowledged()
                && areMasterStatesFullyAcknowledged();
    }

    boolean areMasterStatesFullyAcknowledged() {
        return notYetAcknowledgedMasterStates.isEmpty() && !disposed;
    }

    boolean areCoordinatorsFullyAcknowledged() {
        return notYetAcknowledgedOperatorCoordinators.isEmpty() && !disposed;
    }

    boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty() && !disposed;
    }

    public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
        return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
    }

    public boolean isDisposed() {
        return disposed;
    }

    /**
     * Checks whether this checkpoint can be subsumed or whether it should always continue,
     * regardless of newer checkpoints in progress.
     *
     * @return True if the checkpoint can be subsumed, false otherwise.
     */
    public boolean canBeSubsumed() {
        // If the checkpoint is forced, it cannot be subsumed.
        return !props.isSavepoint();
    }

    CheckpointProperties getProps() {
        return props;
    }

    /**
     * Sets the handle for the canceller to this pending checkpoint. This method fails with an
     * exception if a handle has already been set.
     *
     * @return true, if the handle was set, false, if the checkpoint is already disposed;
     */
    public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
        synchronized (lock) {
            if (this.cancellerHandle == null) {
                if (!disposed) {
                    this.cancellerHandle = cancellerHandle;
                    return true;
                } else {
                    return false;
                }
            } else {
                throw new IllegalStateException("A canceller handle was already set");
            }
        }
    }

    public CheckpointException getFailureCause() {
        return failureCause;
    }

    // ------------------------------------------------------------------------
    //  Progress and Completion
    // ------------------------------------------------------------------------

    /**
     * Returns the completion future.
     *
     * @return A future to the completed checkpoint
     */
    public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
        return onCompletionPromise;
    }

    public CompletedCheckpoint finalizeCheckpoint(
            CheckpointsCleaner checkpointsCleaner, Runnable postCleanup, Executor executor)
            throws IOException {

        synchronized (lock) {
            checkState(!isDisposed(), "checkpoint is discarded");
            checkState(
                    isFullyAcknowledged(),
                    "Pending checkpoint has not been fully acknowledged yet");

            // make sure we fulfill the promise with an exception if something fails
            try {
                checkpointPlan.fulfillFinishedTaskStatus(operatorStates);

                // write out the metadata
                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(
                                checkpointId, operatorStates.values(), masterStates, props);
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }

                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                checkpointTimestamp,
                                System.currentTimeMillis(),
                                operatorStates,
                                masterStates,
                                props,
                                finalizedLocation,
                                toCompletedCheckpointStats(finalizedLocation));

                // mark this pending checkpoint as disposed, but do NOT drop the state
                dispose(false, checkpointsCleaner, postCleanup, executor);

                return completed;
            } catch (Throwable t) {
                onCompletionPromise.completeExceptionally(t);
                ExceptionUtils.rethrowIOException(t);
                return null; // silence the compiler
            }
        }
    }

    @Nullable
    private CompletedCheckpointStats toCompletedCheckpointStats(
            CompletedCheckpointStorageLocation finalizedLocation) {
        return pendingCheckpointStats != null
                ? pendingCheckpointStats.toCompletedCheckpointStats(
                        finalizedLocation.getExternalPointer())
                : null;
    }

    /**
     * Acknowledges the task with the given execution attempt id and the given subtask state.
     *
     * @param executionAttemptId of the acknowledged task
     * @param operatorSubtaskStates of the acknowledged task
     * @param metrics Checkpoint metrics for the stats
     * @return TaskAcknowledgeResult of the operation
     */
    public TaskAcknowledgeResult acknowledgeTask(
            ExecutionAttemptID executionAttemptId,
            TaskStateSnapshot operatorSubtaskStates,
            CheckpointMetrics metrics) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

            if (vertex == null) {
                if (acknowledgedTasks.contains(executionAttemptId)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
            } else {
                acknowledgedTasks.add(executionAttemptId);
            }

            long ackTimestamp = System.currentTimeMillis();
            if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskDeployedAsFinished()) {
                checkpointPlan.reportTaskFinishedOnRestore(vertex);
            } else {
                List<OperatorIDPair> operatorIDs = vertex.getJobVertex().getOperatorIDs();
                for (OperatorIDPair operatorID : operatorIDs) {
                    updateOperatorState(vertex, operatorSubtaskStates, operatorID);
                }

                if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskFinished()) {
                    checkpointPlan.reportTaskHasFinishedOperators(vertex);
                }
            }

            ++numAcknowledgedTasks;

            // publish the checkpoint statistics
            // to prevent null-pointers from concurrent modification, copy reference onto stack
            if (pendingCheckpointStats != null) {
                // Do this in millis because the web frontend works with them
                long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
                long checkpointStartDelayMillis =
                        metrics.getCheckpointStartDelayNanos() / 1_000_000;

                SubtaskStateStats subtaskStateStats =
                        new SubtaskStateStats(
                                vertex.getParallelSubtaskIndex(),
                                ackTimestamp,
                                metrics.getBytesPersistedOfThisCheckpoint(),
                                metrics.getTotalBytesPersisted(),
                                metrics.getSyncDurationMillis(),
                                metrics.getAsyncDurationMillis(),
                                metrics.getBytesProcessedDuringAlignment(),
                                metrics.getBytesPersistedDuringAlignment(),
                                alignmentDurationMillis,
                                checkpointStartDelayMillis,
                                metrics.getUnalignedCheckpoint(),
                                true);

                LOG.trace(
                        "Checkpoint {} stats for {}: size={}Kb, duration={}ms, sync part={}ms, async part={}ms",
                        checkpointId,
                        vertex.getTaskNameWithSubtaskIndex(),
                        subtaskStateStats.getStateSize() == 0
                                ? 0
                                : subtaskStateStats.getStateSize() / 1024,
                        subtaskStateStats.getEndToEndDuration(
                                pendingCheckpointStats.getTriggerTimestamp()),
                        subtaskStateStats.getSyncCheckpointDuration(),
                        subtaskStateStats.getAsyncCheckpointDuration());
                pendingCheckpointStats.reportSubtaskStats(
                        vertex.getJobvertexId(), subtaskStateStats);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    private void updateOperatorState(
            ExecutionVertex vertex,
            TaskStateSnapshot operatorSubtaskStates,
            OperatorIDPair operatorID) {
        OperatorState operatorState = operatorStates.get(operatorID.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new OperatorState(
                            operatorID.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
        }
        OperatorSubtaskState operatorSubtaskState =
                operatorSubtaskStates == null
                        ? null
                        : operatorSubtaskStates.getSubtaskStateByOperatorID(
                                operatorID.getGeneratedOperatorID());

        if (operatorSubtaskState != null) {
            operatorState.putState(vertex.getParallelSubtaskIndex(), operatorSubtaskState);
        }
    }

    public TaskAcknowledgeResult acknowledgeCoordinatorState(
            OperatorInfo coordinatorInfo, @Nullable ByteStreamStateHandle stateHandle) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final OperatorID operatorId = coordinatorInfo.operatorId();
            OperatorState operatorState = operatorStates.get(operatorId);

            // sanity check for better error reporting
            if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
                return operatorState != null && operatorState.getCoordinatorState() != null
                        ? TaskAcknowledgeResult.DUPLICATE
                        : TaskAcknowledgeResult.UNKNOWN;
            }

            if (operatorState == null) {
                operatorState =
                        new OperatorState(
                                operatorId,
                                coordinatorInfo.currentParallelism(),
                                coordinatorInfo.maxParallelism());
                operatorStates.put(operatorId, operatorState);
            }
            if (stateHandle != null) {
                operatorState.setCoordinatorState(stateHandle);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    /**
     * Acknowledges a master state (state generated on the checkpoint coordinator) to the pending
     * checkpoint.
     *
     * @param identifier The identifier of the master state
     * @param state The state to acknowledge
     */
    public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

        synchronized (lock) {
            if (!disposed) {
                if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
                    masterStates.add(state);
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Cancellation
    // ------------------------------------------------------------------------

    /** Aborts a checkpoint with reason and cause. */
    public void abort(
            CheckpointFailureReason reason,
            @Nullable Throwable cause,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor,
            CheckpointStatsTracker statsTracker) {
        try {
            failureCause = new CheckpointException(reason, cause);
            onCompletionPromise.completeExceptionally(failureCause);
            masterTriggerCompletionPromise.completeExceptionally(failureCause);
            assertAbortSubsumedForced(reason);
        } finally {
            dispose(true, checkpointsCleaner, postCleanup, executor);
        }
    }

    private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
        if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
            throw new IllegalStateException(
                    "Bug: savepoints must never be subsumed, "
                            + "the abort reason is : "
                            + reason.message());
        }
    }

    private void dispose(
            boolean releaseState,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {

        synchronized (lock) {
            try {
                numAcknowledgedTasks = -1;
                checkpointsCleaner.cleanCheckpoint(this, releaseState, postCleanup, executor);
            } finally {
                disposed = true;
                notYetAcknowledgedTasks.clear();
                acknowledgedTasks.clear();
                cancelCanceller();
            }
        }
    }

    @Override
    public DiscardObject markAsDiscarded() {
        return new PendingCheckpointDiscardObject();
    }

    private void cancelCanceller() {
        try {
            final ScheduledFuture<?> canceller = this.cancellerHandle;
            if (canceller != null) {
                canceller.cancel(false);
            }
        } catch (Exception e) {
            // this code should not throw exceptions
            LOG.warn("Error while cancelling checkpoint timeout task", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
                checkpointId,
                checkpointTimestamp,
                getNumberOfAcknowledgedTasks(),
                getNumberOfNonAcknowledgedTasks());
    }

    /**
     * Implementation of {@link org.apache.flink.runtime.checkpoint.Checkpoint.DiscardObject} for
     * {@link PendingCheckpoint}.
     */
    public class PendingCheckpointDiscardObject implements DiscardObject {
        /**
         * Discard state. Must be called after {@link #dispose(boolean, CheckpointsCleaner,
         * Runnable, Executor) dispose}.
         */
        @Override
        public void discard() {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                    return;
                } else {
                    discarded = true;
                }
            }
            // discard the private states.
            // unregistered shared states are still considered private at this point.
            try {
                StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
                if (targetLocation != null) {
                    targetLocation.disposeOnFailure();
                }
            } catch (Throwable t) {
                LOG.warn(
                        "Could not properly dispose the private states in the pending checkpoint {} of job {}.",
                        checkpointId,
                        jobId,
                        t);
            } finally {
                operatorStates.clear();
            }
        }

        @Override
        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                } else {
                    discarded = true;
                }
            }
            List<StateObject> discardables =
                    operatorStates.values().stream()
                            .flatMap(op -> op.getDiscardables().stream())
                            .collect(Collectors.toList());

            ConjunctFuture<Void> discardStates =
                    FutureUtils.completeAll(
                            discardables.stream()
                                    .map(
                                            item ->
                                                    FutureUtils.runAsync(
                                                            item::discardState, ioExecutor))
                                    .collect(Collectors.toList()));

            return FutureUtils.runAfterwards(
                    discardStates,
                    () -> {
                        operatorStates.clear();
                        if (targetLocation != null) {
                            targetLocation.disposeOnFailure();
                        }
                    });
        }
    }
}
