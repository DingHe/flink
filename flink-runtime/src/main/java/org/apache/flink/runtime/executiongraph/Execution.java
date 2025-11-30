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
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.INITIALIZING;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.runtime.execution.ExecutionState.SCHEDULED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single
 * execution of that vertex and the resources.
 *
 * <h2>Lock free state transitions</h2>
 *
 * <p>In several points of the code, we need to deal with possible concurrent state changes and
 * actions. For example, while the call to deploy a task (send it to the TaskManager) happens, the
 * task gets cancelled.
 *
 * <p>We could lock the entire portion of the code (decision to deploy, deploy, set state to
 * running) such that it is guaranteed that any "cancel command" will only pick up after deployment
 * is done and that the "cancel command" call will never overtake the deploying call.
 *
 * <p>This blocks the threads big time, because the remote calls may take long. Depending of their
 * locking behavior, it may even result in distributed deadlocks (unless carefully avoided). We
 * therefore use atomic state updates and occasional double-checking to ensure that the state after
 * a completed call is as expected, and trigger correcting actions if it is not. Many actions are
 * also idempotent (like canceling).
 */
// Execution 类是 Flink 执行图 (ExecutionGraph) 中的核心运行时对象，它代表了对一个特定并行子任务（ExecutionVertex）的单次执行尝试。
// 单次尝试的生命周期管理： ExecutionVertex 可以被多次执行（例如，为了恢复或重试），而 Execution 对象则负责跟踪和管理其中一次尝试的完整生命周期（从创建到终止）。
// 状态机： 它管理着任务执行的状态机，包括 CREATED、SCHEDULED、DEPLOYING、RUNNING、FINISHED、CANCELED、FAILED 等状态之间的原子转换。
// 资源绑定： 它记录了任务被分配到的**逻辑槽（LogicalSlot）**和相应的 TaskManager 位置。
// 通信和部署： 它负责与 TaskManager 进行通信，执行任务的部署、取消、暂停等操作，并处理来自 TaskManager 的状态更新。
// 元数据收集： 它收集和存储了该次执行尝试的关键元数据，包括尝试 ID、状态时间戳、失败原因、累加器结果以及用于恢复的检查点信息。
// Execution 是 Flink 调度和任务控制的中心，它将一个抽象的执行尝试转化为一个具体、可控制、可跟踪的物理运行时实例。

public class Execution
        implements AccessExecution, Archiveable<ArchivedExecution>, LogicalSlot.Payload {

    private static final Logger LOG = DefaultExecutionGraph.LOG;
    //定义取消任务时的最大重试次数，值为 3
    private static final int NUM_CANCEL_CALL_TRIES = 3;

    // --------------------------------------------------------------------------------------------

    /** The executor which is used to execute futures. */
    // 异步执行器。
    // 用于执行 Future 的回调和异步 RPC 逻辑，防止阻塞 JobMaster 主线程。
    private final Executor executor;

    /** The execution vertex whose task this execution executes. */
    // 所属执行顶点。
    // 指向该执行尝试所属的并行子任务容器 (ExecutionVertex)。
    private final ExecutionVertex vertex;

    /** The unique ID marking the specific execution instant of the task. */
    // 执行尝试 ID。
    // 标识这次执行尝试的全局唯一 ID，包含 JobID、ExecutionVertexID 和尝试编号。
    private ExecutionAttemptID attemptId;

    /**
     * The timestamps when state transitions occurred, indexed by {@link ExecutionState#ordinal()}.
     */
    // 状态开始时间戳。
    // 记录任务进入每个 ExecutionState 的时间戳。
    private final long[] stateTimestamps;

    /**
     * The end timestamps when state transitions occurred, indexed by {@link
     * ExecutionState#ordinal()}.
     */
    // 状态结束时间戳。
    // 记录任务离开（终止）每个 ExecutionState 的时间戳。
    private final long[] stateEndTimestamps;
    // 用于部署、取消等操作的 RPC 调用超时时间。
    private final Time rpcTimeout;
    // 分区信息。
    // 用于缓存分区信息的集合，通常用于调度或网络连接。
    private final Collection<PartitionInfo> partitionInfos;

    /** A future that completes once the Execution reaches a terminal ExecutionState.*/
    // 终止状态 Future。
    // 在任务达到任何终止状态（FINISHED、FAILED、CANCELED）时完成。
    private final CompletableFuture<ExecutionState> terminalStateFuture;
   // 确保资源在任务结束后正确释放
    // 在任务达到终止状态且分配的资源被释放后完成。
    private final CompletableFuture<?> releaseFuture;
    // 表示任务分配的 TaskManager 的位置，异步获取
    // 在任务被分配到资源时，异步完成，提供 TaskManager 的位置信息。
    private final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture;

    /**
     * Gets completed successfully when the task switched to {@link ExecutionState#INITIALIZING} or
     * {@link ExecutionState#RUNNING}. If the task never switches to those state, but fails
     * immediately, then this future never completes.
     */
    // 在任务状态进入 INITIALIZING 或 RUNNING 时完成，表示任务已开始在 TaskManager 上运行。
    private final CompletableFuture<?> initializingOrRunningFuture;
    // 当前执行状态。
    // 任务的当前状态，使用 volatile 关键字保证可见性。
    private volatile ExecutionState state = CREATED;
    // 任务当前被分配到的计算资源槽。
    private LogicalSlot assignedResource;
    // 如果任务失败，记录失败的异常信息和时间。
    // 一旦设置，通常不会改变
    private Optional<ErrorInfo> failureCause =
            Optional.empty(); // once an ErrorInfo is set, never changes

    /**
     * Information to restore the task on recovery, such as checkpoint id and task state snapshot.
     */
    // 用于任务在故障恢复时重新加载状态（如检查点 ID 和状态快照）。
    @Nullable private JobManagerTaskRestore taskRestore;

    /** This field holds the allocation id once it was assigned successfully. */
    // 分配给该任务的资源分配的唯一 ID。
    @Nullable private AllocationID assignedAllocationID;

    // ------------------------ Accumulators & Metrics ------------------------

    /**
     * Lock for updating the accumulators atomically. Prevents final accumulators to be overwritten
     * by partial accumulators on a late heartbeat.
     */
    // 用于同步更新用户定义的累加器，防止并发修改。
    private final Object accumulatorLock = new Object();
    //存储用户定义的、持续更新的累加器。
    /* Continuously updated map of user-defined accumulators */
    private Map<String, Accumulator<?, ?>> userAccumulators;
    //用于监控任务的 I/O 性能
    private IOMetrics ioMetrics;
    // 任务生产的中间结果分区的部署信息。
    private Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>
            producedPartitions;

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a new Execution attempt.
     *
     * @param executor The executor used to dispatch callbacks from futures and asynchronous RPC
     *     calls.
     * @param vertex The execution vertex to which this Execution belongs
     * @param attemptNumber The execution attempt number.
     * @param startTimestamp The timestamp that marks the creation of this Execution
     * @param rpcTimeout The rpcTimeout for RPC calls like deploy/cancel/stop.
     */
    public Execution(
            Executor executor,
            ExecutionVertex vertex,
            int attemptNumber,
            long startTimestamp,
            Time rpcTimeout) {

        this.executor = checkNotNull(executor);
        this.vertex = checkNotNull(vertex);
        this.attemptId =
                new ExecutionAttemptID(
                        vertex.getExecutionGraphAccessor().getExecutionGraphID(),
                        vertex.getID(),
                        attemptNumber);
        this.rpcTimeout = checkNotNull(rpcTimeout);

        this.stateTimestamps = new long[ExecutionState.values().length];
        this.stateEndTimestamps = new long[ExecutionState.values().length];
        markTimestamp(CREATED, startTimestamp);

        this.partitionInfos = new ArrayList<>(16);
        this.producedPartitions = Collections.emptyMap();
        this.terminalStateFuture = new CompletableFuture<>();
        this.releaseFuture = new CompletableFuture<>();
        this.taskManagerLocationFuture = new CompletableFuture<>();
        this.initializingOrRunningFuture = new CompletableFuture<>();

        this.assignedResource = null;
    }

    // --------------------------------------------------------------------------------------------
    //   Properties
    // --------------------------------------------------------------------------------------------

    public ExecutionVertex getVertex() {
        return vertex;
    }

    @Override
    public ExecutionAttemptID getAttemptId() {
        return attemptId;
    }

    @Override
    public int getAttemptNumber() {
        return attemptId.getAttemptNumber();
    }

    @Override
    public ExecutionState getState() {
        return state;
    }

    @Nullable
    public AllocationID getAssignedAllocationID() {
        return assignedAllocationID;
    }

    public CompletableFuture<TaskManagerLocation> getTaskManagerLocationFuture() {
        return taskManagerLocationFuture;
    }

    public LogicalSlot getAssignedResource() {
        return assignedResource;
    }

    public Optional<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptor(
            IntermediateResultPartitionID id) {
        return Optional.ofNullable(producedPartitions.get(id));
    }

    /**
     * Tries to assign the given slot to the execution. The assignment works only if the Execution
     * is in state SCHEDULED. Returns true, if the resource could be assigned.
     *
     * @param logicalSlot to assign to this execution
     * @return true if the slot could be assigned to the execution, otherwise false
     */
    // 用于尝试将一个逻辑资源槽 (LogicalSlot) 分配给一个任务的执行尝试 (Execution)。这是 Flink 任务从调度到实际部署过程中的关键一步。
    public boolean tryAssignResource(final LogicalSlot logicalSlot) {

        assertRunningInJobMasterMainThread();

        checkNotNull(logicalSlot);

        // only allow to set the assigned resource in state SCHEDULED or CREATED
        // note: we also accept resource assignment when being in state CREATED for testing purposes
        // 判断当前 Execution 实例的状态是否为 SCHEDULED 或 CREATED。
        // 如果不是，则资源分配失败，跳转到代码末尾的 return false;。
        if (state == SCHEDULED || state == CREATED) {
            if (assignedResource == null) {
                // 临时分配资源。
                assignedResource = logicalSlot;
                // 尝试将当前的 Execution 实例（作为 LogicalSlot.Payload）绑定到 logicalSlot 上。
                // LogicalSlot 确保一个槽只能被一个 Payload 成功绑定一次。
                if (logicalSlot.tryAssignPayload(this)) {
                    // check for concurrent modification (e.g. cancelling call)
                    // 再次检查状态是否仍为 SCHEDULED 或 CREATED，
                    // 并且确认 taskManagerLocationFuture 尚未完成（表示任务还未被取消或部署流程尚未结束）。
                    if ((state == SCHEDULED || state == CREATED)
                            && !taskManagerLocationFuture.isDone()) {
                        taskManagerLocationFuture.complete(logicalSlot.getTaskManagerLocation());
                        assignedAllocationID = logicalSlot.getAllocationId();
                        getVertex()
                                .setLatestPriorSlotAllocation(
                                        assignedResource.getTaskManagerLocation(),
                                        logicalSlot.getAllocationId());
                        return true;
                    } else {
                        // free assigned resource and return false
                        assignedResource = null;
                        return false;
                    }
                } else {
                    assignedResource = null;
                    return false;
                }
            } else {
                // the slot already has another slot assigned
                return false;
            }
        } else {
            // do not allow resource assignment if we are not in state SCHEDULED
            return false;
        }
    }
    // 用于获取当前任务执行尝试（Execution）需要处理的下一个输入切片 (InputSplit)。
    // 这是任务在 TaskManager 上启动和开始处理数据的前提。
    public Optional<InputSplit> getNextInputSplit() {
        final LogicalSlot slot = this.getAssignedResource();
        final String host = slot != null ? slot.getTaskManagerLocation().getHostname() : null;
        return this.vertex.getNextInputSplit(host, getAttemptNumber());
    }
    // 用于获取当前任务执行尝试（Execution）所分配的TaskManager 的位置信息。
    @Override
    public TaskManagerLocation getAssignedResourceLocation() {
        // returns non-null only when a location is already assigned
        final LogicalSlot currentAssignedResource = assignedResource;
        return currentAssignedResource != null
                ? currentAssignedResource.getTaskManagerLocation()
                : null;
    }

    @Override
    public Optional<ErrorInfo> getFailureInfo() {
        return failureCause;
    }

    @Override
    public long[] getStateTimestamps() {
        return stateTimestamps;
    }

    @Override
    public long[] getStateEndTimestamps() {
        return stateEndTimestamps;
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        return this.stateTimestamps[state.ordinal()];
    }

    @Override
    public long getStateEndTimestamp(ExecutionState state) {
        return this.stateEndTimestamps[state.ordinal()];
    }

    public boolean isFinished() {
        return state.isTerminal();
    }

    @Nullable
    public JobManagerTaskRestore getTaskRestore() {
        return taskRestore;
    }

    /**
     * Sets the initial state for the execution. The serialized state is then shipped via the {@link
     * TaskDeploymentDescriptor} to the TaskManagers.
     *
     * @param taskRestore information to restore the state
     */
    public void setInitialState(JobManagerTaskRestore taskRestore) {
        this.taskRestore = taskRestore;
    }

    /**
     * Gets a future that completes once the task execution reaches one of the states {@link
     * ExecutionState#INITIALIZING} or {@link ExecutionState#RUNNING}. If this task never reaches
     * these states (for example because the task is cancelled before it was properly deployed and
     * restored), then this future will never complete.
     *
     * <p>The future is completed already in the {@link ExecutionState#INITIALIZING} state, because
     * various running actions are already possible in that state (the task already accepts and
     * sends events and network data for task recovery). (Note that in earlier versions, the
     * INITIALIZING state was not separate but part of the RUNNING state).
     *
     * <p>This future is always completed from the job master's main thread.
     */
    public CompletableFuture<?> getInitializingOrRunningFuture() {
        return initializingOrRunningFuture;
    }

    /**
     * Gets a future that completes once the task execution reaches a terminal state. The future
     * will be completed with specific state that the execution reached. This future is always
     * completed from the job master's main thread.
     *
     * @return A future which is completed once the execution reaches a terminal state
     */
    @Override
    public CompletableFuture<ExecutionState> getTerminalStateFuture() {
        return terminalStateFuture;
    }

    /**
     * Gets the release future which is completed once the execution reaches a terminal state and
     * the assigned resource has been released. This future is always completed from the job
     * master's main thread.
     *
     * @return A future which is completed once the assigned resource has been released
     */
    public CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  Actions
    // --------------------------------------------------------------------------------------------
    // 用于触发并处理该任务执行尝试（Execution）所产生的所有中间结果分区的注册过程。
    // 它是任务部署流程中的重要一环，确保任务在开始执行前，其输出数据路径已在 JobMaster 和 ShuffleMaster 中设置完毕
    // 接收任务被分配到的 TaskManagerLocation 作为参数。
    public CompletableFuture<Void> registerProducedPartitions(TaskManagerLocation location) {

        assertRunningInJobMasterMainThread();

        return FutureUtils.thenApplyAsyncIfNotDone(
                // 调用 静态的 registerProducedPartitions 辅助方法（上一个回复中解读的方法），
                // 该方法负责异步地与 ShuffleMaster 交互，获取所有分区的部署描述符。
                registerProducedPartitions(vertex, location, attemptId),
                vertex.getExecutionGraphAccessor().getJobMasterMainThreadExecutor(),
                // 定义当静态注册方法返回所有分区描述符 Map (producedPartitionsCache) 后的回调逻辑。
                producedPartitionsCache -> {
                    // 将成功获取的分区部署描述符 Map 赋值给当前 Execution 实例的成员变量 (producedPartitions)，
                    // 供后续部署 TaskManager 使用。
                    producedPartitions = producedPartitionsCache;
                    // 检查当前 Execution 的状态是否仍为 SCHEDULED（已调度）。
                    // 这是预期的状态，表示任务准备好被部署。
                    if (getState() == SCHEDULED) {
                        // 开始监控这些已注册分区的状态，例如，当分区数据就绪（FINISHED）时进行通知。
                        startTrackingPartitions(
                                location.getResourceID(), producedPartitionsCache.values());
                    } else {
                        // 状态检查：延迟或取消。
                        LOG.info(
                                "Discarding late registered partitions for {} task {}.",
                                getState(),
                                attemptId);
                        for (ResultPartitionDeploymentDescriptor desc :
                                producedPartitionsCache.values()) {
                            getVertex()
                                    .getExecutionGraphAccessor()
                                    .getShuffleMaster()
                                    // 调用 ShuffleMaster 的 releasePartitionExternally 方法，
                                    // 立即释放这些分区所关联的外部资源（例如，在外部 Shuffle 服务或分层存储中预留的资源），防止资源泄漏。
                                    .releasePartitionExternally(desc.getShuffleDescriptor());
                        }
                    }
                    return null;
                });
    }

    private void recoverAttempt(ExecutionAttemptID newId) {
        if (!this.attemptId.equals(newId)) {
            getVertex().getExecutionGraphAccessor().deregisterExecution(this);
            this.attemptId = newId;
            getVertex().getExecutionGraphAccessor().registerExecution(this);
        }
    }

    /** Recover the execution attempt status after JM failover. */
    public void recoverExecution(
            ExecutionAttemptID attemptId,
            TaskManagerLocation location,
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics) {
        recoverAttempt(attemptId);
        taskManagerLocationFuture.complete(location);

        try {
            transitionState(this.state, FINISHED);
            finishPartitionsAndUpdateConsumers();
            updateAccumulatorsAndMetrics(userAccumulators, metrics);
            releaseAssignedResource(null);
            vertex.getExecutionGraphAccessor().deregisterExecution(this);
        } finally {
            vertex.executionFinished(this);
        }
    }

    public void recoverProducedPartitions(
            Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>
                    producedPartitions) {
        this.producedPartitions = checkNotNull(producedPartitions);
    }
    // Flink 在任务部署到 TaskManager 之前执行的关键步骤之一。
    // 它的作用是向 ShuffleMaster 注册该任务（Execution）将要产生的所有中间结果分区，
    // 并获取用于 TaskManager 部署的结果分区部署描述符 (ResultPartitionDeploymentDescriptor)。
    // ExecutionVertex（任务所在的顶点）
    // TaskManagerLocation（任务被分配到的 TaskManager 位置）
    // ExecutionAttemptID（当前执行尝试 ID）。
    private static CompletableFuture<
                    Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>>
            registerProducedPartitions(
                    ExecutionVertex vertex,
                    TaskManagerLocation location,
                    ExecutionAttemptID attemptId) {

        // 创建生产者描述符。
        // 该描述符包含了该任务作为数据生产者的物理位置和连接信息
        ProducerDescriptor producerDescriptor = ProducerDescriptor.create(location, attemptId);
        // 获取生产的分区集合。
        // 从 ExecutionVertex 中获取该任务产生的所有 IntermediateResultPartition 对象集合。
        Collection<IntermediateResultPartition> partitions =
                vertex.getProducedPartitions().values();

        Collection<CompletableFuture<ResultPartitionDeploymentDescriptor>> partitionRegistrations =
                new ArrayList<>(partitions.size());

        for (IntermediateResultPartition partition : partitions) {
            // 为当前 IntermediateResultPartition 创建一个 PartitionDescriptor。
            // 该描述符包含了分区的逻辑结构和类型信息。
            PartitionDescriptor partitionDescriptor = PartitionDescriptor.from(partition);
            // 调用 ShuffleMaster 注册。
            CompletableFuture<? extends ShuffleDescriptor> shuffleDescriptorFuture =
                    vertex.getExecutionGraphAccessor()
                            .getShuffleMaster()
                            .registerPartitionWithProducer(
                                    vertex.getJobId(), partitionDescriptor, producerDescriptor);

            CompletableFuture<ResultPartitionDeploymentDescriptor> partitionRegistration =
                    shuffleDescriptorFuture.thenApply(
                            // 创建部署描述符。
                            shuffleDescriptor ->
                                    createResultPartitionDeploymentDescriptor(
                                            partitionDescriptor, partition, shuffleDescriptor));
            partitionRegistrations.add(partitionRegistration);
        }

        return FutureUtils.combineAll(partitionRegistrations)
                .thenApply(
                        rpdds -> {
                            Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>
                                    producedPartitions =
                                            CollectionUtil.newLinkedHashMapWithExpectedSize(
                                                    partitions.size());
                            rpdds.forEach(
                                    rpdd -> producedPartitions.put(rpdd.getPartitionId(), rpdd));
                            return producedPartitions;
                        });
    }

    private static int getPartitionMaxParallelism(IntermediateResultPartition partition) {
        return partition.getIntermediateResult().getConsumersMaxParallelism();
    }

    public static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
            IntermediateResultPartition partition, ShuffleDescriptor shuffleDescriptor) {
        PartitionDescriptor partitionDescriptor = PartitionDescriptor.from(partition);
        return createResultPartitionDeploymentDescriptor(
                partitionDescriptor, partition, shuffleDescriptor);
    }

    // 创建分区部署描述
    private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
            PartitionDescriptor partitionDescriptor,
            IntermediateResultPartition partition,
            ShuffleDescriptor shuffleDescriptor) {
        return new ResultPartitionDeploymentDescriptor(
                partitionDescriptor, shuffleDescriptor, getPartitionMaxParallelism(partition));
    }

    /**
     * Deploys the execution to the previously assigned resource.
     *
     * @throws JobException if the execution cannot be deployed to the assigned resource
     */
    // 负责将任务的执行尝试（Execution）真正部署到已经分配好的 TaskManager 资源槽（LogicalSlot）上。
    // 这是 Flink 任务调度和生命周期管理的核心步骤。
    public void deploy() throws JobException {
        // 断言当前操作在 JobMaster 的主线程中执行，确保状态转换和资源访问的线程安全。
        assertRunningInJobMasterMainThread();

        final LogicalSlot slot = assignedResource;

        checkNotNull(
                slot,
                "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

        // Check if the TaskManager died in the meantime
        // This only speeds up the response to TaskManagers failing concurrently to deployments.
        // The more general check is the rpcTimeout of the deployment call
        if (!slot.isAlive()) {
            throw new JobException("Target slot (TaskManager) for deployment is no longer alive.");
        }

        // make sure exactly one deployment call happens from the correct state
        ExecutionState previous = this.state;
        // 确保只在正确的状态 (SCHEDULED) 下发生一次部署调用。
        if (previous == SCHEDULED) {
            if (!transitionState(previous, DEPLOYING)) {
                // race condition, someone else beat us to the deploying call.
                // this should actually not happen and indicates a race somewhere else
                throw new IllegalStateException(
                        "Cannot deploy task: Concurrent deployment call race.");
            }
        } else {
            // vertex may have been cancelled, or it was already scheduled
            throw new IllegalStateException(
                    "The vertex must be in SCHEDULED state to be deployed. Found state "
                            + previous);
        }

        if (this != slot.getPayload()) {
            throw new IllegalStateException(
                    String.format(
                            "The execution %s has not been assigned to the assigned slot.", this));
        }

        try {

            // race double check, did we fail/cancel and do we need to release the slot?
            if (this.state != DEPLOYING) {
                slot.releaseSlot(
                        new FlinkException(
                                "Actual state of execution "
                                        + this
                                        + " ("
                                        + state
                                        + ") does not match expected state DEPLOYING."));
                return;
            }

            LOG.info(
                    "Deploying {} (attempt #{}) with attempt id {} and vertex id {} to {} with allocation id {}",
                    vertex.getTaskNameWithSubtaskIndex(),
                    getAttemptNumber(),
                    attemptId,
                    vertex.getID(),
                    getAssignedResourceLocation(),
                    slot.getAllocationId());
            // 生成需要部署的task
            final TaskDeploymentDescriptor deployment =
                    vertex.getExecutionGraphAccessor()
                            .getTaskDeploymentDescriptorFactory()
                            .createDeploymentDescriptor(
                                    this,
                                    slot.getAllocationId(),
                                    taskRestore,
                                    producedPartitions.values());

            // null taskRestore to let it be GC'ed
            taskRestore = null;
            // 获取 TaskManager Gateway。
            // 从 LogicalSlot 中获取用于与 TaskManager 进行 RPC 通信的 TaskManagerGateway。
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
            // 再次获取 JobMaster 的主线程执行器，用于处理部署结果的回调。
            final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                    vertex.getExecutionGraphAccessor().getJobMasterMainThreadExecutor();
            // 通知父 ExecutionVertex 和 ExecutionGraph，当前任务已进入部署流程（通常用于更新监控状态）。
            getVertex().notifyPendingDeployment(this);
            // We run the submission in the future executor so that the serialization of large TDDs
            // does not block
            // the main thread and sync back to the main thread once submission is completed.
            // 异步提交任务。
            // 在指定的 executor 上异步执行提交任务的 RPC 调用，防止 TDD 序列化阻塞 JobMaster 主线程。
            CompletableFuture.supplyAsync(
                            () -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
                    .thenCompose(Function.identity())
                    .whenCompleteAsync(
                            // 异步处理结果。
                            // 在任务提交完成（无论是成功还是失败）后，在 JobMaster 主线程执行器 (jobMasterMainThreadExecutor) 上执行回调函数。
                            (ack, failure) -> {
                                if (failure == null) {
                                    vertex.notifyCompletedDeployment(this);
                                } else {
                                    final Throwable actualFailure =
                                            ExceptionUtils.stripCompletionException(failure);

                                    if (actualFailure instanceof TimeoutException) {
                                        String taskname =
                                                vertex.getTaskNameWithSubtaskIndex()
                                                        + " ("
                                                        + attemptId
                                                        + ')';

                                        markFailed(
                                                new Exception(
                                                        "Cannot deploy task "
                                                                + taskname
                                                                + " - TaskManager ("
                                                                + getAssignedResourceLocation()
                                                                + ") not responding after a rpcTimeout of "
                                                                + rpcTimeout,
                                                        actualFailure));
                                    } else {
                                        markFailed(actualFailure);
                                    }
                                }
                            },
                            jobMasterMainThreadExecutor);

        } catch (Throwable t) {
            markFailed(t);
        }
    }

    public void cancel() {
        // depending on the previous state, we go directly to cancelled (no cancel call necessary)
        // -- or to canceling (cancel call needs to be sent to the task manager)

        // because of several possibly previous states, we need to again loop until we make a
        // successful atomic state transition
        assertRunningInJobMasterMainThread();
        while (true) {

            ExecutionState current = this.state;

            if (current == CANCELING || current == CANCELED) {
                // already taken care of, no need to cancel again
                return;
            }

            // these two are the common cases where we need to send a cancel call
            else if (current == INITIALIZING || current == RUNNING || current == DEPLOYING) {
                // try to transition to canceling, if successful, send the cancel call
                if (startCancelling(NUM_CANCEL_CALL_TRIES)) {
                    return;
                }
                // else: fall through the loop
            } else if (current == FINISHED) {
                // finished before it could be cancelled.
                // in any case, the task is removed from the TaskManager already

                // a pipelined partition whose consumer has never been deployed could still be
                // buffered on the TM
                // release it here since pipelined partitions for FINISHED executions aren't handled
                // elsewhere
                // covers the following cases:
                // 		a) restarts of this vertex
                // 		b) a global failure (which may result in a FAILED job state)
                sendReleaseIntermediateResultPartitionsRpcCall();

                return;
            } else if (current == FAILED) {
                // failed before it could be cancelled.
                // in any case, the task is removed from the TaskManager already

                return;
            } else if (current == CREATED || current == SCHEDULED) {
                // from here, we can directly switch to cancelled, because no task has been deployed
                if (cancelAtomically()) {
                    return;
                }
                // else: fall through the loop
            } else {
                throw new IllegalStateException(current.name());
            }
        }
    }

    public CompletableFuture<?> suspend() {
        switch (state) {
            case RUNNING:
            case INITIALIZING:
            case DEPLOYING:
            case CREATED:
            case SCHEDULED:
                if (!cancelAtomically()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Could not directly go to %s from %s.",
                                    CANCELED.name(), state.name()));
                }
                break;
            case CANCELING:
                completeCancelling();
                break;
            case FINISHED:
                // a pipelined partition whose consumer has never been deployed could still be
                // buffered on the TM
                // release it here since pipelined partitions for FINISHED executions aren't handled
                // elsewhere
                // most notably, the TaskExecutor does not release pipelined partitions when
                // disconnecting from the JM
                sendReleaseIntermediateResultPartitionsRpcCall();
                break;
            case FAILED:
            case CANCELED:
                break;
            default:
                throw new IllegalStateException(state.name());
        }

        return releaseFuture;
    }

    private void updatePartitionConsumers(final IntermediateResultPartition partition) {
        final List<ConsumerVertexGroup> consumerVertexGroups = partition.getConsumerVertexGroups();
        if (consumerVertexGroups.isEmpty()) {
            return;
        }
        final Set<ExecutionVertexID> updatedVertices = new HashSet<>();
        for (ConsumerVertexGroup consumerVertexGroup : consumerVertexGroups) {
            for (ExecutionVertexID consumerVertexId : consumerVertexGroup) {
                if (updatedVertices.contains(consumerVertexId)) {
                    continue;
                }

                final ExecutionVertex consumerVertex =
                        vertex.getExecutionGraphAccessor()
                                .getExecutionVertexOrThrow(consumerVertexId);
                final Collection<Execution> consumers = consumerVertex.getCurrentExecutions();
                for (Execution consumer : consumers) {
                    final ExecutionState consumerState = consumer.getState();
                    // ----------------------------------------------------------------
                    // Consumer is recovering or running => send update message now
                    // Consumer is deploying => cache the partition info which would be
                    // sent after switching to running
                    // ----------------------------------------------------------------
                    if (consumerState == DEPLOYING
                            || consumerState == RUNNING
                            || consumerState == INITIALIZING) {
                        final PartitionInfo partitionInfo = createFinishedPartitionInfo(partition);
                        updatedVertices.add(consumerVertexId);

                        if (consumerState == DEPLOYING) {
                            consumerVertex.cachePartitionInfo(partitionInfo);
                        } else {
                            consumer.sendUpdatePartitionInfoRpcCall(
                                    Collections.singleton(partitionInfo));
                        }
                    }
                }
            }
        }
    }

    private static PartitionInfo createFinishedPartitionInfo(
            IntermediateResultPartition consumedPartition) {
        IntermediateDataSetID intermediateDataSetID =
                consumedPartition.getIntermediateResult().getId();
        ShuffleDescriptor shuffleDescriptor =
                getConsumedPartitionShuffleDescriptor(
                        consumedPartition,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                        // because partition is already finished, false is fair enough.
                        false);
        return new PartitionInfo(intermediateDataSetID, shuffleDescriptor);
    }

    /**
     * This method fails the vertex due to an external condition. The task will move to state
     * FAILED. If the task was in state RUNNING or DEPLOYING before, it will send a cancel call to
     * the TaskManager.
     *
     * @param t The exception that caused the task to fail.
     */
    @Override
    public void fail(Throwable t) {
        processFail(t, true);
    }

    /**
     * Notify the task of this execution about a completed checkpoint and the last subsumed
     * checkpoint id if possible.
     *
     * @param completedCheckpointId of the completed checkpoint
     * @param completedTimestamp of the completed checkpoint
     * @param lastSubsumedCheckpointId of the last subsumed checkpoint, a value of {@link
     *     org.apache.flink.runtime.checkpoint.CheckpointStoreUtil#INVALID_CHECKPOINT_ID} means no
     *     checkpoint has been subsumed.
     */
    public void notifyCheckpointOnComplete(
            long completedCheckpointId, long completedTimestamp, long lastSubsumedCheckpointId) {
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            taskManagerGateway.notifyCheckpointOnComplete(
                    attemptId,
                    getVertex().getJobId(),
                    completedCheckpointId,
                    completedTimestamp,
                    lastSubsumedCheckpointId);
        } else {
            LOG.debug(
                    "The execution has no slot assigned. This indicates that the execution is "
                            + "no longer running.");
        }
    }

    /**
     * Notify the task of this execution about a aborted checkpoint.
     *
     * @param abortCheckpointId of the subsumed checkpoint
     * @param latestCompletedCheckpointId of the latest completed checkpoint
     * @param timestamp of the subsumed checkpoint
     */
    public void notifyCheckpointAborted(
            long abortCheckpointId, long latestCompletedCheckpointId, long timestamp) {
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            taskManagerGateway.notifyCheckpointAborted(
                    attemptId,
                    getVertex().getJobId(),
                    abortCheckpointId,
                    latestCompletedCheckpointId,
                    timestamp);
        } else {
            LOG.debug(
                    "The execution has no slot assigned. This indicates that the execution is "
                            + "no longer running.");
        }
    }

    /**
     * Trigger a new checkpoint on the task of this execution.
     *
     * @param checkpointId of th checkpoint to trigger
     * @param timestamp of the checkpoint to trigger
     * @param checkpointOptions of the checkpoint to trigger
     * @return Future acknowledge which is returned once the checkpoint has been triggered
     */
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
        return triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions);
    }

    /**
     * Trigger a new checkpoint on the task of this execution.
     *
     * @param checkpointId of th checkpoint to trigger
     * @param timestamp of the checkpoint to trigger
     * @param checkpointOptions of the checkpoint to trigger
     * @return Future acknowledge which is returned once the checkpoint has been triggered
     */
    public CompletableFuture<Acknowledge> triggerSynchronousSavepoint(
            long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
        return triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions);
    }

    private CompletableFuture<Acknowledge> triggerCheckpointHelper(
            long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {

        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            return taskManagerGateway.triggerCheckpoint(
                    attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions);
        }
        LOG.debug(
                "The execution has no slot assigned. This indicates that the execution is no longer running.");
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Sends the operator event to the Task on the Task Executor.
     *
     * @return True, of the message was sent, false is the task is currently not running.
     */
    public CompletableFuture<Acknowledge> sendOperatorEvent(
            OperatorID operatorId, SerializedValue<OperatorEvent> event) {

        assertRunningInJobMasterMainThread();
        final LogicalSlot slot = assignedResource;

        if (slot != null && (getState() == RUNNING || getState() == INITIALIZING)) {
            final TaskExecutorOperatorEventGateway eventGateway = slot.getTaskManagerGateway();
            return eventGateway.sendOperatorEventToTask(getAttemptId(), operatorId, event);
        } else {
            return FutureUtils.completedExceptionally(
                    new TaskNotRunningException(
                            '"'
                                    + vertex.getTaskNameWithSubtaskIndex()
                                    + "\" is not running, but in state "
                                    + getState()));
        }
    }

    // --------------------------------------------------------------------------------------------
    //   Callbacks
    // --------------------------------------------------------------------------------------------

    /**
     * This method marks the task as failed, but will make no attempt to remove task execution from
     * the task manager. It is intended for cases where the task is known not to be running, or then
     * the TaskManager reports failure (in which case it has already removed the task).
     *
     * @param t The exception that caused the task to fail.
     */
    public void markFailed(Throwable t) {
        processFail(t, false);
    }

    void markFailed(
            Throwable t,
            boolean cancelTask,
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics,
            boolean releasePartitions,
            boolean fromSchedulerNg) {
        processFail(t, cancelTask, userAccumulators, metrics, releasePartitions, fromSchedulerNg);
    }

    @VisibleForTesting
    public void markFinished() {
        markFinished(null, null);
    }

    void markFinished(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

        assertRunningInJobMasterMainThread();

        // this call usually comes during RUNNING, but may also come while still in deploying (very
        // fast tasks!)
        while (true) {
            ExecutionState current = this.state;

            if (current == INITIALIZING || current == RUNNING || current == DEPLOYING) {

                if (transitionState(current, FINISHED)) {
                    try {
                        finishPartitionsAndUpdateConsumers();
                        updateAccumulatorsAndMetrics(userAccumulators, metrics);
                        releaseAssignedResource(null);
                        vertex.getExecutionGraphAccessor().deregisterExecution(this);
                    } finally {
                        vertex.executionFinished(this);
                    }
                    return;
                }
            } else if (current == CANCELING) {
                // we sent a cancel call, and the task manager finished before it arrived. We
                // will never get a CANCELED call back from the job manager
                completeCancelling(userAccumulators, metrics, true);
                return;
            } else if (current == CANCELED || current == FAILED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Task FINISHED, but concurrently went to state " + state);
                }
                return;
            } else {
                // this should not happen, we need to fail this
                markFailed(
                        new Exception(
                                "Vertex received FINISHED message while being in state " + state));
                return;
            }
        }
    }

    private void finishPartitionsAndUpdateConsumers() {
        final List<IntermediateResultPartition> finishedPartitions =
                getVertex().finishPartitionsIfNeeded();

        for (IntermediateResultPartition partition : finishedPartitions) {
            updatePartitionConsumers(partition);
        }
    }

    private boolean cancelAtomically() {
        if (startCancelling(0)) {
            completeCancelling();
            return true;
        } else {
            return false;
        }
    }

    private boolean startCancelling(int numberCancelRetries) {
        if (transitionState(state, CANCELING)) {
            taskManagerLocationFuture.cancel(false);
            sendCancelRpcCall(numberCancelRetries);
            return true;
        } else {
            return false;
        }
    }

    void completeCancelling() {
        completeCancelling(null, null, true);
    }

    void completeCancelling(
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics,
            boolean releasePartitions) {

        // the taskmanagers can themselves cancel tasks without an external trigger, if they find
        // that the
        // network stack is canceled (for example by a failing / canceling receiver or sender
        // this is an artifact of the old network runtime, but for now we need to support task
        // transitions
        // from running directly to canceled

        while (true) {
            ExecutionState current = this.state;

            if (current == CANCELED) {
                return;
            } else if (current == CANCELING
                    || current == RUNNING
                    || current == INITIALIZING
                    || current == DEPLOYING) {

                updateAccumulatorsAndMetrics(userAccumulators, metrics);

                if (transitionState(current, CANCELED)) {
                    finishCancellation(releasePartitions);
                    return;
                }

                // else fall through the loop
            } else {
                // failing in the meantime may happen and is no problem.
                // anything else is a serious problem !!!
                if (current != FAILED) {
                    String message =
                            String.format(
                                    "Asynchronous race: Found %s in state %s after successful cancel call.",
                                    vertex.getTaskNameWithSubtaskIndex(), state);
                    LOG.error(message);
                    vertex.getExecutionGraphAccessor().failGlobal(new Exception(message));
                }
                return;
            }
        }
    }

    private void finishCancellation(boolean releasePartitions) {
        releaseAssignedResource(new FlinkException("Execution " + this + " was cancelled."));
        vertex.getExecutionGraphAccessor().deregisterExecution(this);
        handlePartitionCleanup(releasePartitions, releasePartitions);
    }

    void cachePartitionInfo(PartitionInfo partitionInfo) {
        partitionInfos.add(partitionInfo);
    }

    private void sendPartitionInfos() {
        if (!partitionInfos.isEmpty()) {
            sendUpdatePartitionInfoRpcCall(new ArrayList<>(partitionInfos));

            partitionInfos.clear();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal Actions
    // --------------------------------------------------------------------------------------------

    private void processFail(Throwable t, boolean cancelTask) {
        processFail(t, cancelTask, null, null, true, false);
    }

    /**
     * Process a execution failure. The failure can be fired by JobManager or reported by
     * TaskManager. If it is fired by JobManager and the execution is already deployed, it needs to
     * send a PRC call to remove the task from TaskManager. It also needs to release the produced
     * partitions if it fails before deployed (because the partitions are possibly already created
     * in external shuffle service) or JobManager proactively fails it (in case that it finishes in
     * TaskManager when JobManager tries to fail it). The failure will be notified to SchedulerNG if
     * it is from within the ExecutionGraph. This is to trigger the failure handling of SchedulerNG
     * to recover this failed execution.
     *
     * @param t Failure cause
     * @param cancelTask Indicating whether to send a PRC call to remove task from TaskManager. True
     *     if the failure is fired by JobManager and the execution is already deployed. Otherwise it
     *     should be false.
     * @param userAccumulators User accumulators
     * @param metrics IO metrics
     * @param releasePartitions Indicating whether to release result partitions produced by this
     *     execution. False if the task is FAILED in TaskManager, otherwise true.
     * @param fromSchedulerNg Indicating whether the failure is from the SchedulerNg. It should be
     *     false if it is from within the ExecutionGraph.
     */
    private void processFail(
            Throwable t,
            boolean cancelTask,
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics,
            boolean releasePartitions,
            boolean fromSchedulerNg) {

        assertRunningInJobMasterMainThread();

        ExecutionState current = this.state;

        if (current == FAILED) {
            // already failed. It is enough to remember once that we failed (its sad enough)
            return;
        }

        if (current == CANCELED || current == FINISHED) {
            // we are already aborting or are already aborted or we are already finished
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Ignoring transition of vertex {} to {} while being {}.",
                        getVertexWithAttempt(),
                        FAILED,
                        current);
            }
            return;
        }

        if (current == CANCELING) {
            completeCancelling(userAccumulators, metrics, true);
            return;
        }

        if (!fromSchedulerNg) {
            vertex.getExecutionGraphAccessor()
                    .notifySchedulerNgAboutInternalTaskFailure(
                            attemptId, t, cancelTask, releasePartitions);
            return;
        }

        checkState(transitionState(current, FAILED, t));

        // success (in a manner of speaking)
        this.failureCause =
                Optional.of(
                        ErrorInfo.createErrorInfoWithNullableCause(t, getStateTimestamp(FAILED)));

        updateAccumulatorsAndMetrics(userAccumulators, metrics);

        releaseAssignedResource(t);
        vertex.getExecutionGraphAccessor().deregisterExecution(this);

        maybeReleasePartitionsAndSendCancelRpcCall(current, cancelTask, releasePartitions);
    }

    private void maybeReleasePartitionsAndSendCancelRpcCall(
            final ExecutionState stateBeforeFailed,
            final boolean cancelTask,
            final boolean releasePartitions) {

        handlePartitionCleanup(releasePartitions, releasePartitions);

        if (cancelTask
                && (stateBeforeFailed == RUNNING
                        || stateBeforeFailed == INITIALIZING
                        || stateBeforeFailed == DEPLOYING)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending out cancel request, to remove task execution from TaskManager.");
            }

            try {
                if (assignedResource != null) {
                    sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);
                }
            } catch (Throwable tt) {
                // no reason this should ever happen, but log it to be safe
                LOG.error(
                        "Error triggering cancel call while marking task {} as failed.",
                        getVertex().getTaskNameWithSubtaskIndex(),
                        tt);
            }
        }
    }

    boolean switchToInitializing() {
        if (switchTo(DEPLOYING, INITIALIZING)) {
            sendPartitionInfos();
            return true;
        }

        return false;
    }

    boolean switchToRunning() {
        return switchTo(INITIALIZING, RUNNING);
    }

    private boolean switchTo(ExecutionState from, ExecutionState to) {

        if (transitionState(from, to)) {
            return true;
        } else {
            // something happened while the call was in progress.
            // it can mean:
            //  - canceling, while deployment was in progress. state is now canceling, or canceled,
            // if the response overtook
            //  - finishing (execution and finished call overtook the deployment answer, which is
            // possible and happens for fast tasks)
            //  - failed (execution, failure, and failure message overtook the deployment answer)

            ExecutionState currentState = this.state;

            if (currentState == FINISHED || currentState == CANCELED) {
                // do nothing, the task was really fast (nice)
                // or it was canceled really fast
            } else if (currentState == CANCELING || currentState == FAILED) {
                if (LOG.isDebugEnabled()) {
                    // this log statement is guarded because the 'getVertexWithAttempt()' method
                    // performs string concatenations
                    LOG.debug(
                            "Concurrent canceling/failing of {} while deployment was in progress.",
                            getVertexWithAttempt());
                }
                sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);
            } else {
                String message =
                        String.format(
                                "Concurrent unexpected state transition of task %s from %s (expected %s) to %s while deployment was in progress.",
                                getAttemptId(), currentState, from, to);

                LOG.debug(message);

                // undo the deployment
                sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);

                // record the failure
                markFailed(new Exception(message));
            }

            return false;
        }
    }

    /**
     * This method sends a CancelTask message to the instance of the assigned slot.
     *
     * <p>The sending is tried up to NUM_CANCEL_CALL_TRIES times.
     */
    private void sendCancelRpcCall(int numberRetries) {
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
            final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                    getVertex().getExecutionGraphAccessor().getJobMasterMainThreadExecutor();

            CompletableFuture<Acknowledge> cancelResultFuture =
                    FutureUtils.retry(
                            () -> taskManagerGateway.cancelTask(attemptId, rpcTimeout),
                            numberRetries,
                            jobMasterMainThreadExecutor);

            cancelResultFuture.whenComplete(
                    (ack, failure) -> {
                        if (failure != null) {
                            fail(new Exception("Task could not be canceled.", failure));
                        }
                    });
        }
    }
    // 启动对该任务所有输出中间结果分区状态的跟踪和监控
    // 接收任务所在的 TaskExecutor (TaskManager) 的唯一 ID。
    // 这是分区所在的物理位置信息。
    // 接收该任务要产生的所有中间结果分区的部署描述符集合。这些描述符包含了分区 ID 和 Shuffle 访问信息。
    private void startTrackingPartitions(
            final ResourceID taskExecutorId,
            final Collection<ResultPartitionDeploymentDescriptor> partitions) {
        JobMasterPartitionTracker partitionTracker =
                vertex.getExecutionGraphAccessor().getPartitionTracker();
        for (ResultPartitionDeploymentDescriptor partition : partitions) {
            partitionTracker.startTrackingPartition(taskExecutorId, partition);
        }
    }

    void handlePartitionCleanup(
            boolean releasePipelinedPartitions, boolean releaseBlockingPartitions) {
        if (releasePipelinedPartitions) {
            sendReleaseIntermediateResultPartitionsRpcCall();
        }

        final Collection<ResultPartitionID> partitionIds = getPartitionIds();
        final JobMasterPartitionTracker partitionTracker =
                getVertex().getExecutionGraphAccessor().getPartitionTracker();

        if (!partitionIds.isEmpty()) {
            if (releaseBlockingPartitions) {
                LOG.info("Discarding the results produced by task execution {}.", attemptId);
                partitionTracker.stopTrackingAndReleasePartitions(partitionIds);
            } else {
                partitionTracker.stopTrackingPartitions(partitionIds);
            }
        }
    }

    private Collection<ResultPartitionID> getPartitionIds() {
        return producedPartitions.values().stream()
                .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                .map(ShuffleDescriptor::getResultPartitionID)
                .collect(Collectors.toList());
    }

    private void sendReleaseIntermediateResultPartitionsRpcCall() {
        LOG.info("Discarding the results produced by task execution {}.", attemptId);
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            final ShuffleMaster<?> shuffleMaster =
                    getVertex().getExecutionGraphAccessor().getShuffleMaster();

            Set<ResultPartitionID> partitionIds =
                    producedPartitions.values().stream()
                            .filter(
                                    resultPartitionDeploymentDescriptor ->
                                            resultPartitionDeploymentDescriptor
                                                    .getPartitionType()
                                                    .isReleaseByUpstream())
                            .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                            .peek(shuffleMaster::releasePartitionExternally)
                            .map(ShuffleDescriptor::getResultPartitionID)
                            .collect(Collectors.toSet());

            if (!partitionIds.isEmpty()) {
                // TODO For some tests this could be a problem when querying too early if all
                // resources were released
                taskManagerGateway.releasePartitions(getVertex().getJobId(), partitionIds);
            }
        }
    }

    /**
     * Update the partition infos on the assigned resource.
     *
     * @param partitionInfos for the remote task
     */
    private void sendUpdatePartitionInfoRpcCall(final Iterable<PartitionInfo> partitionInfos) {

        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
            final TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();

            CompletableFuture<Acknowledge> updatePartitionsResultFuture =
                    taskManagerGateway.updatePartitions(attemptId, partitionInfos, rpcTimeout);

            updatePartitionsResultFuture.whenCompleteAsync(
                    (ack, failure) -> {
                        // fail if there was a failure
                        if (failure != null) {
                            fail(
                                    new IllegalStateException(
                                            "Update to task ["
                                                    + getVertexWithAttempt()
                                                    + "] on TaskManager "
                                                    + taskManagerLocation
                                                    + " failed",
                                            failure));
                        }
                    },
                    getVertex().getExecutionGraphAccessor().getJobMasterMainThreadExecutor());
        }
    }

    /**
     * Releases the assigned resource and completes the release future once the assigned resource
     * has been successfully released.
     *
     * @param cause for the resource release, null if none
     */
    private void releaseAssignedResource(@Nullable Throwable cause) {

        assertRunningInJobMasterMainThread();

        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                    getVertex().getExecutionGraphAccessor().getJobMasterMainThreadExecutor();

            slot.releaseSlot(cause)
                    .whenComplete(
                            (Object ignored, Throwable throwable) -> {
                                jobMasterMainThreadExecutor.assertRunningInMainThread();
                                if (throwable != null) {
                                    releaseFuture.completeExceptionally(throwable);
                                } else {
                                    releaseFuture.complete(null);
                                }
                            });
        } else {
            // no assigned resource --> we can directly complete the release future
            releaseFuture.complete(null);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Miscellaneous
    // --------------------------------------------------------------------------------------------

    public void transitionState(ExecutionState targetState) {
        transitionState(state, targetState);
    }
    // 状态转移
    private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
        return transitionState(currentState, targetState, null);
    }
    // 状态转移
    private boolean transitionState(
            ExecutionState currentState, ExecutionState targetState, Throwable error) {
        // sanity check
        if (currentState.isTerminal()) {
            throw new IllegalStateException(
                    "Cannot leave terminal state "
                            + currentState
                            + " to transition to "
                            + targetState
                            + '.');
        }

        if (state == currentState) {
            state = targetState;
            markTimestamp(currentState, targetState);

            if (error == null) {
                LOG.info(
                        "{} ({}) switched from {} to {}.",
                        getVertex().getTaskNameWithSubtaskIndex(),
                        getAttemptId(),
                        currentState,
                        targetState);
            } else if (LOG.isInfoEnabled()) {
                LOG.info(
                        "{} ({}) switched from {} to {} on {}.",
                        getVertex().getTaskNameWithSubtaskIndex(),
                        getAttemptId(),
                        currentState,
                        targetState,
                        getLocationInformation(),
                        ExceptionUtils.stripCompletionException(error));
            }

            if (targetState == INITIALIZING || targetState == RUNNING) {
                initializingOrRunningFuture.complete(null);
            } else if (targetState.isTerminal()) {
                // complete the terminal state future
                terminalStateFuture.complete(targetState);
            }

            // make sure that the state transition completes normally.
            // potential errors (in listeners may not affect the main logic)
            try {
                vertex.notifyStateTransition(this, currentState, targetState);
            } catch (Throwable t) {
                LOG.error(
                        "Error while notifying execution graph of execution state transition.", t);
            }
            return true;
        } else {
            return false;
        }
    }

    private String getLocationInformation() {
        if (assignedResource != null) {
            return assignedResource.getTaskManagerLocation().toString();
        } else {
            return "[unassigned resource]";
        }
    }
    //记录状态的开始和结束时间
    private void markTimestamp(ExecutionState currentState, ExecutionState targetState) {
        long now = System.currentTimeMillis();
        markTimestamp(targetState, now);
        markEndTimestamp(currentState, now);
    }

    private void markTimestamp(ExecutionState state, long timestamp) {
        this.stateTimestamps[state.ordinal()] = timestamp;
    }

    private void markEndTimestamp(ExecutionState state, long timestamp) {
        this.stateEndTimestamps[state.ordinal()] = timestamp;
    }

    public String getVertexWithAttempt() {
        return vertex.getTaskNameWithSubtaskIndex() + " - execution #" + getAttemptNumber();
    }

    // ------------------------------------------------------------------------
    //  Accumulators
    // ------------------------------------------------------------------------

    /**
     * Update accumulators (discarded when the Execution has already been terminated).
     *
     * @param userAccumulators the user accumulators
     */
    public void setAccumulators(Map<String, Accumulator<?, ?>> userAccumulators) {
        synchronized (accumulatorLock) {
            if (!state.isTerminal()) {
                this.userAccumulators = userAccumulators;
            }
        }
    }

    public Map<String, Accumulator<?, ?>> getUserAccumulators() {
        return userAccumulators;
    }

    @Override
    public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
        Map<String, OptionalFailure<Accumulator<?, ?>>> accumulators =
                userAccumulators == null
                        ? null
                        : userAccumulators.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                entry -> OptionalFailure.of(entry.getValue())));
        return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulators);
    }

    @Override
    public int getParallelSubtaskIndex() {
        return getVertex().getParallelSubtaskIndex();
    }

    @Override
    public IOMetrics getIOMetrics() {
        return ioMetrics;
    }

    private void updateAccumulatorsAndMetrics(
            Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {
        if (userAccumulators != null) {
            synchronized (accumulatorLock) {
                this.userAccumulators = userAccumulators;
            }
        }
        if (metrics != null) {
            // Drop IOMetrics#resultPartitionBytes because it will not be used anymore. It can
            // result in very high memory usage when there are many executions and sub-partitions.
            this.ioMetrics =
                    new IOMetrics(
                            metrics.getNumBytesIn(),
                            metrics.getNumBytesOut(),
                            metrics.getNumRecordsIn(),
                            metrics.getNumRecordsOut(),
                            metrics.getAccumulateIdleTime(),
                            metrics.getAccumulateBusyTime(),
                            metrics.getAccumulateBackPressuredTime());
        }
    }

    // ------------------------------------------------------------------------
    //  Standard utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final LogicalSlot slot = assignedResource;

        return String.format(
                "Attempt #%d (%s) @ %s - [%s]",
                getAttemptNumber(),
                vertex.getTaskNameWithSubtaskIndex(),
                (slot == null ? "(unassigned)" : slot),
                state);
    }

    @Override
    public ArchivedExecution archive() {
        return new ArchivedExecution(this);
    }

    private void assertRunningInJobMasterMainThread() {
        vertex.getExecutionGraphAccessor()
                .getJobMasterMainThreadExecutor()
                .assertRunningInMainThread();
    }
}
