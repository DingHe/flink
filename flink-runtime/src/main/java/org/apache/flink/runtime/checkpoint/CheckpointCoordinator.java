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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.FinishedTaskStateProvider.PartialFinishingNotSupportedByStateException;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.FULL_CHECKPOINT;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state. It
 * triggers the checkpoint by sending the messages to the relevant tasks and collects the checkpoint
 * acknowledgements. It also collects and maintains the overview of the state handles reported by
 * the tasks that acknowledge the checkpoint.
 */
// CheckpointCoordinator 是 Flink JobMaster 上的核心组件，负责管理和协调整个分布式应用的状态快照（检查点和保存点）的生命周期。
// 触发和调度： 根据配置（时间间隔、数据积压状态等）或外部请求，触发新的检查点。
// 协调和追踪： 向所有参与任务发送检查点障碍物 (Barrier)，并追踪哪些任务已确认 (Acknowledge) 和哪些任务未完成。
// 状态收集和存储： 收集所有任务报告的算子状态句柄，并将其写入 CompletedCheckpointStore 和 CheckpointStorage
// 是 Flink 可靠性机制的大脑，负责将分散在集群中的任务状态同步、持久化并管理起来，以实现容错和状态恢复。
public class CheckpointCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    /** The number of recent checkpoints whose IDs are remembered. */
    private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

    // ------------------------------------------------------------------------

    /** Coordinator-wide lock to safeguard the checkpoint updates.*/
    // 协调器级锁。
    // 用于同步访问所有关键的、可变的状态，特别是对 pendingCheckpoints 的操作，以确保线程安全。
    private final Object lock = new Object();

    /** The job whose checkpoint this coordinator coordinates. */
    // 作业 ID。
    // 当前 Job 的唯一标识符。
    private final JobID job;

    /** Default checkpoint properties. */
    // 默认检查点属性。
    // 封装了如保留策略 (RETAIN_ON_FAILURE 等) 等检查点配置。
    private final CheckpointProperties checkpointProperties;

    /** The executor used for asynchronous calls, like potentially blocking I/O. */
    // 异步执行器。
    // 用于执行可能阻塞的 I/O 操作（如存储检查点），避免阻塞 JobMaster 的主线程。
    private final Executor executor;
    // 检查点清理器。
    // 负责异步清理已完成或失败的检查点。
    private final CheckpointsCleaner checkpointsCleaner;

    /** The operator coordinators that need to be checkpointed. */
    // 要检查点的算子协调器。
    // 存储需要参与检查点流程的所有 OperatorCoordinator 的上下文，包括 Source 协调器等。
    private final Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint;

    /** Map from checkpoint ID to the pending checkpoint. */
    // 待处理的检查点映射。
    // 存储所有当前正在进行中、等待任务确认的检查点。Key 是检查点 ID，Value 是 PendingCheckpoint 对象。
    @GuardedBy("lock")
    private final Map<Long, PendingCheckpoint> pendingCheckpoints;

    /**
     * Completed checkpoints. Implementations can be blocking. Make sure calls to methods accessing
     * this don't block the job manager actor and run asynchronously.
     */
    // 已完成检查点存储。
    // 外部存储接口，用于持久化已成功完成的检查点元数据。
    // 侧重点是元数据管理 (Metadata Management)
    // 仅存储已完成检查点的元数据（CompletedCheckpoint 对象，包含 ID、状态句柄引用等）。
    // 通常驻留在 高可用服务 (HA) 中，如 ZooKeeper、JobManager 的内存或外部持久化服务（如 Flink 的 FileSystemCompletedCheckpointStore）。
    private final CompletedCheckpointStore completedCheckpointStore;

    /**
     * The root checkpoint state backend, which is responsible for initializing the checkpoint,
     * storing the metadata, and cleaning up the checkpoint.
     */
    // 检查点存储视图。
    // 负责与底层的 CheckpointStorage 交互，包括初始化位置、存储元数据和清理。
    // 侧重点是实际数据 I/O 与位置管理 (Data I/O & Location)
    // 负责创建和管理检查点数据文件和元数据文件的存储位置。
    // 负责与 底层存储系统 (如 HDFS, S3, 本地文件系统) 交互。
    private final CheckpointStorageCoordinatorView checkpointStorageView;

    /** A list of recent expired checkpoint IDs, to identify late messages (vs invalid ones). */
    // 最近过期 ID 队列。
    // 存储最近被清除或超时的检查点 ID，用于在接收到延迟的 ACK 或 Decline 消息时，判断消息是否针对一个已知的过期检查点，而非完全无效的 ID。
    private final ArrayDeque<Long> recentExpiredCheckpoints;

    /**
     * Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these need to
     * be ascending across job managers.
     */
    // 检查点 ID 计数器。
    // 负责生成严格递增且全局唯一的检查点 ID，在 JobMaster 故障恢复时也需要保证 ID 的连续性。
    private final CheckpointIDCounter checkpointIdCounter;

    /**
     * The checkpoint interval when there is no source reporting isProcessingBacklog=true. Actual
     * trigger time may be affected by the max concurrent checkpoints, minimum-pause values and
     * checkpoint interval during backlog.
     */
    // 基础检查点间隔。
    // 当没有任务报告数据积压时，定时检查点使用的触发间隔（毫秒）。
    private final long baseInterval;

    /**
     * The checkpoint interval when any source reports isProcessingBacklog=true. Actual trigger time
     * may be affected by the max concurrent checkpoints and minimum-pause values.
     */
    // 积压数据时的检查点间隔。
    // 当有任务报告正在处理积压数据时，使用的触发间隔（毫秒）
    private final long baseIntervalDuringBacklog;

    /** The max time (in ms) that a checkpoint may take.*/
    // 检查点最大超时时间。
    // 单个检查点从触发到完成的最大允许时间。
    private final long checkpointTimeout;

    /**
     * The min time(in ms) to delay after a checkpoint could be triggered. Allows to enforce minimum
     * processing time between checkpoint attempts
     */
    // 检查点最小暂停时间。
    // 两个连续检查点触发之间必须等待的最短时间间隔，用于防止系统过载。
    private final long minPauseBetweenCheckpoints;

    /**
     * The timer that handles the checkpoint timeouts and triggers periodic checkpoints. It must be
     * single-threaded. Eventually it will be replaced by main thread executor.
     */
    // 定时器执行器。
    // 单线程定时器，用于调度周期性检查点触发和超时检查。
    private final ScheduledExecutor timer;

    /** The master checkpoint hooks executed by this checkpoint coordinator. */
    // 主触发恢复钩子。
    // 允许外部组件在检查点/恢复过程中执行自定义逻辑（如清理、状态处理）。
    private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;
    // 非对齐检查点启用标志。
    // 标记是否启用了非对齐检查点。
    private final boolean unalignedCheckpointsEnabled;
    // 对齐检查点超时时间。
    // 启用了非对齐检查点时，允许任务在转换为非对齐之前等待对齐的最长时间。
    private final long alignedCheckpointTimeout;

    /** Actor that receives status updates from the execution graph this coordinator works for. */
    // 作业状态监听器，用于接收来自执行图（Execution Graph）的状态更新
    private JobStatusListener jobStatusListener;

    /**
     * The current periodic trigger. Used to deduplicate concurrently scheduled checkpoints if any.
     */
    // 当前的周期触发器。
    // 封装了下一个周期性检查点的触发逻辑。
    @GuardedBy("lock")
    private ScheduledTrigger currentPeriodicTrigger;

    /** A handle to the current periodic trigger, to cancel it when necessary. */
    // 当前触发器 Future。
    // timer 调度的未来任务句柄，用于取消或管理当前的周期性触发任务。
    @GuardedBy("lock")
    private ScheduledFuture<?> currentPeriodicTriggerFuture;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the next checkpoint will be
     * triggered.
     * <p>If it's value is {@link Long#MAX_VALUE}, it means there is not a next checkpoint
     * scheduled.
     */
    // 下一次触发相对时间。
    // 下一个检查点计划触发的相对时间戳（基于 Clock），如果为 Long.MAX_VALUE 则表示没有调度下一个检查点。
    @GuardedBy("lock")
    private long nextCheckpointTriggeringRelativeTime;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the last checkpoint completed.
     */
    // 上一次完成时间。
    // 上一个检查点完成的相对时间戳。
    private long lastCheckpointCompletionRelativeTime;

    /**
     * Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
     * Non-volatile, because only accessed in synchronized scope
     */
    // 周期性调度标志。
    // 标记是否应该在当前检查点完成后立即调度下一个检查点。
    private boolean periodicScheduling;

    /** Flag marking the coordinator as shut down (not accepting any messages any more). */
    // 标记协调器是否已关闭，关闭后不再接受任何消息或操作
    private volatile boolean shutdown;

    /** Optional tracker for checkpoint statistics. */
    // 检查点统计追踪器。
    // 用于收集、聚合和报告检查点的性能和状态统计信息。
    private final CheckpointStatsTracker statsTracker;
    // 顶点完成状态检查器工厂。
    // 用于创建 VertexFinishedStateChecker，用于验证部分完成任务在状态类型上的兼容性（例如，是否使用了 UnionListState）。
    private final BiFunction<
                    Set<ExecutionJobVertex>,
                    Map<OperatorID, OperatorState>,
                    VertexFinishedStateChecker>
            vertexFinishedStateCheckerFactory;

    /** Id of checkpoint for which in-flight data should be ignored on recovery. */
    // 忽略在途数据检查点 ID。
    // 仅用于恢复场景，如果 Job 从某个特殊的检查点恢复，则该检查点之后的在途数据会被忽略。
    private final long checkpointIdOfIgnoredInFlightData;
    // 检查点失败管理器。
    // 负责管理检查点失败后的行为，例如触发重试。
    private final CheckpointFailureManager failureManager;
    //提供当前时间的时钟接口，通常用于获取相对时间
    private final Clock clock;
    // 精确一次模式标志。
    // 标记当前 Job 是否以精确一次 (Exactly-Once) 语义运行。
    private final boolean isExactlyOnceMode;

    /** Flag represents there is an in-flight trigger request.*/
    // 正在触发请求标志。
    // 标记当前是否正在处理一个检查点触发请求，用于防止并发触发。
    private boolean isTriggering = false;
    // 检查点请求决策器。
    // 封装了检查点触发的决策逻辑，根据并发限制和最小暂停时间来决定是否允许触发新的检查点。
    private final CheckpointRequestDecider requestDecider;
    // 检查点计划计算器。
    // 负责根据执行图的当前状态，计算下一个检查点的参与任务列表。
    private final CheckpointPlanCalculator checkpointPlanCalculator;

    /** IDs of the source operators that are currently processing backlog. */
    // 积压数据算子 ID 集合。
    // 存储当前正在处理积压数据（Backlog）的 Source 算子 ID。这会影响检查点的触发间隔。
    @GuardedBy("lock")
    private final Set<OperatorID> backlogOperators = new HashSet<>();
    // 基础位置初始化标志。
    // 标记检查点存储的基础位置是否已成功初始化。
    private boolean baseLocationsForCheckpointInitialized = false;
    // 强制全量快照标志。
    // 如果为 true，则检查点将强制执行全量快照，而非增量快照。
    private boolean forceFullSnapshot;

    // --------------------------------------------------------------------------------------------

    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            CheckpointStatsTracker statsTracker) {

        this(
                job,
                chkConfig,
                coordinatorsToCheckpoint,
                checkpointIDCounter,
                completedCheckpointStore,
                checkpointStorage,
                executor,
                checkpointsCleaner,
                timer,
                failureManager,
                checkpointPlanCalculator,
                SystemClock.getInstance(),
                statsTracker,
                VertexFinishedStateChecker::new);
    }

    @VisibleForTesting
    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint, //需要进行检查点的操作符协调器的集合。每个操作符可能会有一个专门的协调器来处理它的检查点
            CheckpointIDCounter checkpointIDCounter, //检查点协调器的配置对象，包含所有与检查点相关的配置，如检查点间隔、超时等
            CompletedCheckpointStore completedCheckpointStore, //已完成的检查点存储，用于存储成功完成的检查点信息
            CheckpointStorage checkpointStorage, //检查点存储对象，负责存储检查点数据
            Executor executor, //用于异步执行任务的线程池，通常用于执行可能会阻塞的 I/O 操作。
            CheckpointsCleaner checkpointsCleaner, //负责清理已完成的检查点，避免过期的检查点占用存储空间。
            ScheduledExecutor timer, //定时器，用于调度定时任务（例如触发检查点）。此定时器需要是单线程的
            CheckpointFailureManager failureManager, //检查点失败管理器，用于管理检查点失败的情况，如重试机制、清理等。
            CheckpointPlanCalculator checkpointPlanCalculator, //计算检查点计划的工具，负责根据当前的作业状态计算哪些操作符需要在检查点过程中执行
            Clock clock,
            CheckpointStatsTracker statsTracker, //统计信息跟踪器，用于收集和报告检查点的统计信息（如成功率、超时等）
            BiFunction<
                            Set<ExecutionJobVertex>,
                            Map<OperatorID, OperatorState>,
                            VertexFinishedStateChecker>
                    vertexFinishedStateCheckerFactory) {

        // sanity checks
        checkNotNull(checkpointStorage);
        //这里定义了一个最大值限制，确保检查点之间的最小暂停时间不能超过一年（以毫秒为单位）。这样做是为了防止因为极长的时间间隔导致数值溢出
        // max "in between duration" can be one year - this is to prevent numeric overflows
        long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
        if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
            minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
        }

        // it does not make sense to schedule checkpoints more often then the desired
        // time between checkpoints
        long baseInterval = chkConfig.getCheckpointInterval();
        if (baseInterval < minPauseBetweenCheckpoints) {
            baseInterval = minPauseBetweenCheckpoints;
        }

        this.job = checkNotNull(job);
        this.baseInterval = baseInterval;
        this.baseIntervalDuringBacklog = chkConfig.getCheckpointIntervalDuringBacklog();
        this.nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
        this.checkpointTimeout = chkConfig.getCheckpointTimeout();
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.coordinatorsToCheckpoint =
                Collections.unmodifiableCollection(coordinatorsToCheckpoint);
        this.pendingCheckpoints = new LinkedHashMap<>();
        this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
        this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
        this.executor = checkNotNull(executor);
        this.checkpointsCleaner = checkNotNull(checkpointsCleaner);
        this.failureManager = checkNotNull(failureManager);
        this.checkpointPlanCalculator = checkNotNull(checkpointPlanCalculator);
        this.clock = checkNotNull(clock);
        this.isExactlyOnceMode = chkConfig.isExactlyOnce();
        this.unalignedCheckpointsEnabled = chkConfig.isUnalignedCheckpointsEnabled();
        this.alignedCheckpointTimeout = chkConfig.getAlignedCheckpointTimeout();
        this.checkpointIdOfIgnoredInFlightData = chkConfig.getCheckpointIdOfIgnoredInFlightData();

        this.recentExpiredCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
        this.masterHooks = new HashMap<>();

        this.timer = timer;

        this.checkpointProperties =
                CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy());

        try {
            this.checkpointStorageView = checkpointStorage.createCheckpointStorage(job);

            if (isPeriodicCheckpointingConfigured()) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
                baseLocationsForCheckpointInitialized = true;
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Failed to create checkpoint storage at checkpoint coordinator side.", e);
        }

        try {
            // Make sure the checkpoint ID enumerator is running. Possibly
            // issues a blocking call to ZooKeeper. 启动检查点 ID 计数器，确保检查点 ID 从正确的地方开始
            checkpointIDCounter.start();
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Failed to start checkpoint ID counter: " + t.getMessage(), t);
        }  //CheckpointRequestDecider 用于决定是否可以触发新的检查点请求，它根据最大并发检查点数、当前正在进行的检查点数、检查点清理等因素来进行决策
        this.requestDecider =
                new CheckpointRequestDecider(
                        chkConfig.getMaxConcurrentCheckpoints(),
                        this::rescheduleTrigger,
                        this.clock,
                        this.minPauseBetweenCheckpoints,
                        this.pendingCheckpoints::size,
                        this.checkpointsCleaner::getNumberOfCheckpointsToClean);
        this.statsTracker = checkNotNull(statsTracker, "Statistic tracker can not be null");
        this.vertexFinishedStateCheckerFactory = checkNotNull(vertexFinishedStateCheckerFactory);
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration
    // --------------------------------------------------------------------------------------------

    /**
     * Adds the given master hook to the checkpoint coordinator. This method does nothing, if the
     * checkpoint coordinator already contained a hook with the same ID (as defined via {@link
     * MasterTriggerRestoreHook#getIdentifier()}).
     *
     * @param hook The hook to add.
     * @return True, if the hook was added, false if the checkpoint coordinator already contained a
     *     hook with the same ID.
     */
    public boolean addMasterHook(MasterTriggerRestoreHook<?> hook) {
        checkNotNull(hook);

        final String id = hook.getIdentifier();
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(id), "The hook has a null or empty id");

        synchronized (lock) {
            if (!masterHooks.containsKey(id)) {
                masterHooks.put(id, hook);
                return true;
            } else {
                return false;
            }
        }
    }

    /** Gets the number of currently register master hooks. */
    public int getNumberOfRegisteredMasterHooks() {
        synchronized (lock) {
            return masterHooks.size();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Clean shutdown
    // --------------------------------------------------------------------------------------------

    /**
     * Shuts down the checkpoint coordinator.
     *
     * <p>After this method has been called, the coordinator does not accept and further messages
     * and cannot trigger any further checkpoints.
     */
    public void shutdown() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
                LOG.info("Stopping checkpoint coordinator for job {}.", job);

                periodicScheduling = false;

                // shut down the hooks
                MasterHooks.close(masterHooks.values(), LOG);
                masterHooks.clear();

                final CheckpointException reason =
                        new CheckpointException(
                                CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
                // clear queued requests and in-flight checkpoints
                abortPendingAndQueuedCheckpoints(reason);
            }
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Reports whether a source operator is currently processing backlog.
     *
     * <p>If any source operator is processing backlog, the checkpoint interval would be decided by
     * {@code execution.checkpointing.interval-during-backlog} instead of {@code
     * execution.checkpointing.interval}.
     *
     * <p>If a source has not invoked this method, the source is considered to have
     * isProcessingBacklog=false. If a source operator has invoked this method multiple times, the
     * last reported value is used.
     *
     * @param operatorID the operator ID of the source operator.
     * @param isProcessingBacklog whether the source operator is processing backlog.
     */

    // 用于动态调整检查点（Checkpoint）的触发间隔，以应对 Source 算子（Source Operator）是否正在处理积压数据（Backlog）的情况。
    // operatorID (OperatorID): 传入的参数，表示报告当前状态的 Source 算子的唯一标识符。
    // isProcessingBacklog (boolean): 传入的参数，表示该 Source 算子当前是否正在处理积压数据（如果为 true 则表示正在处理）。
    public void setIsProcessingBacklog(OperatorID operatorID, boolean isProcessingBacklog) {
        synchronized (lock) {
            if (isProcessingBacklog) {
                backlogOperators.add(operatorID);
            } else {
                backlogOperators.remove(operatorID);
            }
            // 获取当前生效的检查点间隔
            // 根据 backlogOperators 集合的状态来决定使用哪个配置值：
            long currentCheckpointInterval = getCurrentCheckpointInterval();
            // 检查检查点功能是否被禁用。
            if (currentCheckpointInterval
                    != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                long currentRelativeTime = clock.relativeTimeMillis();
                // 如果现在就按照新的间隔立即设置下一次触发时间，新的触发时间点。
                // 新的触发时间点（新的、更短的间隔）早于当前计划的旧触发时间点时，条件才成立。
                // 这通常发生在检查点间隔从长变短时（例如，从正常间隔切换到积压间隔）
                if (currentRelativeTime + currentCheckpointInterval
                        < nextCheckpointTriggeringRelativeTime) {
                    // 重调度检查点触发器
                    rescheduleTrigger(currentRelativeTime, currentCheckpointInterval);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Triggering Checkpoints and Savepoints
    // --------------------------------------------------------------------------------------------

    /**
     * Triggers a savepoint with the given savepoint directory as a target.
     *
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSavepoint(
            @Nullable final String targetLocation, final SavepointFormatType formatType) {
        final CheckpointProperties properties =
                CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled, formatType);
        return triggerSavepointInternal(properties, targetLocation);
    }

    /**
     * Triggers a synchronous savepoint with the given savepoint directory as a target.
     *
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
            final boolean terminate,
            @Nullable final String targetLocation,
            SavepointFormatType formatType) {

        final CheckpointProperties properties =
                CheckpointProperties.forSyncSavepoint(
                        !unalignedCheckpointsEnabled, terminate, formatType);

        return triggerSavepointInternal(properties, targetLocation);
    }

    private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
            final CheckpointProperties checkpointProperties,
            @Nullable final String targetLocation) {

        checkNotNull(checkpointProperties);

        return triggerCheckpointFromCheckpointThread(checkpointProperties, targetLocation, false);
    }
    // 这个方法是实际触发检查点的核心逻辑的包装器，它确保了检查点触发操作在一个专用的线程（通常是 CheckpointCoordinator 的 定时器线程）中执行，以维护线程安全和操作的顺序性。
    // 主要目的是将检查点触发的操作提交到 timer 执行器上执行，并将执行结果通过一个 CompletableFuture 返回给调用者。
    // 1. checkpointProperties: 检查点的属性，如是否是保存点 (isSavepoint)、是否外部持久化等。
    // 2. targetLocation: 可选的目标存储路径。如果非空，通常用于用户指定的保存点路径。
    // 3. isPeriodic: 指示此次触发是否为周期性检查点。
    private CompletableFuture<CompletedCheckpoint> triggerCheckpointFromCheckpointThread(
            CheckpointProperties checkpointProperties, String targetLocation, boolean isPeriodic) {
        // TODO, call triggerCheckpoint directly after removing timer thread
        // for now, execute the trigger in timer thread to avoid competition
        final CompletableFuture<CompletedCheckpoint> resultFuture = new CompletableFuture<>();
        timer.execute(
                () ->
                        triggerCheckpoint(checkpointProperties, targetLocation, isPeriodic)
                                .whenComplete(
                                        (completedCheckpoint, throwable) -> {
                                            if (throwable == null) {
                                                resultFuture.complete(completedCheckpoint);
                                            } else {
                                                resultFuture.completeExceptionally(throwable);
                                            }
                                        }));
        return resultFuture;
    }

    /**
     * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint timestamp.
     * The return value is a future. It completes when the checkpoint triggered finishes or an error
     * occurred.
     *
     * @param isPeriodic Flag indicating whether this triggered checkpoint is periodic.
     * @return a future to the completed checkpoint.
     */
    // 核心作用是作为外部调用（比如定时器、用户请求）触发标准检查点的入口，并将工作转发给一个更通用的内部方法。
    // 承诺返回一个已完成的检查点对象，允许调用者异步等待结果。
    // 参数 isPeriodic： 一个布尔标志，指示此次触发是否为周期性的检查点（例如，由配置的间隔时间触发），而不是用户或故障恢复等原因触发的单次检查点。
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
        return triggerCheckpointFromCheckpointThread(checkpointProperties, null, isPeriodic);
    }

    /**
     * Triggers one new checkpoint with the given checkpointType. The returned future completes when
     * the triggered checkpoint finishes or an error occurred.
     *
     * @param checkpointType specifies the backup type of the checkpoint to trigger.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {

        if (checkpointType == null) {
            throw new IllegalArgumentException("checkpointType cannot be null");
        }

        final SnapshotType snapshotType;
        switch (checkpointType) {
            case CONFIGURED:
                snapshotType = checkpointProperties.getCheckpointType();
                break;
            case FULL:
                snapshotType = FULL_CHECKPOINT;
                break;
            case INCREMENTAL:
                snapshotType = CHECKPOINT;
                break;
            default:
                throw new IllegalArgumentException("unknown checkpointType: " + checkpointType);
        }

        final CheckpointProperties properties =
                new CheckpointProperties(
                        checkpointProperties.forceCheckpoint(),
                        snapshotType,
                        checkpointProperties.discardOnSubsumed(),
                        checkpointProperties.discardOnJobFinished(),
                        checkpointProperties.discardOnJobCancelled(),
                        checkpointProperties.discardOnJobFailed(),
                        checkpointProperties.discardOnJobSuspended(),
                        checkpointProperties.isUnclaimed());
        return triggerCheckpointFromCheckpointThread(properties, null, false);
    }
    // 用于**触发检查点（或保存点）**的核心方法
    // 将检查点触发逻辑分为三个步骤：封装请求、选择执行请求、返回结果承诺。
    @VisibleForTesting
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean isPeriodic) {

        CheckpointTriggerRequest request =
                new CheckpointTriggerRequest(props, externalSavepointLocation, isPeriodic);
        chooseRequestToExecute(request).ifPresent(this::startTriggeringCheckpoint);
        return request.onCompletionPromise;
    }
    // 负责启动检查点或保存点触发流程的关键方法
    private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
        try {
            synchronized (lock) {
                // 确保在触发检查点时 JobMaster 处于合法状态（例如，作业正在运行），并且如果请求是周期性检查点，
                // 它会检查是否违反了最小检查点间隔等约束。如果预检查失败，将抛出异常。
                preCheckGlobalState(request.isPeriodic);
            }

            // we will actually trigger this checkpoint!
            // 断言当前没有其他检查点正在被触发
            Preconditions.checkState(!isTriggering);
            isTriggering = true;

            final long timestamp = System.currentTimeMillis();
            // 计算本次检查点需要涉及的所有任务（Source、Operator、Sink 等）
            CompletableFuture<CheckpointPlan> checkpointPlanFuture =
                    checkpointPlanCalculator.calculateCheckpointPlan();

            boolean initializeBaseLocations = !baseLocationsForCheckpointInitialized;
            baseLocationsForCheckpointInitialized = true;

            CompletableFuture<Void> masterTriggerCompletionPromise = new CompletableFuture<>();
            // 创建 PendingCheckpoint 和分配 ID
            final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture =
                    checkpointPlanFuture
                            .thenApplyAsync(
                                    plan -> {
                                        try {
                                            // this must happen outside the coordinator-wide lock,
                                            // because it communicates with external services
                                            // (in HA mode) and may block for a while.
                                            long checkpointID =
                                                    checkpointIdCounter.getAndIncrement();
                                            return new Tuple2<>(plan, checkpointID);
                                        } catch (Throwable e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenApplyAsync(
                                    (checkpointInfo) ->
                                            createPendingCheckpoint(
                                                    timestamp,
                                                    request.props,
                                                    checkpointInfo.f0,
                                                    request.isPeriodic,
                                                    checkpointInfo.f1,
                                                    request.getOnCompletionFuture(),
                                                    masterTriggerCompletionPromise),
                                    timer);
            // 初始化存储位置和协调者状态 (Master/Coordinator State)
            final CompletableFuture<?> coordinatorCheckpointsComplete =
                    pendingCheckpointCompletableFuture
                            .thenApplyAsync(
                                    pendingCheckpoint -> {
                                        try {
                                            CheckpointStorageLocation checkpointStorageLocation =
                                                    initializeCheckpointLocation(
                                                            pendingCheckpoint.getCheckpointID(),
                                                            request.props,
                                                            request.externalSavepointLocation,
                                                            initializeBaseLocations);
                                            return Tuple2.of(
                                                    pendingCheckpoint, checkpointStorageLocation);
                                        } catch (Throwable e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenComposeAsync(
                                    (checkpointInfo) -> {
                                        PendingCheckpoint pendingCheckpoint = checkpointInfo.f0;
                                        if (pendingCheckpoint.isDisposed()) {
                                            // The disposed checkpoint will be handled later,
                                            // skip snapshotting the coordinator states.
                                            return null;
                                        }
                                        synchronized (lock) {
                                            pendingCheckpoint.setCheckpointTargetLocation(
                                                    checkpointInfo.f1);
                                        }
                                        // 触发并等待所有 OperatorCoordinator（如 Source Coordinator）的状态快照完成
                                        return OperatorCoordinatorCheckpoints
                                                .triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                                                        coordinatorsToCheckpoint,
                                                        pendingCheckpoint,
                                                        timer);
                                    },
                                    timer);

            // We have to take the snapshot of the master hooks after the coordinator checkpoints
            // has completed.
            // This is to ensure the tasks are checkpointed after the OperatorCoordinators in case
            // ExternallyInducedSource is used.
            final CompletableFuture<?> masterStatesComplete =
                    coordinatorCheckpointsComplete.thenComposeAsync(
                            ignored -> {
                                // If the code reaches here, the pending checkpoint is guaranteed to
                                // be not null.
                                // We use FutureUtils.getWithoutException() to make compiler happy
                                // with checked
                                // exceptions in the signature.
                                PendingCheckpoint checkpoint =
                                        FutureUtils.getWithoutException(
                                                pendingCheckpointCompletableFuture);
                                if (checkpoint == null || checkpoint.isDisposed()) {
                                    // The disposed checkpoint will be handled later,
                                    // skip snapshotting the master states.
                                    return null;
                                }
                                // 触发对JobMaster 自身的 Master Hook 状态（例如，一些框架级的状态）进行快照。
                                // 顺序非常重要： 先协调者状态，后 Master 状态，以保证数据流的正确性（特别是对 ExternallyInducedSource）。
                                return snapshotMasterState(checkpoint);
                            },
                            timer);
            // 创建一个 Future，表示协调者状态 (coordinatorCheckpointsComplete) 和 Master 状态 (masterStatesComplete) 两者都已成功完成。
            FutureUtils.forward(
                    CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete),
                    masterTriggerCompletionPromise);

            FutureUtils.assertNoException(
                    masterTriggerCompletionPromise
                            .handleAsync(
                                    (ignored, throwable) -> {
                                        final PendingCheckpoint checkpoint =
                                                FutureUtils.getWithoutException(
                                                        pendingCheckpointCompletableFuture);

                                        Preconditions.checkState(
                                                checkpoint != null || throwable != null,
                                                "Either the pending checkpoint needs to be created or an error must have occurred.");

                                        if (throwable != null) {
                                            // the initialization might not be finished yet
                                            if (checkpoint == null) {
                                                onTriggerFailure(request, throwable);
                                            } else {
                                                onTriggerFailure(checkpoint, throwable);
                                            }
                                        } else {
                                            // 如果所有协调者端的准备工作（计划、ID、定位、Master/Coordinator 状态快照）都成功完成，则调用 triggerCheckpointRequest。
                                            // 这是真正向所有 Source TaskManager 发送检查点屏障（Checkpoint Barrier）的调用，标志着分布式检查点过程开始。
                                            triggerCheckpointRequest(
                                                    request, timestamp, checkpoint);
                                        }
                                        return null;
                                    },
                                    timer)
                            .exceptionally(
                                    error -> {
                                        if (!isShutdown()) {
                                            throw new CompletionException(error);
                                        } else if (findThrowable(
                                                        error, RejectedExecutionException.class)
                                                .isPresent()) {
                                            LOG.debug("Execution rejected during shutdown");
                                        } else {
                                            LOG.warn("Error encountered during shutdown", error);
                                        }
                                        return null;
                                    }));
        } catch (Throwable throwable) {
            onTriggerFailure(request, throwable);
        }
    }
    // 在协调者（Master）端准备工作完成后，正式向 Task 发起检查点请求
    // request: 原始的检查点触发请求对象，包含属性和完成 Future。
    // timestamp: 检查点开始触发的时间戳。
    // checkpoint: 已经创建好的 PendingCheckpoint 对象，代表本次正在进行的检查点实例。
    private void triggerCheckpointRequest(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        // 检查 PendingCheckpoint 是否在协调者准备期间（例如，计算计划、定位存储或快照 Coordinator 状态时）已经被取消或丢弃。
        if (checkpoint.isDisposed()) {
            onTriggerFailure(
                    checkpoint,
                    new CheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
                            checkpoint.getFailureCause()));
        } else {
            // 向任务发送检查点屏障 (Trigger Tasks)
            triggerTasks(request, timestamp, checkpoint)
                    .exceptionally(
                            failure -> {
                                try (MdcUtils.MdcCloseable ignored =
                                        MdcUtils.withContext(MdcUtils.asContextData(job))) {
                                    LOG.info(
                                            "Triggering Checkpoint {} for job {} failed due to {}",
                                            checkpoint.getCheckpointID(),
                                            job,
                                            failure);
                                    final CheckpointException cause;
                                    if (failure instanceof CheckpointException) {
                                        cause = (CheckpointException) failure;
                                    } else {
                                        cause =
                                                new CheckpointException(
                                                        CheckpointFailureReason
                                                                .TRIGGER_CHECKPOINT_FAILURE,
                                                        failure);
                                    }
                                    timer.execute(
                                            () -> {
                                                synchronized (lock) {
                                                    abortPendingCheckpoint(checkpoint, cause);
                                                }
                                            });
                                    return null;
                                }
                            });

            // It is possible that the tasks has finished
            // checkpointing at this point.
            // So we need to complete this pending checkpoint.
            // 完成检查点 (Complete Checkpoint)
            if (maybeCompleteCheckpoint(checkpoint)) {
                onTriggerSuccess();
            }
        }
    }
    // 负责向 Source Task 发送检查点屏障以启动分布式快照的核心方法
    // 将协调者端的准备工作转化为对实际任务的远程调用，以启动数据快照。
    private CompletableFuture<Void> triggerTasks(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        // no exception, no discarding, everything is OK
        final long checkpointId = checkpoint.getCheckpointID();

        final SnapshotType type;
        // 检查是否配置了强制全量快照 (forceFullSnapshot) 且本次请求不是保存点 (!isSavepoint())。
        if (this.forceFullSnapshot && !request.props.isSavepoint()) {
            type = FULL_CHECKPOINT;
        } else {
            type = request.props.getCheckpointType();
        }
        // 创建一个 CheckpointOptions 对象。
        // 这个对象将与检查点屏障一起发送给任务，指导任务如何执行快照操作。
        final CheckpointOptions checkpointOptions =
                CheckpointOptions.forConfig(
                        type,
                        checkpoint.getCheckpointStorageLocation().getLocationReference(),
                        isExactlyOnceMode,
                        unalignedCheckpointsEnabled,
                        alignedCheckpointTimeout);

        // send messages to the tasks to trigger their checkpoints
        // 初始化一个列表，用于存储所有任务触发请求返回的 Future。
        // 每个 Future 在任务成功接收到触发消息并开始处理时完成。
        List<CompletableFuture<Acknowledge>> acks = new ArrayList<>();
        // 遍历检查点计划中所有需要发送屏障的任务（通常是 Source 任务）
        for (Execution execution : checkpoint.getCheckpointPlan().getTasksToTrigger()) {
            // 检查本次请求是否是同步保存点
            if (request.props.isSynchronous()) {
                acks.add(
                        execution.triggerSynchronousSavepoint(
                                checkpointId, timestamp, checkpointOptions));
            } else {
                // 如果是普通检查点（异步），则调用标准的 execution.triggerCheckpoint(...) 方法。
                acks.add(execution.triggerCheckpoint(checkpointId, timestamp, checkpointOptions));
            }
        }
        // 返回等待所有任务触发完成的 Future
        return FutureUtils.waitForAll(acks);
    }

    /**
     * Initialize the checkpoint location asynchronously. It will be expected to be executed in io
     * thread due to it might be time-consuming.
     *
     * @param checkpointID checkpoint id
     * @param props checkpoint properties
     * @param externalSavepointLocation the external savepoint location, it might be null
     * @return the checkpoint location
     */
    private CheckpointStorageLocation initializeCheckpointLocation(
            long checkpointID,
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean initializeBaseLocations)
            throws Exception {
        final CheckpointStorageLocation checkpointStorageLocation;
        if (props.isSavepoint()) {
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForSavepoint(
                            checkpointID, externalSavepointLocation);
        } else {
            if (initializeBaseLocations) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
            }
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForCheckpoint(checkpointID);
        }

        return checkpointStorageLocation;
    }
    // 用于实例化和注册一个新的 PendingCheckpoint 对象的关键方法。
    private PendingCheckpoint createPendingCheckpoint(
            long timestamp,
            CheckpointProperties props,
            CheckpointPlan checkpointPlan,
            boolean isPeriodic,
            long checkpointID,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            CompletableFuture<Void> masterTriggerCompletionPromise) {

        synchronized (lock) {
            try {
                // since we haven't created the PendingCheckpoint yet, we need to check the
                // global state here.
                preCheckGlobalState(isPeriodic);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }

        PendingCheckpointStats pendingCheckpointStats =
                trackPendingCheckpointStats(checkpointID, checkpointPlan, props, timestamp);

        final PendingCheckpoint checkpoint =
                new PendingCheckpoint(
                        job,
                        checkpointID,
                        timestamp,
                        checkpointPlan,
                        OperatorInfo.getIds(coordinatorsToCheckpoint),
                        masterHooks.keySet(),
                        props,
                        onCompletionPromise,
                        pendingCheckpointStats,
                        masterTriggerCompletionPromise);

        synchronized (lock) {
            pendingCheckpoints.put(checkpointID, checkpoint);

            ScheduledFuture<?> cancellerHandle =
                    timer.schedule(
                            new CheckpointCanceller(checkpoint),
                            checkpointTimeout,
                            TimeUnit.MILLISECONDS);

            if (!checkpoint.setCancellerHandle(cancellerHandle)) {
                // checkpoint is already disposed!
                cancellerHandle.cancel(false);
            }
        }

        LOG.info(
                "Triggering checkpoint {} (type={}) @ {} for job {}.",
                checkpointID,
                checkpoint.getProps().getCheckpointType(),
                timestamp,
                job);
        return checkpoint;
    }

    /**
     * Snapshot master hook states asynchronously.
     *
     * @param checkpoint the pending checkpoint
     * @return the future represents master hook states are finished or not
     */
    private CompletableFuture<Void> snapshotMasterState(PendingCheckpoint checkpoint) {
        if (masterHooks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final long checkpointID = checkpoint.getCheckpointID();
        final long timestamp = checkpoint.getCheckpointTimestamp();

        final CompletableFuture<Void> masterStateCompletableFuture = new CompletableFuture<>();
        for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
            MasterHooks.triggerHook(masterHook, checkpointID, timestamp, executor)
                    .whenCompleteAsync(
                            (masterState, throwable) -> {
                                try {
                                    synchronized (lock) {
                                        if (masterStateCompletableFuture.isDone()) {
                                            return;
                                        }
                                        if (checkpoint.isDisposed()) {
                                            throw new IllegalStateException(
                                                    "Checkpoint "
                                                            + checkpointID
                                                            + " has been discarded");
                                        }
                                        if (throwable == null) {
                                            checkpoint.acknowledgeMasterState(
                                                    masterHook.getIdentifier(), masterState);
                                            if (checkpoint.areMasterStatesFullyAcknowledged()) {
                                                masterStateCompletableFuture.complete(null);
                                            }
                                        } else {
                                            masterStateCompletableFuture.completeExceptionally(
                                                    throwable);
                                        }
                                    }
                                } catch (Throwable t) {
                                    masterStateCompletableFuture.completeExceptionally(t);
                                }
                            },
                            timer);
        }
        return masterStateCompletableFuture;
    }

    /** Trigger request is successful. NOTE, it must be invoked if trigger request is successful. */
    private void onTriggerSuccess() {
        isTriggering = false;
        // 调度下一个请求
        // 如果在当前检查点触发期间，有其他周期性检查点或手动保存点请求被拦截并排队，
        // 此方法将尝试将队列中的第一个请求取出并启动其触发流程（调用 startTriggeringCheckpoint）。
        executeQueuedRequest();
    }

    /**
     * The trigger request is failed prematurely without a proper initialization. There is no
     * resource to release, but the completion promise needs to fail manually here.
     *
     * @param onCompletionPromise the completion promise of the checkpoint/savepoint
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            CheckpointTriggerRequest onCompletionPromise, Throwable throwable) {
        final CheckpointException checkpointException =
                getCheckpointException(
                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
        onCompletionPromise.completeExceptionally(checkpointException);
        onTriggerFailure((PendingCheckpoint) null, onCompletionPromise.props, checkpointException);
    }

    private void onTriggerFailure(PendingCheckpoint checkpoint, Throwable throwable) {
        checkArgument(checkpoint != null, "Pending checkpoint can not be null.");

        onTriggerFailure(checkpoint, checkpoint.getProps(), throwable);
    }

    /**
     * The trigger request is failed. NOTE, it must be invoked if trigger request is failed.
     *
     * @param checkpoint the pending checkpoint which is failed. It could be null if it's failed
     *     prematurely without a proper initialization.
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            @Nullable PendingCheckpoint checkpoint,
            CheckpointProperties checkpointProperties,
            Throwable throwable) {
        try {
            // beautify the stack trace a bit
            throwable = ExceptionUtils.stripCompletionException(throwable);

            coordinatorsToCheckpoint.forEach(
                    OperatorCoordinatorCheckpointContext::abortCurrentTriggering);

            final CheckpointException cause =
                    getCheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);

            if (checkpoint != null && !checkpoint.isDisposed()) {
                synchronized (lock) {
                    abortPendingCheckpoint(checkpoint, cause);
                }
            } else {
                failureManager.handleCheckpointException(
                        checkpoint, checkpointProperties, cause, null, job, null, statsTracker);
            }
        } catch (Throwable secondThrowable) {
            secondThrowable.addSuppressed(throwable);
            throw secondThrowable;

        } finally {
            isTriggering = false;
            executeQueuedRequest();
        }
    }

    private void executeQueuedRequest() {
        chooseQueuedRequestToExecute().ifPresent(this::startTriggeringCheckpoint);
    }

    private Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute() {
        synchronized (lock) {
            return requestDecider.chooseQueuedRequestToExecute(
                    isTriggering, lastCheckpointCompletionRelativeTime);
        }
    }
    // 检查点请求决策
    private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            CheckpointTriggerRequest request) {
        synchronized (lock) {
            Optional<CheckpointTriggerRequest> checkpointTriggerRequest =
                    requestDecider.chooseRequestToExecute(
                            request, isTriggering, lastCheckpointCompletionRelativeTime);
            return checkpointTriggerRequest;
        }
    }

    // Returns true if the checkpoint is successfully completed, false otherwise.
    // 主要在任务（Task）向协调者发送检查点确认（ACK）后，或者在所有屏障发送完成后，被调用以检查检查点是否已准备好进入完成阶段。
    private boolean maybeCompleteCheckpoint(PendingCheckpoint checkpoint) {
        // 在 Flink 中，所有对检查点状态（包括 PendingCheckpoint 列表）的修改都必须在持有这个锁的情况下进行，以确保线程安全。
        synchronized (lock) {
            // 检查当前 PendingCheckpoint 是否已获得所有任务的确认（ACK）。
            if (checkpoint.isFullyAcknowledged()) {
                try {
                    // we need to check inside the lock for being shutdown as well,
                    // otherwise we get races and invalid error log messages.
                    if (shutdown) {
                        return false;
                    }
                    // 将 CompletedCheckpoint 写入 JobManager 的状态后端（如果配置了）；更新最新完成的检查点 ID；并通知等待该结果的 Future。
                    completePendingCheckpoint(checkpoint);
                } catch (CheckpointException ce) {
                    onTriggerFailure(checkpoint, ce);
                    return false;
                }
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------
    //  Handling checkpoints and messages
    // --------------------------------------------------------------------------------------------

    /**
     * Receives a {@link DeclineCheckpoint} message for a pending checkpoint.
     *
     * @param message Checkpoint decline from the task manager
     * @param taskManagerLocationInfo The location info of the decline checkpoint message's sender
     */
    public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
        if (shutdown || message == null) {
            return;
        }

        if (!job.equals(message.getJob())) {
            throw new IllegalArgumentException(
                    "Received DeclineCheckpoint message for job "
                            + message.getJob()
                            + " from "
                            + taskManagerLocationInfo
                            + " while this coordinator handles job "
                            + job);
        }

        final long checkpointId = message.getCheckpointId();
        final CheckpointException checkpointException =
                message.getSerializedCheckpointException().unwrap();
        final String reason = checkpointException.getMessage();

        PendingCheckpoint checkpoint;

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return;
            }

            checkpoint = pendingCheckpoints.get(checkpointId);

            if (checkpoint != null) {
                Preconditions.checkState(
                        !checkpoint.isDisposed(),
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
                LOG.info(
                        "Decline checkpoint {} by task {} of job {} at {}.",
                        checkpointId,
                        message.getTaskExecutionId(),
                        job,
                        taskManagerLocationInfo,
                        checkpointException.getCause());
                abortPendingCheckpoint(
                        checkpoint, checkpointException, message.getTaskExecutionId());
            } else if (LOG.isDebugEnabled()) {
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    // message is for an expired checkpoint
                    LOG.debug(
                            "Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                } else {
                    // message is for an unknown checkpoint. might be so old that we don't even
                    // remember it any more
                    LOG.debug(
                            "Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                }
            }
        }
    }

    /**
     * Receives an AcknowledgeCheckpoint message and returns whether the message was associated with
     * a pending checkpoint.
     *
     * @param message Checkpoint ack from the task manager
     * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
     * @return Flag indicating whether the ack'd checkpoint was associated with a pending
     *     checkpoint.
     * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint
     *     store.
     */
    public boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message, String taskManagerLocationInfo)
            throws CheckpointException {
        if (shutdown || message == null) {
            return false;
        }

        if (!job.equals(message.getJob())) {
            LOG.error(
                    "Received wrong AcknowledgeCheckpoint message for job {} from {} : {}",
                    job,
                    taskManagerLocationInfo,
                    message);
            return false;
        }

        final long checkpointId = message.getCheckpointId();

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return false;
            }

            final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

            if (message.getSubtaskState() != null) {
                // Register shared state regardless of checkpoint state and task ACK state.
                // This way, shared state is
                // 1. kept if the message is late or state will be used by the task otherwise
                // 2. removed eventually upon checkpoint subsumption (or job cancellation)
                // Do not register savepoints' shared state, as Flink is not in charge of
                // savepoints' lifecycle
                if (checkpoint == null || !checkpoint.getProps().isSavepoint()) {
                    message.getSubtaskState()
                            .registerSharedStates(
                                    completedCheckpointStore.getSharedStateRegistry(),
                                    checkpointId);
                }
            }

            if (checkpoint != null && !checkpoint.isDisposed()) {

                switch (checkpoint.acknowledgeTask(
                        message.getTaskExecutionId(),
                        message.getSubtaskState(),
                        message.getCheckpointMetrics())) {
                    case SUCCESS:
                        LOG.debug(
                                "Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                                checkpointId,
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        if (checkpoint.isFullyAcknowledged()) {
                            completePendingCheckpoint(checkpoint);
                        }
                        break;
                    case DUPLICATE:
                        LOG.debug(
                                "Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        break;
                    case UNKNOWN:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the task's execution attempt id was unknown. Discarding "
                                        + "the state handle to avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());

                        break;
                    case DISCARDED:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the pending checkpoint had been discarded. Discarding the "
                                        + "state handle tp avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());
                }

                return true;
            } else if (checkpoint != null) {
                // this should not happen
                throw new IllegalStateException(
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
            } else {
                reportCheckpointMetrics(
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getCheckpointMetrics());
                boolean wasPendingCheckpoint;

                // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    wasPendingCheckpoint = true;
                    LOG.warn(
                            "Received late message for now expired checkpoint attempt {} from task "
                                    + "{} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                } else {
                    LOG.debug(
                            "Received message for an unknown checkpoint {} from task {} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                    wasPendingCheckpoint = false;
                }

                // try to discard the state so that we don't have lingering state lying around
                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());

                return wasPendingCheckpoint;
            }
        }
    }

    /**
     * Try to complete the given pending checkpoint.
     *
     * <p>Important: This method should only be called in the checkpoint lock scope.
     *
     * @param pendingCheckpoint to complete
     * @throws CheckpointException if the completion failed
     */
    // 用于正式完成一个已确认的检查点 PendingCheckpoint
    // 接收一个 PendingCheckpoint 对象作为参数，表示这个检查点已收到所有任务的确认，可以进入完成阶段。
    private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        final long checkpointId = pendingCheckpoint.getCheckpointID();
        final CompletedCheckpoint completedCheckpoint;
        final CompletedCheckpoint lastSubsumed;
        final CheckpointProperties props = pendingCheckpoint.getProps();
        // 通知共享状态注册中心 (SharedStateRegistry)，本次检查点 ID 已成功完成。
        completedCheckpointStore.getSharedStateRegistry().checkpointCompleted(checkpointId);

        try {
            // 将 PendingCheckpoint 中收集到的所有状态句柄和元数据整合，创建一个不可变的 CompletedCheckpoint 对象
            completedCheckpoint = finalizeCheckpoint(pendingCheckpoint);

            // the pending checkpoint must be discarded after the finalization
            Preconditions.checkState(pendingCheckpoint.isDisposed() && completedCheckpoint != null);

            if (!props.isSavepoint()) {
                // 将新完成的 completedCheckpoint 写入 CompletedCheckpointStore（JobManager 持久化存储），并根据保留策略（如配置的最大检查点数量）淘汰（subsume）最旧的检查点
                lastSubsumed =
                        addCompletedCheckpointToStoreAndSubsumeOldest(
                                checkpointId, completedCheckpoint, pendingCheckpoint);
            } else {
                lastSubsumed = null;
            }

            pendingCheckpoint.getCompletionFuture().complete(completedCheckpoint);
            // 向外部系统（如度量系统、HA 存储或日志）报告本次检查点的成功信息。
            reportCompletedCheckpoint(completedCheckpoint);
        } catch (Exception exception) {
            // For robustness reasons, we need catch exception and try marking the checkpoint
            // completed.
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            throw exception;
        } finally {
            // 将本次检查点从 CheckpointCoordinator 维护的正在进行的检查点列表中移除，因为其生命周期已经结束。
            pendingCheckpoints.remove(checkpointId);
            // 重新调度周期性检查点触发器。由于本次检查点已完成，协调者可以尝试立即触发下一个周期性检查点（如果配置了最小间隔，则会等待到期）。
            scheduleTriggerRequest();
        }
        // 事后清理工作。
        // 清理被淘汰的旧检查点 (lastSubsumed) 的外部文件/句柄，释放资源，并清理与本次检查点相关的其他临时资源。
        cleanupAfterCompletedCheckpoint(
                pendingCheckpoint, checkpointId, completedCheckpoint, lastSubsumed, props);
    }

    private void reportCompletedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        failureManager.handleCheckpointSuccess(completedCheckpoint.getCheckpointID());
        CompletedCheckpointStats completedCheckpointStats = completedCheckpoint.getStatistic();
        if (completedCheckpointStats != null) {
            LOG.trace(
                    "Checkpoint {} size: {}Kb, duration: {}ms",
                    completedCheckpoint.getCheckpointID(),
                    completedCheckpointStats.getStateSize() == 0
                            ? 0
                            : completedCheckpointStats.getStateSize() / 1024,
                    completedCheckpointStats.getEndToEndDuration());
            // Finalize the statsCallback and give the completed checkpoint a
            // callback for discards.
            statsTracker.reportCompletedCheckpoint(completedCheckpointStats);
        }
    }

    private void cleanupAfterCompletedCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            CompletedCheckpoint lastSubsumed,
            CheckpointProperties props) {

        // record the time when this was completed, to calculate
        // the 'min delay between checkpoints'
        lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();

        logCheckpointInfo(completedCheckpoint);

        if (!props.isSavepoint() || props.isSynchronous()) {
            // drop those pending checkpoints that are at prior to the completed one
            dropSubsumedCheckpoints(checkpointId);

            // send the "notify complete" call to all vertices, coordinators, etc.
            sendAcknowledgeMessages(
                    pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                    checkpointId,
                    completedCheckpoint.getTimestamp(),
                    extractIdIfDiscardedOnSubsumed(lastSubsumed));
        }
    }

    private void logCheckpointInfo(CompletedCheckpoint completedCheckpoint) {
        LOG.info(
                "Completed checkpoint {} for job {} ({} bytes, checkpointDuration={} ms, finalizationTime={} ms).",
                completedCheckpoint.getCheckpointID(),
                job,
                completedCheckpoint.getStateSize(),
                completedCheckpoint.getCompletionTimestamp() - completedCheckpoint.getTimestamp(),
                System.currentTimeMillis() - completedCheckpoint.getCompletionTimestamp());

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Checkpoint state: ");
            for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
                builder.append(state);
                builder.append(", ");
            }
            // Remove last two chars ", "
            builder.setLength(builder.length() - 2);

            LOG.debug(builder.toString());
        }
    }

    private CompletedCheckpoint finalizeCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        try {
            final CompletedCheckpoint completedCheckpoint =
                    pendingCheckpoint.finalizeCheckpoint(
                            checkpointsCleaner, this::scheduleTriggerRequest, executor);

            return completedCheckpoint;
        } catch (Exception e1) {
            // abort the current pending checkpoint if we fails to finalize the pending
            // checkpoint.
            final CheckpointFailureReason failureReason =
                    e1 instanceof PartialFinishingNotSupportedByStateException
                            ? CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING
                            : CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE;

            if (!pendingCheckpoint.isDisposed()) {
                abortPendingCheckpoint(
                        pendingCheckpoint, new CheckpointException(failureReason, e1));
            }

            throw new CheckpointException(
                    "Could not finalize the pending checkpoint "
                            + pendingCheckpoint.getCheckpointID()
                            + '.',
                    failureReason,
                    e1);
        }
    }

    private long extractIdIfDiscardedOnSubsumed(CompletedCheckpoint lastSubsumed) {
        final long lastSubsumedCheckpointId;
        if (lastSubsumed != null && lastSubsumed.getProperties().discardOnSubsumed()) {
            lastSubsumedCheckpointId = lastSubsumed.getCheckpointID();
        } else {
            lastSubsumedCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
        }
        return lastSubsumedCheckpointId;
    }

    private CompletedCheckpoint addCompletedCheckpointToStoreAndSubsumeOldest(
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        List<ExecutionVertex> tasksToAbort =
                pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo();
        try {
            final CompletedCheckpoint subsumedCheckpoint =
                    completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                            completedCheckpoint, checkpointsCleaner, this::scheduleTriggerRequest);
            // reset the force full snapshot flag, we should've completed at least one full
            // snapshot by now
            this.forceFullSnapshot = false;
            return subsumedCheckpoint;
        } catch (Exception exception) {
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            if (exception instanceof PossibleInconsistentStateException) {
                LOG.warn(
                        "An error occurred while writing checkpoint {} to the underlying metadata"
                                + " store. Flink was not able to determine whether the metadata was"
                                + " successfully persisted. The corresponding state located at '{}'"
                                + " won't be discarded and needs to be cleaned up manually.",
                        completedCheckpoint.getCheckpointID(),
                        completedCheckpoint.getExternalPointer());
            } else {
                // we failed to store the completed checkpoint. Let's clean up
                checkpointsCleaner.cleanCheckpointOnFailedStoring(completedCheckpoint, executor);
            }

            final CheckpointException checkpointException =
                    new CheckpointException(
                            "Could not complete the pending checkpoint " + checkpointId + '.',
                            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE,
                            exception);
            reportFailedCheckpoint(pendingCheckpoint, checkpointException);
            sendAbortedMessages(tasksToAbort, checkpointId, completedCheckpoint.getTimestamp());
            throw checkpointException;
        }
    }

    private void reportFailedCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        failureManager.handleCheckpointException(
                pendingCheckpoint,
                pendingCheckpoint.getProps(),
                exception,
                null,
                job,
                getStatsCallback(pendingCheckpoint),
                statsTracker);
    }

    void scheduleTriggerRequest() {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.debug(
                        "Skip scheduling trigger request because the CheckpointCoordinator is shut down");
            } else {
                timer.execute(this::executeQueuedRequest);
            }
        }
    }

    @VisibleForTesting
    void sendAcknowledgeMessages(
            List<ExecutionVertex> tasksToCommit,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        // commit tasks
        for (ExecutionVertex ev : tasksToCommit) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointOnComplete(
                        completedCheckpointId, completedTimestamp, lastSubsumedCheckpointId);
            }
        }

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointComplete(completedCheckpointId);
        }
    }

    private void sendAbortedMessages(
            List<ExecutionVertex> tasksToAbort, long checkpointId, long timeStamp) {
        assert (Thread.holdsLock(lock));
        long latestCompletedCheckpointId = completedCheckpointStore.getLatestCheckpointId();

        // send notification of aborted checkpoints asynchronously.
        executor.execute(
                () -> {
                    // send the "abort checkpoint" messages to necessary vertices.
                    for (ExecutionVertex ev : tasksToAbort) {
                        Execution ee = ev.getCurrentExecutionAttempt();
                        if (ee != null) {
                            try {
                                ee.notifyCheckpointAborted(
                                        checkpointId, latestCompletedCheckpointId, timeStamp);
                            } catch (Throwable e) {
                                LOG.warn(
                                        "Could not send aborted message of checkpoint {} to task {} belonging to job {}.",
                                        checkpointId,
                                        ee.getAttemptId(),
                                        ee.getVertex().getJobId(),
                                        e);
                            }
                        }
                    }
                });

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointAborted(checkpointId);
        }
    }

    private void rememberRecentExpiredCheckpointId(long id) {
        if (recentExpiredCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
            recentExpiredCheckpoints.removeFirst();
        }
        recentExpiredCheckpoints.addLast(id);
    }

    private void dropSubsumedCheckpoints(long checkpointId) {
        abortPendingCheckpoints(
                checkpoint ->
                        checkpoint.getCheckpointID() < checkpointId && checkpoint.canBeSubsumed(),
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_SUBSUMED));
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpoint State Restoring
    // --------------------------------------------------------------------------------------------

    /**
     * Restores the latest checkpointed state to a set of subtasks. This method represents a "local"
     * or "regional" failover and does restore states to coordinators. Note that a regional failover
     * might still include all tasks.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return An {@code OptionalLong} with the checkpoint ID, if state was restored, an empty
     *     {@code OptionalLong} otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    // 作用是将最近一次成功的检查点状态恢复到一个特定的任务子集（通常是一个故障区域）。它代表了一种“局部”或“区域性”的故障恢复机制。
    // 从最近一次完成的检查点或保存点中，将状态加载并映射到传入的 tasks 集合中指定的任务实例。
    // tasks: 需要恢复状态并重新启动的 ExecutionJobVertex 集合。这些任务通常是导致故障或受到故障影响的区域中的一部分。
    public OptionalLong restoreLatestCheckpointedStateToSubtasks(
            final Set<ExecutionJobVertex> tasks) throws Exception {
        // when restoring subtasks only we accept potentially unmatched state for the
        // following reasons
        //   - the set frequently does not include all Job Vertices (only the ones that are part
        //     of the restarted region), meaning there will be unmatched state by design.
        //   - because what we might end up restoring from an original savepoint with unmatched
        //     state, if there is was no checkpoint yet.
        return restoreLatestCheckpointedStateInternal(
                tasks, // 明确指定了需要恢复状态的 JobVertex 集合。
                // 对于局部/区域性恢复，我们只关心数据流任务的状态，不重置或恢复 OperatorCoordinator 的状态。
                OperatorCoordinatorRestoreBehavior
                        .SKIP, // local/regional recovery does not reset coordinators
                // 表示即使找不到可用的检查点或保存点状态，也不要抛出异常。任务将以无状态方式启动。这是因为区域恢复可能在作业第一次检查点成功前发生。
                false, // recovery might come before first successful checkpoint
                // 表示如果检查点中包含未映射到当前 tasks 集合的状态，也允许恢复继续
                true,
                // 表示在本次恢复中，不严格检查状态对应的最大并行度是否发生了变化。
                false); // see explanation above
    }

    /**
     * Restores the latest checkpointed state to all tasks and all coordinators. This method
     * represents a "global restore"-style operation where all stateful tasks and coordinators from
     * the given set of Job Vertices are restored. are restored to their latest checkpointed state.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param allowNonRestoredState Allow checkpoint state that cannot be mapped to any job vertex
     *     in tasks.
     * @return <code>true</code> if state was restored, <code>false</code> otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public boolean restoreLatestCheckpointedStateToAll(
            final Set<ExecutionJobVertex> tasks, final boolean allowNonRestoredState)
            throws Exception {

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior
                                .RESTORE_OR_RESET, // global recovery restores coordinators, or
                        // resets them to empty
                        false, // recovery might come before first successful checkpoint
                        allowNonRestoredState,
                        false);

        return restoredCheckpointId.isPresent();
    }

    /**
     * Restores the latest checkpointed at the beginning of the job execution. If there is a
     * checkpoint, this method acts like a "global restore"-style operation where all stateful tasks
     * and coordinators from the given set of Job Vertices are restored.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return True, if a checkpoint was found and its state was restored, false otherwise.
     */
    public boolean restoreInitialCheckpointIfPresent(final Set<ExecutionJobVertex> tasks)
            throws Exception {
        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        false, // initial checkpoints exist only on JobManager failover. ok if not
                        // present.
                        false,
                        true); // JobManager failover means JobGraphs match exactly.

        return restoredCheckpointId.isPresent();
    }

    /**
     * Performs the actual restore operation to the given tasks.
     *
     * <p>This method returns the restored checkpoint ID (as an optional) or an empty optional, if
     * no checkpoint was restored.
     */
    // 负责从最近一次成功的检查点中获取状态，将其分配给需要重启的任务，并处理 JobMaster 和 OperatorCoordinator 的状态恢复。
    // tasks: 需要恢复状态的 JobVertex 集合。
    // operatorCoordinatorRestoreBehavior: 协调者状态的恢复策略（跳过、重置或恢复）。
    // errorIfNoCheckpoint: 如果没有检查点状态，是否抛出异常。
    // allowNonRestoredState: 是否允许检查点中存在未分配给当前 tasks 的状态。
    // checkForPartiallyFinishedOperators: 是否检查那些已标记为“已完成”的 Operator 是否存在不该有的状态。
    private OptionalLong restoreLatestCheckpointedStateInternal(
            final Set<ExecutionJobVertex> tasks,
            final OperatorCoordinatorRestoreBehavior operatorCoordinatorRestoreBehavior,
            final boolean errorIfNoCheckpoint,
            final boolean allowNonRestoredState,
            final boolean checkForPartiallyFinishedOperators)
            throws Exception {
        // 初始化和前置检查
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("CheckpointCoordinator is shut down");
            }
            long restoreTimestamp = SystemClock.getInstance().absoluteTimeMillis();
            statsTracker.reportInitializationStarted(
                    tasks.stream()
                            .map(ExecutionJobVertex::getTaskVertices)
                            .flatMap(Stream::of)
                            .map(ExecutionVertex::getCurrentExecutionAttempt)
                            .map(Execution::getAttemptId)
                            .collect(Collectors.toSet()),
                    restoreTimestamp);

            // Restore from the latest checkpoint
            // 查找最近检查点与无检查点处理
            CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint();
            // 无可用检查点
            if (latest == null) {
                LOG.info("No checkpoint found during restore.");

                if (errorIfNoCheckpoint) {
                    throw new IllegalStateException("No completed checkpoint available");
                }
                // 重置 JobMaster 注册的所有 Master Hook。这些 Hook（用于 JobMaster 存储状态）会被告知本次重启是无状态重启。
                LOG.debug("Resetting the master hooks.");
                MasterHooks.reset(masterHooks.values(), LOG);
                // 如果协调者恢复策略是 RESTORE_OR_RESET（即在无状态时重置），则调用 restoreStateToCoordinators
                if (operatorCoordinatorRestoreBehavior
                        == OperatorCoordinatorRestoreBehavior.RESTORE_OR_RESET) {
                    // we let the JobManager-side components know that there was a recovery,
                    // even if there was no checkpoint to recover from, yet
                    LOG.info("Resetting the Operator Coordinators to an empty state.");
                    restoreStateToCoordinators(
                            OperatorCoordinator.NO_CHECKPOINT, Collections.emptyMap());
                }

                return OptionalLong.empty();
            }
            // 向统计跟踪器报告本次恢复所使用的检查点信息。
            statsTracker.reportRestoredCheckpoint(
                    latest.getCheckpointID(),
                    latest.getProperties(),
                    latest.getExternalPointer(),
                    latest.getStateSize());

            LOG.info("Restoring job {} from {}.", job, latest);
            // 如果本次恢复使用的检查点是一个未被声明的 Savepoint，那么在下一次检查点触发时，
            // 需要强制进行全量快照 (forceFullSnapshot = true)，以确保状态链正确。
            this.forceFullSnapshot = latest.getProperties().isUnclaimed();

            // re-assign the task states
            // 从 CompletedCheckpoint 中提取所有 Operator 的状态信息，按 OperatorID 映射。
            final Map<OperatorID, OperatorState> operatorStates = extractOperatorStates(latest);
            // 如果调用方要求检查已完成 Operator 的状态：
            if (checkForPartiallyFinishedOperators) {
                VertexFinishedStateChecker vertexFinishedStateChecker =
                        vertexFinishedStateCheckerFactory.apply(tasks, operatorStates);
                vertexFinishedStateChecker.validateOperatorsFinishedState();
            }
            // 分配状态给任务 (Task State Assignment)
            // 负责将检查点中的状态数据结构，映射和分割给 Job 图中的具体任务实例。
            // 接收检查点 ID、目标任务 (tasks)、提取的 Operator 状态 (operatorStates) 和是否允许不匹配状态的标志 (allowNonRestoredState)
            StateAssignmentOperation stateAssignmentOperation =
                    new StateAssignmentOperation(
                            latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState);
            // 执行状态分配逻辑。
            stateAssignmentOperation.assignStates();

            // call master hooks for restore. we currently call them also on "regional restore"
            // because
            // there is no other failure notification mechanism in the master hooks
            // ultimately these should get removed anyways in favor of the operator coordinators
            // 调用所有注册的 JobMaster Hook 来恢复它们的状态。
            MasterHooks.restoreMasterHooks(
                    masterHooks,
                    latest.getMasterHookStates(),
                    latest.getCheckpointID(),
                    allowNonRestoredState,
                    LOG);
            // 如果协调者恢复策略不是跳过
            if (operatorCoordinatorRestoreBehavior != OperatorCoordinatorRestoreBehavior.SKIP) {
                // 调用方法恢复所有 OperatorCoordinator 的状态。
                restoreStateToCoordinators(latest.getCheckpointID(), operatorStates);
            }

            return OptionalLong.of(latest.getCheckpointID());
        }
    }
    // Flink CheckpointCoordinator 中用于从已完成检查点中提取 Operator 状态的方法。
    // 核心职责是处理一个特殊情况：从启用了非对齐检查点（Unaligned Checkpoints）的检查点恢复时，需要移除检查点中包含的传输中数据（In-Flight Data）状态。
    private Map<OperatorID, OperatorState> extractOperatorStates(CompletedCheckpoint checkpoint) {
        Map<OperatorID, OperatorState> originalOperatorStates = checkpoint.getOperatorStates();
        // 检查当前要恢复的检查点 ID (checkpoint.getCheckpointID()) 是否不等于协调器内部存储的一个特殊 ID (checkpointIdOfIgnoredInFlightData)
        if (checkpoint.getCheckpointID() != checkpointIdOfIgnoredInFlightData) {
            // Don't do any changes if it is not required.
            return originalOperatorStates;
        }

        HashMap<OperatorID, OperatorState> newStates = new HashMap<>();
        // Create the new operator states without in-flight data.
        // 移除传输中数据状态（仅针对非对齐恢复）
        for (OperatorState originalOperatorState : originalOperatorStates.values()) {
            newStates.put(
                    originalOperatorState.getOperatorID(),
                    originalOperatorState.copyAndDiscardInFlightData());
        }

        return newStates;
    }

    /**
     * Restore the state with given savepoint.
     *
     * @param restoreSettings Settings for a snapshot to restore from. Includes the path and
     *     parameters for the restore process.
     * @param tasks Map of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param userClassLoader The class loader to resolve serialized classes in legacy savepoint
     *     versions.
     */
    public boolean restoreSavepoint(
            SavepointRestoreSettings restoreSettings,
            Map<JobVertexID, ExecutionJobVertex> tasks,
            ClassLoader userClassLoader)
            throws Exception {

        final String savepointPointer = restoreSettings.getRestorePath();
        final boolean allowNonRestored = restoreSettings.allowNonRestoredState();
        Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

        LOG.info(
                "Starting job {} from savepoint {} ({})",
                job,
                savepointPointer,
                (allowNonRestored ? "allowing non restored state" : ""));

        final CompletedCheckpointStorageLocation checkpointLocation =
                checkpointStorageView.resolveCheckpoint(savepointPointer);

        // convert to checkpoint so the system can fall back to it
        final CheckpointProperties checkpointProperties;
        switch (restoreSettings.getRecoveryClaimMode()) {
            case CLAIM:
                checkpointProperties = this.checkpointProperties;
                break;
            case LEGACY:
                checkpointProperties =
                        CheckpointProperties.forSavepoint(
                                false,
                                // we do not care about the format when restoring, the format is
                                // necessary when triggering a savepoint
                                SavepointFormatType.CANONICAL);
                break;
            case NO_CLAIM:
                checkpointProperties = CheckpointProperties.forUnclaimedSnapshot();
                break;
            default:
                throw new IllegalArgumentException("Unknown snapshot claim mode");
        }

        // Load the savepoint as a checkpoint into the system
        CompletedCheckpoint savepoint =
                Checkpoints.loadAndValidateCheckpoint(
                        job,
                        tasks,
                        checkpointLocation,
                        userClassLoader,
                        allowNonRestored,
                        checkpointProperties);

        // register shared state - even before adding the checkpoint to the store
        // because the latter might trigger subsumption so the ref counts must be up-to-date
        savepoint.registerSharedStatesAfterRestored(
                completedCheckpointStore.getSharedStateRegistry(),
                restoreSettings.getRecoveryClaimMode());

        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                savepoint, checkpointsCleaner, this::scheduleTriggerRequest);

        // Reset the checkpoint ID counter
        long nextCheckpointId = savepoint.getCheckpointID() + 1;
        checkpointIdCounter.setCount(nextCheckpointId);

        LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        new HashSet<>(tasks.values()),
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        true,
                        allowNonRestored,
                        true);

        return restoredCheckpointId.isPresent();
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    public int getNumberOfPendingCheckpoints() {
        synchronized (lock) {
            return this.pendingCheckpoints.size();
        }
    }

    public int getNumberOfRetainedSuccessfulCheckpoints() {
        synchronized (lock) {
            return completedCheckpointStore.getNumberOfRetainedCheckpoints();
        }
    }

    public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
        synchronized (lock) {
            return new HashMap<>(this.pendingCheckpoints);
        }
    }

    public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
        synchronized (lock) {
            return completedCheckpointStore.getAllCheckpoints();
        }
    }

    @VisibleForTesting
    public ArrayDeque<Long> getRecentExpiredCheckpoints() {
        return recentExpiredCheckpoints;
    }

    public CheckpointStorageCoordinatorView getCheckpointStorage() {
        return checkpointStorageView;
    }

    public CompletedCheckpointStore getCheckpointStore() {
        return completedCheckpointStore;
    }

    /**
     * Gets the checkpoint interval. Its value might vary depending on whether there is processing
     * backlog.
     */
    private long getCurrentCheckpointInterval() {
        return backlogOperators.isEmpty() ? baseInterval : baseIntervalDuringBacklog;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    /** @deprecated use {@link #getNumQueuedRequests()} */
    @Deprecated
    @VisibleForTesting
    PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
        synchronized (lock) {
            return requestDecider.getTriggerRequestQueue();
        }
    }

    public boolean isTriggering() {
        return isTriggering;
    }

    @VisibleForTesting
    boolean isCurrentPeriodicTriggerAvailable() {
        return currentPeriodicTrigger != null;
    }

    /**
     * Returns whether periodic checkpointing has been configured.
     *
     * @return <code>true</code> if periodic checkpoints have been configured.
     */
    public boolean isPeriodicCheckpointingConfigured() {
        return baseInterval != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
    }

    // --------------------------------------------------------------------------------------------
    //  Periodic scheduling of checkpoints
    // --------------------------------------------------------------------------------------------

    public void startCheckpointScheduler() {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }
            Preconditions.checkState(
                    isPeriodicCheckpointingConfigured(),
                    "Can not start checkpoint scheduler, if no periodic checkpointing is configured");

            if (isPeriodicCheckpointingStarted()) {
                // cancel previously scheduled checkpoints and spare savepoints.
                // TODO: Introduce a more general solution to the race condition
                //  between different checkpoint scheduling triggers.
                //  https://issues.apache.org/jira/browse/FLINK-34519
                stopCheckpointScheduler();
            }

            periodicScheduling = true;
            scheduleTriggerWithDelay(clock.relativeTimeMillis(), getRandomInitDelay());
        }
    }

    public void stopCheckpointScheduler() {
        synchronized (lock) {
            periodicScheduling = false;

            cancelPeriodicTrigger();

            final CheckpointException reason =
                    new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND);
            abortPendingAndQueuedCheckpoints(reason);
        }
    }

    public boolean isPeriodicCheckpointingStarted() {
        return periodicScheduling;
    }

    /**
     * Aborts all the pending checkpoints due to en exception.
     *
     * @param exception The exception.
     */
    public void abortPendingCheckpoints(CheckpointException exception) {
        synchronized (lock) {
            abortPendingCheckpoints(ignored -> true, exception);
        }
    }

    private void abortPendingCheckpoints(
            Predicate<PendingCheckpoint> checkpointToFailPredicate, CheckpointException exception) {

        assert Thread.holdsLock(lock);

        final PendingCheckpoint[] pendingCheckpointsToFail =
                pendingCheckpoints.values().stream()
                        .filter(checkpointToFailPredicate)
                        .toArray(PendingCheckpoint[]::new);

        // do not traverse pendingCheckpoints directly, because it might be changed during
        // traversing
        for (PendingCheckpoint pendingCheckpoint : pendingCheckpointsToFail) {
            abortPendingCheckpoint(pendingCheckpoint, exception);
        }
    }
    // currentTimeMillis (long): 传入的参数，表示当前的相对时间（通常是 Job 启动以来的毫秒数）。
    // tillNextMillis (long): 传入的参数，表示距离下一次检查点触发应该等待的新间隔/延迟时间（以毫秒为单位）。
    private void rescheduleTrigger(long currentTimeMillis, long tillNextMillis) {
        // 取消当前正在运行的、用于定期触发检查点的计时器或调度任务
        cancelPeriodicTrigger();
        // 安排一个新的检查点触发任务
        scheduleTriggerWithDelay(currentTimeMillis, tillNextMillis);
    }
    // 取消当前正在运行的、用于定期触发检查点的计时器或调度任务
    private void cancelPeriodicTrigger() {
        if (currentPeriodicTrigger != null) {
            nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
            currentPeriodicTriggerFuture.cancel(false);
            currentPeriodicTrigger = null;
            currentPeriodicTriggerFuture = null;
        }
    }

    private long getRandomInitDelay() {
        return ThreadLocalRandom.current().nextLong(minPauseBetweenCheckpoints, baseInterval + 1L);
    }
    // 安排一个新的检查点触发任务
    // currentTimeMillis：通常用于计算新的触发时间点 (currentTimeMillis + tillNextMillis)
    private void scheduleTriggerWithDelay(long currentRelativeTime, long initDelay) {
        nextCheckpointTriggeringRelativeTime = currentRelativeTime + initDelay;
        currentPeriodicTrigger = new ScheduledTrigger();
        currentPeriodicTriggerFuture =
                timer.schedule(currentPeriodicTrigger, initDelay, TimeUnit.MILLISECONDS);
    }
    // 用于将状态恢复给所有的 OperatorCoordinator 实例
    // checkpointId: 本次恢复所使用的检查点 ID。
    // operatorStates: 从检查点中提取出的，按 OperatorID 映射的所有 Operator 的状态 (OperatorState 结构)。
    private void restoreStateToCoordinators(
            final long checkpointId, final Map<OperatorID, OperatorState> operatorStates)
            throws Exception {

        for (OperatorCoordinatorCheckpointContext coordContext : coordinatorsToCheckpoint) {
            // 从 operatorStates 映射表中获取该 Operator 对应的完整状态 (OperatorState)。
            final OperatorState state = operatorStates.get(coordContext.operatorId());
            // 则尝试从其中提取 ByteStreamStateHandle。这是 OperatorCoordinator 自己在检查点时存储的二进制状态句柄。
            final ByteStreamStateHandle coordinatorState =
                    state == null ? null : state.getCoordinatorState();
            // 将协调者状态从句柄中反序列化成原始的字节数组 (byte[])。
            final byte[] bytes = coordinatorState == null ? null : coordinatorState.getData();
            coordContext.resetToCheckpoint(checkpointId, bytes);
        }
    }

    // ------------------------------------------------------------------------
    //  job status listener that schedules / cancels periodic checkpoints
    // ------------------------------------------------------------------------

    public JobStatusListener createActivatorDeactivator(boolean allTasksOutputNonBlocking) {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }

            if (jobStatusListener == null) {
                jobStatusListener =
                        new CheckpointCoordinatorDeActivator(this, allTasksOutputNonBlocking);
            }

            return jobStatusListener;
        }
    }

    int getNumQueuedRequests() {
        synchronized (lock) {
            return requestDecider.getNumQueuedRequests();
        }
    }

    public void reportCheckpointMetrics(
            long id, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {
        statsTracker.reportIncompleteStats(id, attemptId, metrics);
    }

    public void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptID,
            SubTaskInitializationMetrics initializationMetrics) {
        statsTracker.reportInitializationMetrics(executionAttemptID, initializationMetrics);
    }

    // ------------------------------------------------------------------------

    final class ScheduledTrigger implements Runnable {

        @Override
        public void run() {
            synchronized (lock) {
                if (currentPeriodicTrigger != this) {
                    // Another periodic trigger has been scheduled but this one
                    // has not been force cancelled yet.
                    return;
                }

                long checkpointInterval = getCurrentCheckpointInterval();
                if (checkpointInterval
                        != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                    nextCheckpointTriggeringRelativeTime += checkpointInterval;
                    currentPeriodicTriggerFuture =
                            timer.schedule(
                                    this,
                                    Math.max(
                                            0,
                                            nextCheckpointTriggeringRelativeTime
                                                    - clock.relativeTimeMillis()),
                                    TimeUnit.MILLISECONDS);
                } else {
                    nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
                    currentPeriodicTrigger = null;
                    currentPeriodicTriggerFuture = null;
                }
            }

            try {
                triggerCheckpoint(checkpointProperties, null, true);
            } catch (Exception e) {
                LOG.error("Exception while triggering checkpoint for job {}.", job, e);
            }
        }
    }

    /**
     * Discards the given state object asynchronously belonging to the given job, execution attempt
     * id and checkpoint id.
     *
     * @param jobId identifying the job to which the state object belongs
     * @param executionAttemptID identifying the task to which the state object belongs
     * @param checkpointId of the state object
     * @param subtaskState to discard asynchronously
     */
    private void discardSubtaskState(
            final JobID jobId,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final TaskStateSnapshot subtaskState) {

        if (subtaskState != null) {
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {

                            try {
                                subtaskState.discardState();
                            } catch (Throwable t2) {
                                LOG.warn(
                                        "Could not properly discard state object of checkpoint {} "
                                                + "belonging to task {} of job {}.",
                                        checkpointId,
                                        executionAttemptID,
                                        jobId,
                                        t2);
                            }
                        }
                    });
        }
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        abortPendingCheckpoint(pendingCheckpoint, exception, null);
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            CheckpointException exception,
            @Nullable final ExecutionAttemptID executionAttemptID) {

        assert (Thread.holdsLock(lock));

        if (!pendingCheckpoint.isDisposed()) {
            try {
                // release resource here
                pendingCheckpoint.abort(
                        exception.getCheckpointFailureReason(),
                        exception.getCause(),
                        checkpointsCleaner,
                        this::scheduleTriggerRequest,
                        executor,
                        statsTracker);

                failureManager.handleCheckpointException(
                        pendingCheckpoint,
                        pendingCheckpoint.getProps(),
                        exception,
                        executionAttemptID,
                        job,
                        getStatsCallback(pendingCheckpoint),
                        statsTracker);
            } finally {
                sendAbortedMessages(
                        pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                        pendingCheckpoint.getCheckpointID(),
                        pendingCheckpoint.getCheckpointTimestamp());
                pendingCheckpoints.remove(pendingCheckpoint.getCheckpointID());
                if (exception
                        .getCheckpointFailureReason()
                        .equals(CheckpointFailureReason.CHECKPOINT_EXPIRED)) {
                    rememberRecentExpiredCheckpointId(pendingCheckpoint.getCheckpointID());
                }
                scheduleTriggerRequest();
            }
        }
    }

    // 用于在全局状态层面对检查点触发请求进行前置检查
    private void preCheckGlobalState(boolean isPeriodic) throws CheckpointException {
        // abort if the coordinator has been shutdown in the meantime
        if (shutdown) {
            throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }

        // Don't allow periodic checkpoint if scheduling has been disabled
        if (isPeriodic && !periodicScheduling) {
            throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
        }
    }

    private void abortPendingAndQueuedCheckpoints(CheckpointException exception) {
        assert (Thread.holdsLock(lock));
        requestDecider.abortAll(exception);
        abortPendingCheckpoints(exception);
    }

    /**
     * The canceller of checkpoint. The checkpoint might be cancelled if it doesn't finish in a
     * configured period.
     */
    class CheckpointCanceller implements Runnable {

        private final PendingCheckpoint pendingCheckpoint;

        private CheckpointCanceller(PendingCheckpoint pendingCheckpoint) {
            this.pendingCheckpoint = checkNotNull(pendingCheckpoint);
        }

        @Override
        public void run() {
            synchronized (lock) {
                // only do the work if the checkpoint is not discarded anyways
                // note that checkpoint completion discards the pending checkpoint object
                if (!pendingCheckpoint.isDisposed()) {
                    LOG.info(
                            "Checkpoint {} of job {} expired before completing.",
                            pendingCheckpoint.getCheckpointID(),
                            job);

                    abortPendingCheckpoint(
                            pendingCheckpoint,
                            new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED));
                }
            }
        }
    }

    private static CheckpointException getCheckpointException(
            CheckpointFailureReason defaultReason, Throwable throwable) {

        final Optional<IOException> ioExceptionOptional =
                findThrowable(throwable, IOException.class);
        if (ioExceptionOptional.isPresent()) {
            return new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, throwable);
        } else {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    findThrowable(throwable, CheckpointException.class);
            return checkpointExceptionOptional.orElseGet(
                    () -> new CheckpointException(defaultReason, throwable));
        }
    }
    // 封装并跟踪 JobMaster 中一次检查点（或保存点 Savepoint）的触发请求的完整信息和状态。
    // 当 Flink 决定要进行一次检查点操作时（无论是定时触发还是用户手动触发），它会创建一个 CheckpointTriggerRequest 实例，并将该实例提交给负责协调检查点流程的组件（如 CheckpointCoordinator）。
    static class CheckpointTriggerRequest {
        // 请求触发时间戳。
        // 记录创建此请求实例时的系统时间（毫秒），用于追踪和调试
        final long timestamp;
        // 封装了本次检查点或保存点的具体属性，例如是否是保存点 (Savepoint)、是否是强制检查点 (forceCheckpoint)、以及检查点的类型和持久性等。
        final CheckpointProperties props;
        // 如果本次请求是保存点 (Savepoint)，
        // 此字段指定保存点文件的外部目标存储路径。如果是一般检查点，则为 null。
        final @Nullable String externalSavepointLocation;
        // 是否为周期性触发。
        // 标识这次检查点请求是定时自动触发的（true）还是手动/一次性触发的（false）。
        final boolean isPeriodic;
        // 完成承诺（结果 Future）。
        // 请求的发送方可以通过获取这个 Future 来阻塞等待或异步回调检查点完成的结果。
        private final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                new CompletableFuture<>();

        CheckpointTriggerRequest(
                CheckpointProperties props,
                @Nullable String externalSavepointLocation,
                boolean isPeriodic) {

            this.timestamp = System.currentTimeMillis();
            this.props = checkNotNull(props);
            this.externalSavepointLocation = externalSavepointLocation;
            this.isPeriodic = isPeriodic;
        }

        CompletableFuture<CompletedCheckpoint> getOnCompletionFuture() {
            return onCompletionPromise;
        }

        public void completeExceptionally(CheckpointException exception) {
            onCompletionPromise.completeExceptionally(exception);
        }

        public boolean isForce() {
            return props.forceCheckpoint();
        }
    }

    private enum OperatorCoordinatorRestoreBehavior {

        /** Coordinators are always restored. If there is no checkpoint, they are restored empty. */
        RESTORE_OR_RESET,

        /** Coordinators are restored if there was a checkpoint. */
        RESTORE_IF_CHECKPOINT_PRESENT,

        /** Coordinators are not restored during this checkpoint restore. */
        SKIP;
    }

    private PendingCheckpointStats trackPendingCheckpointStats(
            long checkpointId,
            CheckpointPlan checkpointPlan,
            CheckpointProperties props,
            long checkpointTimestamp) {
        Map<JobVertexID, Integer> vertices =
                Stream.concat(
                                checkpointPlan.getTasksToWaitFor().stream(),
                                checkpointPlan.getFinishedTasks().stream())
                        .map(Execution::getVertex)
                        .map(ExecutionVertex::getJobVertex)
                        .distinct()
                        .collect(
                                toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism));

        PendingCheckpointStats pendingCheckpointStats =
                statsTracker.reportPendingCheckpoint(
                        checkpointId, checkpointTimestamp, props, vertices);

        reportFinishedTasks(pendingCheckpointStats, checkpointPlan.getFinishedTasks());

        return pendingCheckpointStats;
    }

    private void reportFinishedTasks(
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            List<Execution> finishedTasks) {
        if (pendingCheckpointStats == null) {
            return;
        }

        long now = System.currentTimeMillis();
        finishedTasks.forEach(
                execution ->
                        pendingCheckpointStats.reportSubtaskStats(
                                execution.getVertex().getJobvertexId(),
                                new SubtaskStateStats(execution.getParallelSubtaskIndex(), now)));
    }

    @Nullable
    private PendingCheckpointStats getStatsCallback(PendingCheckpoint pendingCheckpoint) {
        return statsTracker.getPendingCheckpointStats(pendingCheckpoint.getCheckpointID());
    }
}
