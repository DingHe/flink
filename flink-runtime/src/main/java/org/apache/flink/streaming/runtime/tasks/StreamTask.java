/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.InitializationStatus;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageAccess;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.GaugePeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction.Suspension;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxMetricsController;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.PeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_PERIOD;
import static org.apache.flink.runtime.metrics.MetricNames.GATE_RESTORE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.INITIALIZE_STATE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.MAILBOX_START_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.READ_OUTPUT_DATA_DURATION;
import static org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.openChannelStateWriter;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed and
 * executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form the
 * Task's operator chain. Operators that are chained together execute synchronously in the same
 * thread and hence on the same stream partition. A common case for these chains are successive
 * map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators. The StreamTask is
 * specialized for the type of the head operator: one-input and two-input tasks, as well as for
 * sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 *
 * <pre>{@code
 * -- setInitialState -> provides state of all operators in the chain
 *
 * -- invoke()
 *       |
 *       +----> Create basic utils (config, etc) and load the chain of operators
 *       +----> operators.setup()
 *       +----> task specific init()
 *       +----> initialize-operator-states()
 *       +----> open-operators()
 *       +----> run()
 *       +----> finish-operators()
 *       +----> close-operators()
 *       +----> common cleanup
 *       +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a {@code
 * StreamOperator} must be synchronized on this lock object to ensure that no methods are called
 * concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
        implements TaskInvokable,
                CheckpointableTask,
                CoordinatedTask,
                AsyncExceptionHandler,
                ContainingTaskDetails {

    /** The thread group that holds all trigger timer threads.Thread thread = new Thread(group, () -> {})*/
    // 用于将多个线程组织成组，以便统一管理和控制
    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

    /** The logger used by the StreamTask and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

    // ------------------------------------------------------------------------

    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints. The execution will always be performed in the task
     * thread.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
    //立即执行还是同步执行
    private final StreamTaskActionExecutor actionExecutor;

    /** The input processor. Initialized in {@link #init()} method. */
    // 负责拉取输入数据
    @Nullable protected StreamInputProcessor inputProcessor;

    /** the main operator that consumes the input streams of this task. */
    protected OP mainOperator;

    /** The chain of operators executed by this task. */
    protected OperatorChain<OUT, OP> operatorChain;

    /** The configuration of this streaming task. */
    protected final StreamConfig configuration;

    /** Our state backend. We use this to create a keyed state backend. */
    protected final StateBackend stateBackend; //状态后端

    /** Our checkpoint storage. We use this to create checkpoint streams. */
    protected final CheckpointStorage checkpointStorage; // 检查点存储

    private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator; //子任务检查点协调器

    /**
     * The internal {@link TimerService} used to define the current processing time (default =
     * {@code System.currentTimeMillis()}) and register timers for tasks to be executed in the
     * future.
     */
    protected final TimerService timerService;

    /**
     * In contrast to {@link #timerService} we should not register any user timers here. It should
     * be used only for system level timers.
     */
    protected final TimerService systemTimerService;

    /** The currently active background materialization threads. */
    private final CloseableRegistry cancelables = new CloseableRegistry();

    private final AutoCloseableRegistry resourceCloser;

    private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

    /**
     * Flag to mark the task "in operation", in which case check needs to be initialized to true, so
     * that early cancel() before invoke() behaves correctly.
     */
    private volatile boolean isRunning;

    /** Flag to mark the task at restoring duration in {@link #restore()}. */
    private volatile boolean isRestoring;

    /** Flag to mark this task as canceled. */
    private volatile boolean canceled;

    /**
     * Flag to mark this task as failing, i.e. if an exception has occurred inside {@link
     * #invoke()}.
     */
    private volatile boolean failing;

    /** Flags indicating the finished method of all the operators are called. */
    // 是否所有的operators的finished方法被调用
    private boolean finishedOperators;

    private boolean closedOperators;

    /** Thread pool for async snapshot workers. */
    private final ExecutorService asyncOperationsThreadPool;// 异步检查点线程池

    protected final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter; // 记录输出

    protected final MailboxProcessor mailboxProcessor; //消息处理器

    final MailboxExecutor mainMailboxExecutor;

    /** TODO it might be replaced by the global IO executor on TaskManager level future. */
    private final ExecutorService channelIOExecutor;

    // ========================================================
    //  Final  checkpoint / savepoint
    // ========================================================
    private Long syncSavepoint = null;
    private Long finalCheckpointMinId = null;
    private final CompletableFuture<Void> finalCheckpointCompleted = new CompletableFuture<>();

    private long latestReportCheckpointId = -1;

    private long latestAsyncCheckpointStartDelayNanos;
    //标志已经完成数据的接收
    private volatile boolean endOfDataReceived = false;

    private final long bufferDebloatPeriod;

    private final Environment environment;

    private final Object shouldInterruptOnCancelLock = new Object();

    @GuardedBy("shouldInterruptOnCancelLock")
    private boolean shouldInterruptOnCancel = true;

    @Nullable private final AvailabilityProvider changelogWriterAvailabilityProvider;

    private long initializeStateEndTs;

    // ------------------------------------------------------------------------

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    protected StreamTask(Environment env) throws Exception {
        this(env, null);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.时间服务
     */
    protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        this(env, timerService, FatalExitExceptionHandler.INSTANCE);
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                StreamTaskActionExecutor.IMMEDIATE);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the timer service) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param environment The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread
     *     pool
     * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently,
     *     only SynchronizedActionExecutor can be used to preserve locking semantics.
     */
    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                actionExecutor,
                new TaskMailboxImpl(Thread.currentThread()));
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {
        // The registration of all closeable resources. The order of registration is important.
        resourceCloser = new AutoCloseableRegistry();
        try {
            this.environment = environment;
            this.configuration = new StreamConfig(environment.getTaskConfiguration());

            // Initialize mailbox metrics
            MailboxMetricsController mailboxMetricsControl =
                    new MailboxMetricsController(
                            environment.getMetricGroup().getIOMetricGroup().getMailboxLatency(),
                            environment
                                    .getMetricGroup()
                                    .getIOMetricGroup()
                                    .getNumMailsProcessedCounter());
            environment
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .registerMailboxSizeSupplier(() -> mailbox.size());

            this.mailboxProcessor =
                    new MailboxProcessor(
                            this::processInput, mailbox, actionExecutor, mailboxMetricsControl);

            // Should be closed last.
            resourceCloser.registerCloseable(mailboxProcessor);

            this.channelIOExecutor =
                    MdcUtils.scopeToJob(
                            environment.getJobID(),
                            Executors.newSingleThreadExecutor(
                                    new ExecutorThreadFactory("channel-state-unspilling")));
            resourceCloser.registerCloseable(channelIOExecutor::shutdown);

            this.recordWriter = createRecordWriterDelegate(configuration, environment);
            // Release the output resources. this method should never fail.
            resourceCloser.registerCloseable(this::releaseOutputResources);
            // If the operators won't be closed explicitly, register it to a hard close.
            resourceCloser.registerCloseable(this::closeAllOperators);
            resourceCloser.registerCloseable(this::cleanUpInternal);

            this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
            this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
            this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);

            // With maxConcurrentCheckpoints + 1 we more or less adhere to the
            // maxConcurrentCheckpoints configuration, but allow for a small leeway with allowing
            // for simultaneous N ongoing concurrent checkpoints and for example clean up of one
            // aborted one.
            this.asyncOperationsThreadPool =
                    MdcUtils.scopeToJob(
                            getEnvironment().getJobID(),
                            new ThreadPoolExecutor(
                                    0,
                                    configuration.getMaxConcurrentCheckpoints() + 1,
                                    60L,
                                    TimeUnit.SECONDS,
                                    new LinkedBlockingQueue<>(),
                                    new ExecutorThreadFactory(
                                            "AsyncOperations", uncaughtExceptionHandler)));

            // Register all asynchronous checkpoint threads.
            resourceCloser.registerCloseable(this::shutdownAsyncThreads);
            resourceCloser.registerCloseable(cancelables);

            environment.setMainMailboxExecutor(mainMailboxExecutor);
            environment.setAsyncOperationsThreadPool(asyncOperationsThreadPool);

            this.stateBackend = createStateBackend(); //创建状态后端
            this.checkpointStorage = createCheckpointStorage(stateBackend); //检查点
            this.changelogWriterAvailabilityProvider =
                    environment.getTaskStateManager().getStateChangelogStorage() == null
                            ? null
                            : environment
                                    .getTaskStateManager()
                                    .getStateChangelogStorage()
                                    .getAvailabilityProvider();

            CheckpointStorageAccess checkpointStorageAccess =
                    checkpointStorage.createCheckpointStorage(getEnvironment().getJobID());
            checkpointStorageAccess =
                    tryApplyFileMergingCheckpoint(
                            checkpointStorageAccess,
                            environment.getTaskStateManager().getFileMergingSnapshotManager());
            environment.setCheckpointStorageAccess(checkpointStorageAccess);

            // if the clock is not already set, then assign a default TimeServiceProvider
            if (timerService == null) {
                this.timerService = createTimerService("Time Trigger for " + getName());
            } else {
                this.timerService = timerService;
            }

            this.systemTimerService = createTimerService("System Time Trigger for " + getName());
            final CheckpointStorageAccess finalCheckpointStorageAccess = checkpointStorageAccess;

            ChannelStateWriter channelStateWriter =
                    configuration.isUnalignedCheckpointsEnabled()
                            ? openChannelStateWriter(
                                    getName(),
                                    // Note: don't pass checkpointStorageAccess directly to channel
                                    // state writer.
                                    // The fileSystem of checkpointStorageAccess may be an instance
                                    // of SafetyNetWrapperFileSystem, which close all held streams
                                    // when thread exits. Channel state writers are invoked in other
                                    // threads instead of task thread, therefore channel state
                                    // writer cannot share file streams directly, otherwise
                                    // conflicts will occur on job exit.
                                    () -> {
                                        if (finalCheckpointStorageAccess
                                                instanceof FsMergingCheckpointStorageAccess) {
                                            // FsMergingCheckpointStorageAccess using unguarded
                                            // fileSystem, which can be shared.
                                            return finalCheckpointStorageAccess;
                                        } else {
                                            // Other checkpoint storage access should be lazily
                                            // initialized to avoid sharing.
                                            return checkpointStorage.createCheckpointStorage(
                                                    getEnvironment().getJobID());
                                        }
                                    },
                                    environment,
                                    configuration.getMaxSubtasksPerChannelStateFile())
                            : ChannelStateWriter.NO_OP;
            this.subtaskCheckpointCoordinator =
                    new SubtaskCheckpointCoordinatorImpl(
                            checkpointStorageAccess,
                            getName(),
                            actionExecutor,
                            getAsyncOperationsThreadPool(),
                            environment,
                            this,
                            this::prepareInputSnapshot,
                            configuration.getMaxConcurrentCheckpoints(),
                            channelStateWriter,
                            configuration
                                    .getConfiguration()
                                    .get(
                                            CheckpointingOptions
                                                    .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH),
                            BarrierAlignmentUtil.createRegisterTimerCallback(
                                    mainMailboxExecutor, systemTimerService),
                            environment.getTaskStateManager().getFileMergingSnapshotManager());
            resourceCloser.registerCloseable(subtaskCheckpointCoordinator::close);

            // Register to stop all timers and threads. Should be closed first.
            resourceCloser.registerCloseable(this::tryShutdownTimerService);

            injectChannelStateWriterIntoChannels();

            environment.getMetricGroup().getIOMetricGroup().setEnableBusyTime(true);
            Configuration taskManagerConf = environment.getTaskManagerInfo().getConfiguration();

            this.bufferDebloatPeriod = taskManagerConf.get(BUFFER_DEBLOAT_PERIOD).toMillis();
            mailboxMetricsControl.setupLatencyMeasurement(systemTimerService, mainMailboxExecutor);
        } catch (Exception ex) {
            try {
                resourceCloser.close();
            } catch (Throwable throwable) {
                ex.addSuppressed(throwable);
            }
            throw ex;
        }
    }
   //检查点完成时： 当一个检查点成功完成时，Flink会触发这个方法，开始合并状态文件
   // 当任务从故障中恢复时，Flink会调用这个方法，将合并后的状态文件应用到任务中。
    private CheckpointStorageAccess tryApplyFileMergingCheckpoint(
            CheckpointStorageAccess checkpointStorageAccess,
            @Nullable FileMergingSnapshotManager fileMergingSnapshotManager) {
        if (fileMergingSnapshotManager == null) {
            return checkpointStorageAccess;
        }
        try {
            CheckpointStorageAccess mergingCheckpointStorageAccess =
                    (CheckpointStorageAccess)
                            checkpointStorageAccess.toFileMergingStorage(
                                    fileMergingSnapshotManager, environment);
            mergingCheckpointStorageAccess.initializeBaseLocationsForCheckpoint();
            if (mergingCheckpointStorageAccess instanceof FsMergingCheckpointStorageAccess) {
                resourceCloser.registerCloseable(
                        () ->
                                ((FsMergingCheckpointStorageAccess) mergingCheckpointStorageAccess)
                                        .close());
            }
            return mergingCheckpointStorageAccess;
        } catch (IOException e) {
            LOG.warn(
                    "Initiating FsMergingCheckpointStorageAccess failed "
                            + "with exception: {}, falling back to original checkpoint storage access {}.",
                    e.getMessage(),
                    checkpointStorageAccess.getClass(),
                    e);
            return checkpointStorageAccess;
        }
    }

    private TimerService createTimerService(String timerThreadName) {
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, timerThreadName);
        return new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
    }
   //这段代码的目的是将 ChannelStateWriter 注入到所有输入通道（InputGate）和支持通道状态的输出通道（ResultPartitionWriter）中。这样，在执行 Flink 的检查点操作时，输入和输出的通道都能够将它们的状态（例如缓存、缓冲区等）持久化到外部存储中，以确保在发生故障时能够恢复状态，保证 Flink 任务的容错性
    private void injectChannelStateWriterIntoChannels() {
        final Environment env = getEnvironment();
        final ChannelStateWriter channelStateWriter =
                subtaskCheckpointCoordinator.getChannelStateWriter();
        for (final InputGate gate : env.getAllInputGates()) {
            gate.setChannelStateWriter(channelStateWriter);
        }
        for (ResultPartitionWriter writer : env.getAllWriters()) {
            if (writer instanceof ChannelStateHolder) {
                ((ChannelStateHolder) writer).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    private CompletableFuture<Void> prepareInputSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (inputProcessor == null) {
            return FutureUtils.completedVoidFuture();
        }
        return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
    }

    SubtaskCheckpointCoordinator getCheckpointCoordinator() {
        return subtaskCheckpointCoordinator;
    }

    // ------------------------------------------------------------------------
    //  Life cycle methods for specific implementations
    // ------------------------------------------------------------------------
   //做特定 task 的初始化。这里所说的特定 task，取决于 task 的类型 (SourceTask、OneInputStreamTask 或 TwoInputStreamTask 等)
    protected abstract void init() throws Exception;

    protected void cancelTask() throws Exception {}

    /**
     * This method implements the default action of the task (e.g. processing one event from the
     * input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception on any problems in the action.
     */
    // StreamTask 的 processInput 方法是 Flink 单线程邮箱模型中 MailboxDefaultAction 的核心实现之一，负责处理输入数据
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        // 调用 inputProcessor.processInput() 执行实际的输入数据处理逻辑
        // （例如，从网络缓冲区拉取记录，反序列化，然后将其发送给算子
        DataInputStatus status = inputProcessor.processInput();
        switch (status) {
            case MORE_AVAILABLE:
                // 表示任务可以继续运行，即没有反压或其它暂停因素
                if (taskIsAvailable()) {
                    return;
                }
                break;
            //暂时没有数据可用
            case NOTHING_AVAILABLE:
                break;
            // 恢复结束（异常）。
            // 此状态通常在内部恢复过程中使用，不应在 processInput 的默认执行路径中接收到。
            // 如果收到，表示状态异常，抛出异常。
            case END_OF_RECOVERY:
                throw new IllegalStateException("We should not receive this event here.");
            // 输入流被停止（非优雅停止）。
            // 表示上游任务被停止，数据流被硬性截断，无需等待下游处理完（NO_DRAIN）。
            // 调用 endData 处理流的结束，并 return 退出当前方法。
            case STOPPED:
                endData(StopMode.NO_DRAIN);
                return;
            // 输入数据结束（优雅停止）。
            // 表示当前输入的所有数据记录已处理完毕，需要等待所有记录被下游处理完（DRAIN）
            case END_OF_DATA:
                endData(StopMode.DRAIN);
                notifyEndOfData(); //通知TaskManager没有更多的数据到达了
                return;
            // 输入流结束。
            // 表示所有输入通道都已耗尽，并且所有上游算子已完成。
            case END_OF_INPUT:
                // Suspend the mailbox processor, it would be resumed in afterInvoke and finished
                // after all records processed by the downstream tasks. We also suspend the default
                // actions to avoid repeat executing the empty default operation (namely process
                // records).
                controller.suspendDefaultAction();
                mailboxProcessor.suspend();
                return;
        }

        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        PeriodTimer timer;
        CompletableFuture<?> resumeFuture;
        // 检查输出（通过 recordWriter）是否可用。
        // 如果不可用，说明下游反压，任务不能将结果发送出去。
        if (!recordWriter.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getSoftBackPressuredTimePerSecond());
            resumeFuture = recordWriter.getAvailableFuture();
        // 如果输出可用，则检查输入（通过 inputProcessor）是否可用。
        // 如果不可用，说明输入空闲，没有数据可以拉取。
        } else if (!inputProcessor.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getIdleTimeMsPerSecond());
            resumeFuture = inputProcessor.getAvailableFuture();
        // 检查状态变更日志写入器是否可用 (Changelog Busy)
        } else if (changelogWriterAvailabilityProvider != null
                && !changelogWriterAvailabilityProvider.isAvailable()) {
            // waiting for changelog availability is reported as busy
            timer = new GaugePeriodTimer(ioMetrics.getChangelogBusyTimeMsPerSecond());
            resumeFuture = changelogWriterAvailabilityProvider.getAvailableFuture();
        } else {
            // data availability has changed in the meantime; retry immediately
            return;
        }
        assertNoException(
                resumeFuture.thenRun(
                        new ResumeWrapper(controller.suspendDefaultAction(timer), timer)));
    }
    // 用于处理流任务数据结束的信号，并在任务结束时执行必要的清理和收尾工作。
    // 它通常在任务完成处理所有输入数据（或在停止操作期间）被调用。
    protected void endData(StopMode mode) throws Exception {
        // 检查任务停止的模式是否为 DRAIN（数据耗尽模式）。
        // 在 DRAIN 模式下，任务目标是处理完所有剩余数据和状态，不丢弃任何内容
        if (mode == StopMode.DRAIN) {
            // 如果处于 DRAIN 模式，该方法会将事件时间的水位线（Watermark）推进到最大值（$Long.MAX\_VALUE$）
            // 强制触发所有基于事件时间的窗口和定时器，确保所有未计算的窗口（包括那些等待 Watermark 到来的）都能立即完成计算并输出结果，从而达到彻底清空所有待处理事件和状态的目的。
            advanceToEndOfEventTime();
        }
        // finish all operators in a chain effect way
        // 调用操作符链上的 finishOperators 方法
        // 这会依次调用操作符链中所有 StreamOperator 实例的 finish() 方法
        operatorChain.finishOperators(actionExecutor, mode);
        this.finishedOperators = true;
        // 获取当前 Task 中所有输出结果分区的写入器
        for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
            // 这将导致输出通道向下游发送一个 EndOfPartitionEvent（或类似的结束信号）。
            // 下游 Task 接收到这个信号后就知道这个上游分区已经没有更多数据会到达了。
            partitionWriter.notifyEndOfData(mode);
        }

        this.endOfDataReceived = true;
    }
    // 作用是通知 Flink 任务管理器（TaskManager），当前任务（StreamTask）已经完成了数据的处理，并且没有更多数据会从上游到达。
    protected void notifyEndOfData() {
        environment.getTaskManagerActions().notifyEndOfData(environment.getExecutionId());
    }

    protected void setSynchronousSavepoint(long checkpointId) {
        checkState(
                syncSavepoint == null || syncSavepoint == checkpointId,
                "at most one stop-with-savepoint checkpoint at a time is allowed");
        syncSavepoint = checkpointId;
    }

    @VisibleForTesting
    OptionalLong getSynchronousSavepointId() {
        if (syncSavepoint != null) {
            return OptionalLong.of(syncSavepoint);
        } else {
            return OptionalLong.empty();
        }
    }

    private boolean isCurrentSyncSavepoint(long checkpointId) {
        return syncSavepoint != null && syncSavepoint == checkpointId;
    }

    /**
     * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK
     * MAX_WATERMARK} so that all registered timers are fired.
     *
     * <p>This is used by the source task when the job is {@code TERMINATED}. In the case, we want
     * all the timers registered throughout the pipeline to fire and the related state (e.g.
     * windows) to be flushed.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void advanceToEndOfEventTime() throws Exception {}

    // ------------------------------------------------------------------------
    //  Core work methods of the Stream Task
    // ------------------------------------------------------------------------

    public StreamTaskStateInitializer createStreamTaskStateInitializer(
            SubTaskInitializationMetricsBuilder initializationMetrics) {
        InternalTimeServiceManager.Provider timerServiceProvider =
                configuration.getTimerServiceProvider(getUserCodeClassLoader());
        return new StreamTaskStateInitializerImpl(
                getEnvironment(),
                stateBackend,
                initializationMetrics,
                TtlTimeProvider.DEFAULT,
                timerServiceProvider != null
                        ? timerServiceProvider
                        : InternalTimeServiceManagerImpl::create,
                () -> canceled);
    }

    protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
        try {
            return streamOperator.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        } catch (Exception e) {
            LOG.warn("An exception occurred during the metrics setup.", e);
            return new SimpleCounter();
        }
    }

    @Override
    public final void restore() throws Exception {
        restoreInternal();
    }

    void restoreInternal() throws Exception {
        if (isRunning) {
            LOG.debug("Re-restore attempt rejected.");
            return;
        }
        isRestoring = true;
        closedOperators = false;
        getEnvironment().getMetricGroup().getIOMetricGroup().markTaskInitializationStarted();
        LOG.debug("Initializing {}.", getName());

        SubTaskInitializationMetricsBuilder initializationMetrics =
                new SubTaskInitializationMetricsBuilder(
                        SystemClock.getInstance().absoluteTimeMillis());
        try {
            operatorChain =
                    getEnvironment().getTaskStateManager().isTaskDeployedAsFinished()
                            ? new FinishedOperatorChain<>(this, recordWriter)
                            : new RegularOperatorChain<>(this, recordWriter);
            mainOperator = operatorChain.getMainOperator();

            getEnvironment()
                    .getTaskStateManager()
                    .getRestoreCheckpointId()
                    .ifPresent(restoreId -> latestReportCheckpointId = restoreId);

            // task specific initialization
            init();
            configuration.clearInitialConfigs();

            // save the work of reloading state, etc, if the task is already canceled
            ensureNotCanceled();

            // -------- Invoke --------
            LOG.debug("Invoking {}", getName());

            // we need to make sure that any triggers scheduled in open() cannot be
            // executed before all operators are opened
            CompletableFuture<Void> allGatesRecoveredFuture =
                    actionExecutor.call(() -> restoreStateAndGates(initializationMetrics));

            // Run mailbox until all gates will be recovered.
            mailboxProcessor.runMailboxLoop();

            initializationMetrics.addDurationMetric(
                    GATE_RESTORE_DURATION,
                    SystemClock.getInstance().absoluteTimeMillis() - initializeStateEndTs);

            ensureNotCanceled();

            checkState(
                    allGatesRecoveredFuture.isDone(),
                    "Mailbox loop interrupted before recovery was finished.");

            // we recovered all the gates, we can close the channel IO executor as it is no longer
            // needed
            channelIOExecutor.shutdown();

            isRunning = true;
            isRestoring = false;
            initializationMetrics.setStatus(InitializationStatus.COMPLETED);
        } finally {
            environment
                    .getTaskStateManager()
                    .reportInitializationMetrics(initializationMetrics.build());
        }
    }

    private CompletableFuture<Void> restoreStateAndGates(
            SubTaskInitializationMetricsBuilder initializationMetrics) throws Exception {

        long mailboxStartTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                MAILBOX_START_DURATION,
                mailboxStartTs - initializationMetrics.getInitializationStartTs());

        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        long readOutputDataTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                READ_OUTPUT_DATA_DURATION, readOutputDataTs - mailboxStartTs);

        operatorChain.initializeStateAndOpenOperators(
                createStreamTaskStateInitializer(initializationMetrics));

        initializeStateEndTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                INITIALIZE_STATE_DURATION, initializeStateEndTs - readOutputDataTs);
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        channelIOExecutor.execute(
                () -> {
                    try {
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        // We wait for all input channel state to recover before we go into RUNNING state, and thus
        // start checkpointing. If we implement incremental checkpointing of input channel state
        // we must make sure it supports CheckpointType#FULL_CHECKPOINT
        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());

            inputGate
                    .getStateConsumedFuture()
                    .thenRun(
                            () ->
                                    mainMailboxExecutor.execute(
                                            inputGate::requestPartitions,
                                            "Input gate request partitions"));
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
                .thenRun(mailboxProcessor::suspend);
    }

    private void ensureNotCanceled() {
        if (canceled) {
            throw new CancelTaskException();
        }
    }
    // Flink 任务执行的核心入口点，负责任务的启动、状态恢复、主循环运行以及清理等关键步骤。
    @Override
    public final void invoke() throws Exception {
        // Allow invoking method 'invoke' without having to call 'restore' before it.
        // 检查当前任务是否已经处于运行状态。通常在任务启动时，isRunning 应为 false。
        if (!isRunning) {
            LOG.debug("Restoring during invoke will be called.");
            // 调用内部方法执行状态恢复。这个步骤对于有状态（Stateful）的 Flink 任务至关重要，
            // 它会加载上一个检查点（Checkpoint）或保存点（Savepoint）的状态数据，以便任务从中断的地方继续执行。
            restoreInternal();
        }

        // final check to exit early before starting to run
        // 在正式进入主处理循环之前，进行最后一次检查。
        // 如果任务在启动过程中被取消，此方法会抛出异常，提前终止 invoke() 的执行，避免资源浪费。
        ensureNotCanceled();
        // 调度 Flink 的缓冲区去臃肿器（Buffer Debloater）。
        // 这是一个性能优化机制，特别是在网络栈中。它根据运行时反馈动态调整网络传输缓冲区的大小，以优化吞吐量和延迟，确保高效利用内存资源。
        scheduleBufferDebloater();

        // let the task do its work
        // 标记任务的实际计算（I/O）开始时间。
        // 这用于 Flink 的运行时指标系统，精确计算任务的运行时间和 I/O 吞吐量。
        getEnvironment().getMetricGroup().getIOMetricGroup().markTaskStart();
        // Flink 任务处理逻辑的核心。
        // StreamTask 使用 Mailbox 模型 进行并发控制和事件处理。
        runMailboxLoop();

        // if this left the run() method cleanly despite the fact that this was canceled,
        // make sure the "clean shutdown" is not attempted
        // 这个检查非常重要，它确保只有在任务正常完成（非取消状态）的情况下，才能执行后续的清理步骤
        ensureNotCanceled();
        // 内部方法执行任务运行结束后的清理逻辑
        afterInvoke();
    }

    private void scheduleBufferDebloater() {
        // See https://issues.apache.org/jira/browse/FLINK-23560
        // If there are no input gates, there is no point of calculating the throughput and running
        // the debloater. At the same time, for SourceStreamTask using legacy sources and checkpoint
        // lock, enqueuing even a single mailbox action can cause performance regression. This is
        // especially visible in batch, with disabled checkpointing and no processing time timers.
        if (getEnvironment().getAllInputGates().length == 0
                || !environment
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED)) {
            return;
        }
        systemTimerService.registerTimer(
                systemTimerService.getCurrentProcessingTime() + bufferDebloatPeriod,
                timestamp ->
                        mainMailboxExecutor.execute(
                                () -> {
                                    debloat();
                                    scheduleBufferDebloater();
                                },
                                "Buffer size recalculation"));
    }

    @VisibleForTesting
    void debloat() {
        for (IndexedInputGate inputGate : environment.getAllInputGates()) {
            inputGate.triggerDebloating();
        }
    }

    @VisibleForTesting
    public boolean runSingleMailboxLoop() throws Exception {
        return mailboxProcessor.runSingleMailboxLoop();
    }

    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        return mailboxProcessor.runMailboxStep();
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxProcessor.isMailboxLoopRunning();
    }

    public void runMailboxLoop() throws Exception {
        mailboxProcessor.runMailboxLoop();
    }

    protected void afterInvoke() throws Exception {
        LOG.debug("Finished task {}", getName());
        getCompletionFuture().exceptionally(unused -> null).join();

        Set<CompletableFuture<Void>> terminationConditions = new HashSet<>();
        // If checkpoints are enabled, waits for all the records get processed by the downstream
        // tasks. During this process, this task could coordinate with its downstream tasks to
        // continue perform checkpoints.
        if (endOfDataReceived && areCheckpointsWithFinishedTasksEnabled()) {
            LOG.debug("Waiting for all the records processed by the downstream tasks.");

            for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
                terminationConditions.add(partitionWriter.getAllDataProcessedFuture());
            }

            terminationConditions.add(finalCheckpointCompleted);
        }

        if (syncSavepoint != null) {
            terminationConditions.add(finalCheckpointCompleted);
        }

        FutureUtils.waitForAll(terminationConditions)
                .thenRun(mailboxProcessor::allActionsCompleted);

        // Resumes the mailbox processor. The mailbox processor would be completed
        // after all records are processed by the downstream tasks.
        mailboxProcessor.runMailboxLoop();

        // make sure no further checkpoint and notification actions happen.
        // at the same time, this makes sure that during any "regular" exit where still
        actionExecutor.runThrowing(
                () -> {
                    // make sure no new timers can come
                    timerService.quiesce().get();
                    systemTimerService.quiesce().get();

                    // let mailbox execution reject all new letters from this point
                    mailboxProcessor.prepareClose();
                });

        // processes the remaining mails; no new mails can be enqueued
        mailboxProcessor.drain();

        // Set isRunning to false after all the mails are drained so that
        // the queued checkpoint requirements could be triggered normally.
        actionExecutor.runThrowing(
                () -> {
                    // only set the StreamTask to not running after all operators have been
                    // finished!
                    // See FLINK-7430
                    isRunning = false;
                });

        LOG.debug("Finished operators for task {}", getName());

        // make sure all buffered data is flushed
        operatorChain.flushOutputs();

        if (areCheckpointsWithFinishedTasksEnabled()) {
            // No new checkpoints could be triggered since mailbox has been drained.
            subtaskCheckpointCoordinator.waitForPendingCheckpoints();
            LOG.debug("All pending checkpoints are finished");
        }

        disableInterruptOnCancel();

        // make an attempt to dispose the operators such that failures in the dispose call
        // still let the computation fail
        closeAllOperators();
    }

    private boolean areCheckpointsWithFinishedTasksEnabled() {
        return configuration
                        .getConfiguration()
                        .get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)
                && configuration.isCheckpointingEnabled();
    }

    @Override
    public final void cleanUp(Throwable throwable) throws Exception {
        LOG.debug(
                "Cleanup StreamTask (operators closed: {}, cancelled: {})",
                closedOperators,
                canceled);

        failing = !canceled && throwable != null;

        Exception cancelException = null;
        if (throwable != null) {
            try {
                cancelTask();
            } catch (Throwable t) {
                cancelException = t instanceof Exception ? (Exception) t : new Exception(t);
            }
        }

        disableInterruptOnCancel();

        // note: This `.join()` is already uninterruptible, so it doesn't matter if we have already
        // disabled the interruptions or not.
        getCompletionFuture().exceptionally(unused -> null).join();
        // clean up everything we initialized
        isRunning = false;

        // clear any previously issued interrupt for a more graceful shutdown
        Thread.interrupted();

        try {
            resourceCloser.close();
        } catch (Throwable t) {
            Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
            throw firstOrSuppressed(e, cancelException);
        }
    }

    protected void cleanUpInternal() throws Exception {
        if (inputProcessor != null) {
            inputProcessor.close();
        }
    }

    protected CompletableFuture<Void> getCompletionFuture() {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public final void cancel() throws Exception {
        isRunning = false;
        canceled = true;

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        // the "cancel task" call must come first, but the cancelables must be
        // closed no matter what
        try {
            cancelTask();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            getCompletionFuture()
                    .whenComplete(
                            (unusedResult, unusedError) -> {
                                // WARN: the method is called from the task thread but the callback
                                // can be invoked from a different thread
                                mailboxProcessor.allActionsCompleted();
                                try {
                                    subtaskCheckpointCoordinator.cancel();
                                    cancelables.close();
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            });
        }
    }

    public MailboxExecutorFactory getMailboxExecutorFactory() {
        return this.mailboxProcessor::getMailboxExecutor;
    }

    public boolean hasMail() {
        return mailboxProcessor.hasMail();
    }

    // 判断 Flink 任务是否可以继续处理数据，即任务是否因下游反压或状态变更日志写入拥塞而被阻塞。
    private boolean taskIsAvailable() {
        // isAvailable()： 检查输出缓冲区是否有空间写入数据。
        return recordWriter.isAvailable()
                // 任务必须同时满足下游输出可用 和 Changelog 写入器可用才能继续运行。
                && (changelogWriterAvailabilityProvider == null
                        || changelogWriterAvailabilityProvider.isAvailable());
    }

    public CanEmitBatchOfRecordsChecker getCanEmitBatchOfRecords() {
        return () -> !this.mailboxProcessor.hasMail() && taskIsAvailable();
    }

    public final boolean isRunning() {
        return isRunning;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public final boolean isFailing() {
        return failing;
    }
    // 关闭异步线程池
    private void shutdownAsyncThreads() throws Exception {
        if (!asyncOperationsThreadPool.isShutdown()) {
            asyncOperationsThreadPool.shutdownNow();
        }
    }

    private void releaseOutputResources() throws Exception {
        if (operatorChain != null) {
            // beware: without synchronization, #performCheckpoint() may run in
            //         parallel and this call is not thread-safe
            actionExecutor.run(() -> operatorChain.close());
        } else {
            // failed to allocate operatorChain, clean up record writers
            recordWriter.close();
        }
    }

    /** Closes all the operators if not closed before. */
    private void closeAllOperators() throws Exception {
        if (operatorChain != null && !closedOperators) {
            closedOperators = true;
            operatorChain.closeAllOperators();
        }
    }

    /**
     * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
     * shutdown method was never called.
     *
     * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
     * shutdown is attempted, and cause threads to linger for longer than needed.
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!timerService.isTerminated()) {
            LOG.info("Timer service is shutting down.");
            timerService.shutdownService();
        }

        if (!systemTimerService.isTerminated()) {
            LOG.info("System timer service is shutting down.");
            systemTimerService.shutdownService();
        }

        cancelables.close();
    }

    boolean isSerializingTimestamps() {
        TimeCharacteristic tc = configuration.getTimeCharacteristic();
        return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
    }

    // ------------------------------------------------------------------------
    //  Access to properties and utilities
    // ------------------------------------------------------------------------

    /**
     * Gets the name of the task, in the form "taskname (2/5)".
     *
     * @return The name of the task.
     */
    public final String getName() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    /**
     * Gets the name of the task, appended with the subtask indicator and execution id.
     *
     * @return The name of the task, with subtask indicator and execution id.
     */
    String getTaskNameWithSubtaskAndId() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks()
                + " ("
                + getEnvironment().getExecutionId()
                + ')';
    }

    public CheckpointStorageWorkerView getCheckpointStorage() {
        return subtaskCheckpointCoordinator.getCheckpointStorage();
    }

    public StreamConfig getConfiguration() {
        return configuration;
    }

    RecordWriterOutput<?>[] getStreamOutputs() {
        return operatorChain.getStreamOutputs();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and Restore
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        checkForcedFullSnapshotSupport(checkpointOptions);

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    try {
                        boolean noUnfinishedInputGates =
                                Arrays.stream(getEnvironment().getAllInputGates())
                                        .allMatch(InputGate::isFinished);

                        if (noUnfinishedInputGates) {
                            result.complete(
                                    triggerCheckpointAsyncInMailbox(
                                            checkpointMetaData, checkpointOptions));
                        } else {
                            result.complete(
                                    triggerUnfinishedChannelsCheckpoint(
                                            checkpointMetaData, checkpointOptions));
                        }
                    } catch (Exception ex) {
                        // Report the failure both via the Future result but also to the mailbox
                        result.completeExceptionally(ex);
                        throw ex;
                    }
                },
                "checkpoint %s with %s",
                checkpointMetaData,
                checkpointOptions);
        return result;
    }

    private boolean triggerCheckpointAsyncInMailbox(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            latestAsyncCheckpointStartDelayNanos =
                    1_000_000
                            * Math.max(
                                    0,
                                    System.currentTimeMillis() - checkpointMetaData.getTimestamp());

            // No alignment if we inject a checkpoint
            CheckpointMetricsBuilder checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(latestAsyncCheckpointStartDelayNanos);

            subtaskCheckpointCoordinator.initInputsCheckpoint(
                    checkpointMetaData.getCheckpointId(), checkpointOptions);

            boolean success =
                    performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (!success) {
                declineCheckpoint(checkpointMetaData.getCheckpointId());
            }
            return success;
        } catch (Exception e) {
            // propagate exceptions only if the task is still in "running" state
            if (isRunning) {
                throw new Exception(
                        "Could not perform checkpoint "
                                + checkpointMetaData.getCheckpointId()
                                + " for operator "
                                + getName()
                                + '.',
                        e);
            } else {
                LOG.debug(
                        "Could not perform checkpoint {} for operator {} while the "
                                + "invokable was not in state running.",
                        checkpointMetaData.getCheckpointId(),
                        getName(),
                        e);
                return false;
            }
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    private boolean triggerUnfinishedChannelsCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        Optional<CheckpointBarrierHandler> checkpointBarrierHandler = getCheckpointBarrierHandler();
        checkState(
                checkpointBarrierHandler.isPresent(),
                "CheckpointBarrier should exist for tasks with network inputs.");

        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        checkpointMetaData.getCheckpointId(),
                        checkpointMetaData.getTimestamp(),
                        checkpointOptions);

        for (IndexedInputGate inputGate : getEnvironment().getAllInputGates()) {
            if (!inputGate.isFinished()) {
                for (InputChannelInfo channelInfo : inputGate.getUnfinishedChannels()) {
                    checkpointBarrierHandler.get().processBarrier(barrier, channelInfo, true);
                }
            }
        }

        return true;
    }

    /**
     * Acquires the optional {@link CheckpointBarrierHandler} associated with this stream task. The
     * {@code CheckpointBarrierHandler} should exist if the task has data inputs and requires to
     * align the barriers.
     */
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.empty();
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
        } catch (CancelTaskException e) {
            LOG.info(
                    "Operator {} was cancelled while performing checkpoint {}.",
                    getName(),
                    checkpointMetaData.getCheckpointId());
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Could not perform checkpoint "
                            + checkpointMetaData.getCheckpointId()
                            + " for operator "
                            + getName()
                            + '.',
                    e);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        if (isCurrentSyncSavepoint(checkpointId)) {
            throw new FlinkRuntimeException("Stop-with-savepoint failed.");
        }
        subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
    }

    private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {

        final SnapshotType checkpointType = checkpointOptions.getCheckpointType();
        LOG.debug(
                "Starting checkpoint {} {} on task {}",
                checkpointMetaData.getCheckpointId(),
                checkpointType,
                getName());

        if (isRunning) {
            actionExecutor.runThrowing(
                    () -> {
                        if (isSynchronous(checkpointType)) {
                            setSynchronousSavepoint(checkpointMetaData.getCheckpointId());
                        }

                        if (areCheckpointsWithFinishedTasksEnabled()
                                && endOfDataReceived
                                && this.finalCheckpointMinId == null) {
                            this.finalCheckpointMinId = checkpointMetaData.getCheckpointId();
                        }

                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                finishedOperators,
                                this::isRunning);
                    });

            return true;
        } else {
            actionExecutor.runThrowing(
                    () -> {
                        // we cannot perform our checkpoint - let the downstream operators know that
                        // they
                        // should not wait for any input from this operator

                        // we cannot broadcast the cancellation markers on the 'operator chain',
                        // because it may not
                        // yet be created
                        final CancelCheckpointMarker message =
                                new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
                        recordWriter.broadcastEvent(message);
                    });

            return false;
        }
    }

    private boolean isSynchronous(SnapshotType checkpointType) {
        return checkpointType.isSavepoint() && ((SavepointType) checkpointType).isSynchronous();
    }

    private void checkForcedFullSnapshotSupport(CheckpointOptions checkpointOptions) {
        if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)
                && !stateBackend.supportsNoClaimRestoreMode()) {
            throw new IllegalStateException(
                    String.format(
                            "Configured state backend (%s) does not support enforcing a full"
                                    + " snapshot. If you are restoring in %s mode, please"
                                    + " consider choosing %s mode.",
                            stateBackend, RecoveryClaimMode.NO_CLAIM, RecoveryClaimMode.CLAIM));
        } else if (checkpointOptions.getCheckpointType().isSavepoint()) {
            SavepointType savepointType = (SavepointType) checkpointOptions.getCheckpointType();
            if (!stateBackend.supportsSavepointFormat(savepointType.getFormatType())) {
                throw new IllegalStateException(
                        String.format(
                                "Configured state backend (%s) does not support %s savepoints",
                                stateBackend, savepointType.getFormatType()));
            }
        }
    }

    protected void declineCheckpoint(long checkpointId) {
        getEnvironment()
                .declineCheckpoint(
                        checkpointId,
                        new CheckpointException(
                                "Task Name" + getName(),
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    public final ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () -> notifyCheckpointComplete(checkpointId),
                String.format("checkpoint %d complete", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        return notifyCheckpointOperation(
                () -> {
                    if (latestCompletedCheckpointId > 0) {
                        notifyCheckpointComplete(latestCompletedCheckpointId);
                    }

                    if (isCurrentSyncSavepoint(checkpointId)) {
                        throw new FlinkRuntimeException("Stop-with-savepoint failed.");
                    }
                    subtaskCheckpointCoordinator.notifyCheckpointAborted(
                            checkpointId, operatorChain, this::isRunning);
                },
                String.format("checkpoint %d aborted", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () ->
                        subtaskCheckpointCoordinator.notifyCheckpointSubsumed(
                                checkpointId, operatorChain, this::isRunning),
                String.format("checkpoint %d subsumed", checkpointId));
    }

    private Future<Void> notifyCheckpointOperation(
            RunnableWithException runnable, String description) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        mailboxProcessor
                .getMailboxExecutor(TaskMailbox.MAX_PRIORITY)
                .execute(
                        () -> {
                            try {
                                runnable.run();
                            } catch (Exception ex) {
                                result.completeExceptionally(ex);
                                throw ex;
                            }
                            result.complete(null);
                        },
                        description);
        return result;
    }

    private void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Notify checkpoint {} complete on task {}", checkpointId, getName());

        if (checkpointId <= latestReportCheckpointId) {
            return;
        }

        latestReportCheckpointId = checkpointId;

        subtaskCheckpointCoordinator.notifyCheckpointComplete(
                checkpointId, operatorChain, this::isRunning);
        if (isRunning) {
            if (isCurrentSyncSavepoint(checkpointId)) {
                finalCheckpointCompleted.complete(null);
            } else if (syncSavepoint == null
                    && finalCheckpointMinId != null
                    && checkpointId >= finalCheckpointMinId) {
                finalCheckpointCompleted.complete(null);
            }
        }
    }

    private void tryShutdownTimerService() {
        final long timeoutMs =
                getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS)
                        .toMillis();
        tryShutdownTimerService(timeoutMs, timerService);
        tryShutdownTimerService(timeoutMs, systemTimerService);
    }

    private void tryShutdownTimerService(long timeoutMs, TimerService timerService) {
        if (!timerService.isTerminated()) {
            if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
                LOG.warn(
                        "Timer service shutdown exceeded time limit of {} ms while waiting for pending "
                                + "timers. Will continue with shutdown procedure.",
                        timeoutMs);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Operator Events
    // ------------------------------------------------------------------------

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        try {
            mainMailboxExecutor.execute(
                    () -> operatorChain.dispatchOperatorEvent(operator, event),
                    "dispatch operator event");
        } catch (RejectedExecutionException e) {
            // this happens during shutdown, we can swallow this
        }
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------
    // 负责初始化当前任务将使用的 状态后端（State Backend），这是 Flink 状态管理和持久化的核心组件。
    // 目的是确定并加载任务应使用的实际 StateBackend 实例，它遵循 Flink 的状态后端优先级规则（应用配置 > Job 配置 > 集群/默认配置）
    private StateBackend createStateBackend() throws Exception {
        // 尝试从当前任务的配置中获取用户在应用代码中明确设置的 StateBackend 实例
        final StateBackend fromApplication =
                configuration.getStateBackend(getUserCodeClassLoader());
        // 实现了 Flink 状态后端配置的优先级和回退机制
        return StateBackendLoader.fromApplicationOrConfigOrDefault(
                fromApplication,
                getJobConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }
    // 作用是为当前任务初始化和创建 检查点存储（Checkpoint Storage） 实例。检查点存储负责将检查点元数据和状态数据写入持久化存储介质（如 HDFS、S3 等）
    // 根据 Flink 的配置优先级规则，确定任务应该使用哪个 CheckpointStorage 实例
    private CheckpointStorage createCheckpointStorage(StateBackend backend) throws Exception {
        final CheckpointStorage fromApplication =
                configuration.getCheckpointStorage(getUserCodeClassLoader());
        return CheckpointStorageLoader.load(
                fromApplication,
                backend,
                getJobConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    /**
     * Returns the {@link TimerService} responsible for telling the current processing time and
     * registering actual timers.
     */
    @VisibleForTesting
    TimerService getTimerService() {
        return timerService;
    }

    @VisibleForTesting
    OP getMainOperator() {
        return this.mainOperator;
    }

    @VisibleForTesting
    StreamTaskActionExecutor getActionExecutor() {
        return actionExecutor;
    }

    public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
        return mailboxExecutor ->
                new ProcessingTimeServiceImpl(
                        timerService,
                        callback -> deferCallbackToMailbox(mailboxExecutor, callback));
    }

    /**
     * Handles an exception thrown by another thread (e.g. a TriggerTask), other than the one
     * executing the main task by failing the task entirely.
     *
     * <p>In more detail, it marks task execution failed for an external reason (a reason other than
     * the task code itself throwing an exception). If the task is already in a terminal state (such
     * as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running, starts an
     * asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void handleAsyncException(String message, Throwable exception) {
        if (isRestoring || isRunning) {
            // only fail if the task is still in restoring or running
            asyncExceptionHandler.handleAsyncException(message, exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return getName();
    }

    // ------------------------------------------------------------------------
    //异步错误处理
    /** Utility class to encapsulate the handling of asynchronous exceptions. */
    static class StreamTaskAsyncExceptionHandler implements AsyncExceptionHandler {
        private final Environment environment;

        StreamTaskAsyncExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        @Override
        public void handleAsyncException(String message, Throwable exception) {
            environment.failExternally(new AsynchronousException(message, exception));
        }
    }

    public final CloseableRegistry getCancelables() {
        return cancelables;
    }

    // ------------------------------------------------------------------------
    // 方法的作用是根据任务的非链式输出数量，创建一个合适的 RecordWriterDelegate 代理对象。
    // 这个代理对象封装了一个或多个 RecordWriter，为任务提供统一的输出接口。
    @VisibleForTesting
    public static <OUT>
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>
                    createRecordWriterDelegate(
                            StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites =
                createRecordWriters(configuration, environment);
        if (recordWrites.size() == 1) {
            return new SingleRecordWriter<>(recordWrites.get(0));
        } else if (recordWrites.size() == 0) {
            return new NonRecordWriter<>();
        } else {
            return new MultipleRecordWriters<>(recordWrites);
        }
    }
    // 于批量创建一个任务（Task）所有**非链式（Non-Chained）**输出所需的 RecordWriter 列表。
    // 目的是根据任务配置，为每个连接到下游非链式任务的输出边创建一个专门负责数据发送的 RecordWriter
    private static <OUT>
            List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
                    StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters =
                new ArrayList<>();
        // 获取所有非链式输出配置
        // 从任务配置中获取当前任务节点所有非链式输出的配置列表 (NonChainedOutput)
        // 非链式输出指的是那些连接到另一个独立运行的 Task 的输出边（而不是连接到同一 Task 内部的操作符链上的）。
        // 这些输出需要通过网络或本地 ResultPartition 发送数据，因此需要 RecordWriter
        List<NonChainedOutput> outputsInOrder =
                configuration.getVertexNonChainedOutputs(
                        environment.getUserCodeClassLoader().asClassLoader());

        int index = 0;
        for (NonChainedOutput streamOutput : outputsInOrder) {
            replaceForwardPartitionerIfConsumerParallelismDoesNotMatch(
                    environment, streamOutput, index);
            recordWriters.add(
                    createRecordWriter(
                            streamOutput,
                            index++,
                            environment,
                            environment.getTaskInfo().getTaskNameWithSubtasks(),
                            streamOutput.getBufferTimeout()));
        }
        return recordWriters;
    }
    // 该方法用于在特定条件下替换数据分区器（Partitioner），以避免数据传输错误或不均匀。
    // 目的是确保当上游任务使用 ForwardPartitioner（直接转发）时，
    // 如果下游任务的并行度不匹配，则将分区器自动替换为 RebalancePartitioner（重平衡）
    // environment: 当前 StreamTask 的运行时环境，用于获取任务信息（如并行度）和输出写入器信息。
    // streamOutput: 非链式（Non-Chained）输出对象，它封装了数据流的分区器和序列化器等信息。
    // outputIndex: 当前输出的索引（当一个 Task 有多个输出时）
    private static void replaceForwardPartitionerIfConsumerParallelismDoesNotMatch(
            Environment environment, NonChainedOutput streamOutput, int outputIndex) {
        if (streamOutput.getPartitioner() instanceof ForwardPartitioner
                && environment.getWriter(outputIndex).getNumberOfSubpartitions()
                        != environment.getTaskInfo().getNumberOfParallelSubtasks()) {
            LOG.debug(
                    "Replacing forward partitioner with rebalance for {}",
                    environment.getTaskInfo().getTaskNameWithSubtasks());
            streamOutput.setPartitioner(new RebalancePartitioner<>());
        }
    }

    // 方法的作用是为 非链式（Non-Chained） 的下游任务创建一个 RecordWriter，
    // 它是 Task 将数据发送到网络输出缓冲区和下游任务的核心组件。
    // streamOutput	NonChainedOutput	封装了当前输出流的配置，包括原始分区器 (Partitioner)。
    // outputIndex	int	当前输出流在 Task 中的索引（当一个 Task 有多个输出时）。
    // taskNameWithSubtask	String	当前 Task 及其子任务（Subtask）的名称，用于日志和指标报告。
    // bufferTimeout	long	缓冲区超时时间（以毫秒为单位），用于控制数据刷新到网络的时间，平衡延迟和吞吐量。
    @SuppressWarnings("unchecked")
    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            NonChainedOutput streamOutput,
            int outputIndex,
            Environment environment,
            String taskNameWithSubtask,
            long bufferTimeout) {

        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        // 对 streamOutput 中配置的原始分区器进行深拷贝（Clone）
        // 确保每个输出边（Stream Edge）都拥有一个独立的分区器实例。这解决了 [FLINK-14087] 描述的问题：
        // 如果多个输出边共享同一个分区器对象，当其中一个输出边在使用或修改分区器状态时，可能会影响到其他输出边的正确性
        try {
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) streamOutput.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskNameWithSubtask);
        // ResultPartitionWriter 是一个低级的 I/O 组件，负责管理当前 Task 的输出缓冲区和网络传输，
        // 并将数据写入到正确的结果分区（Result Partition）中，以便下游 Task 可以消费。
        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            // 获取目标 Key Groups 数量（即 Flink 集群的最大并行度）
            // 对于像 KeyGroupStreamPartitioner 这样的基于 Key Group 的分区器，它需要知道 Key Group 的总数才能正确地将 Key 映射到输出子分区（Subpartitions）
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        // 设置数据写入器的通道选择器为前面准备好的 outputPartitioner。
                        // 这意味着 RecordWriter 将使用这个分区器来决定将每条记录发送到下游的哪个子任务/通道
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskNameWithSubtask)
                        .build(bufferWriter);
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        return output;
    }

    private void handleTimerException(Exception ex) {
        handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
    }

    @VisibleForTesting
    ProcessingTimeCallback deferCallbackToMailbox(
            MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
        return timestamp -> {
            mailboxExecutor.execute(
                    () -> invokeProcessingTimeCallback(callback, timestamp),
                    "Timer callback for %s @ %d",
                    callback,
                    timestamp);
        };
    }

    private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
        try {
            callback.onProcessingTime(timestamp);
        } catch (Throwable t) {
            handleAsyncException("Caught exception while processing timer.", new TimerException(t));
        }
    }

    protected long getAsyncCheckpointStartDelayNanos() {
        return latestAsyncCheckpointStartDelayNanos;
    }

    private static class ResumeWrapper implements Runnable {
        private final Suspension suspendedDefaultAction;
        @Nullable private final PeriodTimer timer;

        public ResumeWrapper(Suspension suspendedDefaultAction, @Nullable PeriodTimer timer) {
            this.suspendedDefaultAction = suspendedDefaultAction;
            if (timer != null) {
                timer.markStart();
            }
            this.timer = timer;
        }

        @Override
        public void run() {
            if (timer != null) {
                timer.markEnd();
            }
            suspendedDefaultAction.resume();
        }
    }

    @Override
    public boolean isUsingNonBlockingInput() {
        return true;
    }

    /**
     * While we are outside the user code, we do not want to be interrupted further upon
     * cancellation. The shutdown logic below needs to make sure it does not issue calls that block
     * and stall shutdown. Additionally, the cancellation watch dog will issue a hard-cancel (kill
     * the TaskManager process) as a backup in case some shutdown procedure blocks outside our
     * control.
     */
    private void disableInterruptOnCancel() {
        synchronized (shouldInterruptOnCancelLock) {
            shouldInterruptOnCancel = false;
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        synchronized (shouldInterruptOnCancelLock) {
            if (shouldInterruptOnCancel) {
                if (taskName != null && timeout != null) {
                    Task.logTaskThreadStackTrace(toInterrupt, taskName, timeout, "interrupting");
                }

                toInterrupt.interrupt();
            }
        }
    }

    @Override
    public final Environment getEnvironment() {
        return environment;
    }

    /** Check whether records can be emitted in batch. */
    @FunctionalInterface
    public interface CanEmitBatchOfRecordsChecker {

        boolean check();
    }
}
