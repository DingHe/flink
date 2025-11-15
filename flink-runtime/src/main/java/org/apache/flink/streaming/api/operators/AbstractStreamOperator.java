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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.IndexedCombinedWatermarkStatus;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.LatencyStats;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for all stream operators. Operators that contain a user function should extend the
 * class {@link AbstractUdfStreamOperator} instead (which is a specialized subclass of this class).
 *
 * <p>For concrete implementations, one of the following two interfaces must also be implemented, to
 * mark the operator as unary or binary: {@link OneInputStreamOperator} or {@link
 * TwoInputStreamOperator}.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * <p>Note, this class is going to be removed and replaced in the future by {@link
 * AbstractStreamOperatorV2}. However as {@link AbstractStreamOperatorV2} is currently experimental,
 * {@link AbstractStreamOperator} has not been deprecated just yet.
 *
 * @param <OUT> The output type of the operator.
 */
// Flink 所有流式算子（Stream Operator）的核心基类。
// 它封装了所有算子运行所需的基础设施、生命周期管理、状态处理、计时器服务和运行时上下文。
// 统一算子基线： 作为 Flink 流计算图（StreamGraph）中所有节点的通用实现基础，无论是 Source、Map、Filter 还是 Window 算子，都直接或间接继承自它。
// 封装运行时环境： 管理算子与 Flink 运行时环境（StreamTask、StreamConfig、Environment）之间的交互，提供了访问执行配置、类加载器和度量指标的接口。
// 处理状态与容错： 集中管理**键控状态（Keyed State）和算子状态（Operator State）**的初始化、快照（Snapshot）和恢复逻辑，是 Flink Checkpoint 机制在算子层面的入口。
// 提供服务： 实现了事件时间和处理时间**定时器服务（Timer Service）**和键（Key）的上下文管理，这是实现窗口、ProcessFunction 等复杂逻辑的基础。
// 处理控制流： 提供了处理 Watermark、LatencyMarker 和 RecordAttributes 等控制消息的默认逻辑。
@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
        implements StreamOperator<OUT>,
                SetupableStreamOperator<OUT>, // 提供setup方法、设置ChainingStrategy等
                YieldingOperator<OUT>,  // 设置MailboxExecutor
                CheckpointedStreamOperator,  //检查点的接口
                KeyContextHandler, //hasKeyContext
                Serializable {
    private static final long serialVersionUID = 1L;

    /** The logger used by the operator class and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

    // ----------- configuration properties -------------

    // A sane default for most operators
    // 定义算子链接策略。ChainingStrategy.HEAD 表示该算子是链的起点，ChainingStrategy.ALWAYS 表示它可以与前一个算子链接
    protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

    // ---------------- runtime fields ------------------

    /** The task that contains this operator (and other operators in the same chain). */
    //指向包含该算子的流任务（StreamTask）。
    // StreamTask 是 Flink 运行时中的一个执行单元，一个 StreamTask 可能包含一个或多个链接在一起的算子
    private transient StreamTask<?, ?> container;
    // 算子配置。
    // 包含了关于该算子的所有配置信息，如输入输出类型、算子 ID、并行度、状态分区器等
    protected transient StreamConfig config;
    //输出接口。
    // 用于将处理后的数据或控制消息（如水印）发送到下游算子
    protected transient Output<StreamRecord<OUT>> output;
    // 组合水位线状态。
    // 用于双输入算子（TwoInputStreamOperator），跟踪并计算两个输入流的组合水位线。
    private transient IndexedCombinedWatermarkStatus combinedWatermark;

    /** The runtime context for UDFs. */
    //运行时上下文。这是用户函数（UDF）访问 Flink 运行时信息的接口，如获取任务信息、配置、累加器、状态和定时器服务等
    private transient StreamingRuntimeContext runtimeContext;
    // 邮箱执行器。
    // 用于将控制请求（Mail）提交到 StreamTask 的邮箱中，实现与单线程邮箱模型的协作。
    private transient @Nullable MailboxExecutor mailboxExecutor;
    // 邮箱水位线处理器。
    // 当启用可拆分定时器（Splittable Timers）时，用于通过 Mailbox 机制发送 Watermark，以确保与定时器的交互是线程安全的。
    private transient @Nullable MailboxWatermarkProcessor watermarkProcessor;

    // ---------------- key/value state ------------------

    /**
     * {@code KeySelector} for extracting a key from an element being processed. This is used to
     * scope keyed state to a key. This is null if the operator is not a keyed operator.
     *
     * <p>This is for elements from the first input.
     */
    // 输入 1 键选择器。
    // 用于从第一个输入元素中提取键，以便将状态操作限定在特定键的范围内。
    // 对于非键控算子，此属性为 null。
    protected transient KeySelector<?, ?> stateKeySelector1;

    /**
     * {@code KeySelector} for extracting a key from an element being processed. This is used to
     * scope keyed state to a key. This is null if the operator is not a keyed operator.
     *
     * <p>This is for elements from the second input.
     */
    // 输入 2 键选择器。
    // 用于从第二个输入元素中提取键。
    protected transient KeySelector<?, ?> stateKeySelector2;
    // 状态处理器。
    // 管理算子的所有状态（键控状态和算子状态）的内部组件，处理状态的初始化、快照和恢复。
    protected transient StreamOperatorStateHandler stateHandler;
    // 时间服务管理器。
    // 负责管理算子的事件时间和处理时间定时器
    protected transient InternalTimeServiceManager<?> timeServiceManager;

    // --------------- Metrics ---------------------------

    /** Metric group for the operator. */
    // 度量指标组。
    // 用于收集和报告算子级别的运行时指标（如吞吐量、延迟等）。
    protected transient InternalOperatorMetricGroup metrics;
    // 延迟统计。
    // 用于记录和报告端到端数据处理延迟的工具。
    protected transient LatencyStats latencyStats;

    // ---------------- time handler ------------------
    // 处理时间服务。
    // 用于获取当前的机器时间并注册基于处理时间的定时器。
    protected transient ProcessingTimeService processingTimeService;
    // 输入 1 记录属性。
    // 用于存储最近接收到的第一个输入流的记录属性。
    protected transient RecordAttributes lastRecordAttributes1;
    // 输入 2 记录属性。
    // 用于存储最近接收到的第二个输入流的记录属性。
    protected transient RecordAttributes lastRecordAttributes2;

    // ------------------------------------------------------------------------
    //  Life Cycle
    // ------------------------------------------------------------------------
    // 算子的初始化。在算子生命周期中最早被调用，用于将算子与运行时环境绑定。
    // 它会设置 container、config、output、metrics、runtimeContext 等关键属性，并初始化延迟统计等
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        final Environment environment = containingTask.getEnvironment();
        this.container = containingTask;
        this.config = config;
        this.output = output;
        this.metrics =
                environment
                        .getMetricGroup()
                        .getOrAddOperator(config.getOperatorID(), config.getOperatorName());
        this.combinedWatermark = IndexedCombinedWatermarkStatus.forInputsCount(2);

        try {
            Configuration taskManagerConfig = environment.getTaskManagerInfo().getConfiguration();
            int historySize = taskManagerConfig.get(MetricOptions.LATENCY_HISTORY_SIZE);
            if (historySize <= 0) {
                LOG.warn(
                        "{} has been set to a value equal or below 0: {}. Using default.",
                        MetricOptions.LATENCY_HISTORY_SIZE,
                        historySize);
                historySize = MetricOptions.LATENCY_HISTORY_SIZE.defaultValue();
            }

            final String configuredGranularity =
                    taskManagerConfig.get(MetricOptions.LATENCY_SOURCE_GRANULARITY);
            LatencyStats.Granularity granularity;
            try {
                granularity =
                        LatencyStats.Granularity.valueOf(
                                configuredGranularity.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                granularity = LatencyStats.Granularity.OPERATOR;
                LOG.warn(
                        "Configured value {} option for {} is invalid. Defaulting to {}.",
                        configuredGranularity,
                        MetricOptions.LATENCY_SOURCE_GRANULARITY.key(),
                        granularity);
            }
            MetricGroup taskMetricGroup = this.metrics.getTaskMetricGroup();
            this.latencyStats =
                    new LatencyStats(
                            taskMetricGroup.addGroup("latency"),
                            historySize,
                            container.getIndexInSubtaskGroup(),
                            getOperatorID(),
                            granularity);
        } catch (Exception e) {
            LOG.warn("An error occurred while instantiating latency metrics.", e);
            this.latencyStats =
                    new LatencyStats(
                            UnregisteredMetricGroups.createUnregisteredTaskMetricGroup()
                                    .addGroup("latency"),
                            1,
                            0,
                            new OperatorID(),
                            LatencyStats.Granularity.SINGLE);
        }

        this.runtimeContext =
                new StreamingRuntimeContext(
                        environment,
                        environment.getAccumulatorRegistry().getUserMap(),
                        getMetricGroup(),
                        getOperatorID(),
                        getProcessingTimeService(),
                        null,
                        environment.getExternalResourceInfoProvider());

        stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
        stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());

        lastRecordAttributes1 = RecordAttributes.EMPTY_RECORD_ATTRIBUTES;
        lastRecordAttributes2 = RecordAttributes.EMPTY_RECORD_ATTRIBUTES;
    }

    /**
     * @deprecated The {@link ProcessingTimeService} instance should be passed by the operator
     *     constructor and this method will be removed along with {@link SetupableStreamOperator}.
     */
    @Deprecated
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return metrics;
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {

        final TypeSerializer<?> keySerializer =
                config.getStateKeySerializer(getUserCodeClassloader());

        final StreamTask<?, ?> containingTask = Preconditions.checkNotNull(getContainingTask());
        final CloseableRegistry streamTaskCloseableRegistry =
                Preconditions.checkNotNull(containingTask.getCancelables());

        final StreamOperatorStateContext context =
                streamTaskStateManager.streamOperatorStateContext(
                        getOperatorID(),
                        getClass().getSimpleName(),
                        getProcessingTimeService(),
                        this,
                        keySerializer,
                        streamTaskCloseableRegistry,
                        metrics,
                        config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.STATE_BACKEND,
                                runtimeContext.getJobConfiguration(),
                                runtimeContext.getTaskManagerRuntimeInfo().getConfiguration(),
                                runtimeContext.getUserCodeClassLoader()),
                        isUsingCustomRawKeyedState());

        stateHandler =
                new StreamOperatorStateHandler(
                        context, getExecutionConfig(), streamTaskCloseableRegistry);
        timeServiceManager = context.internalTimerServiceManager();
        stateHandler.initializeOperatorState(this);
        runtimeContext.setKeyedStateStore(stateHandler.getKeyedStateStore().orElse(null));
    }

    /**
     * Indicates whether or not implementations of this class is writing to the raw keyed state
     * streams on snapshots, using {@link #snapshotState(StateSnapshotContext)}. If yes, subclasses
     * should override this method to return {@code true}.
     *
     * <p>Subclasses need to explicitly indicate the use of raw keyed state because, internally, the
     * {@link AbstractStreamOperator} may attempt to read from it as well to restore heap-based
     * timers and ultimately fail with read errors. By setting this flag to {@code true}, this
     * allows the {@link AbstractStreamOperator} to know that the data written in the raw keyed
     * states were not written by the timer services, and skips the timer restore attempt.
     *
     * <p>Please refer to FLINK-19741 for further details.
     *
     * <p>TODO: this method can be removed once all timers are moved to be managed by state
     * backends.
     *
     * @return flag indicating whether or not this operator is writing to raw keyed state via {@link
     *     #snapshotState(StateSnapshotContext)}.
     */
    @Internal
    protected boolean isUsingCustomRawKeyedState() {
        return false;
    }

    @Internal
    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
        this.mailboxExecutor = mailboxExecutor;
    }

    /**
     * Can be overridden to disable splittable timers for this particular operator even if config
     * option is enabled. By default, splittable timers are disabled.
     *
     * @return {@code true} if splittable timers should be used (subject to {@link
     *     StreamConfig#isUnalignedCheckpointsEnabled()} and {@link
     *     StreamConfig#isUnalignedCheckpointsSplittableTimersEnabled()}. {@code false} if
     *     splittable timers should never be used.
     */
    @Internal
    public boolean useSplittableTimers() {
        return false;
    }

    @Internal
    private boolean areSplittableTimersConfigured() {
        return areSplittableTimersConfigured(config);
    }

    static boolean areSplittableTimersConfigured(StreamConfig config) {
        return config.isCheckpointingEnabled()
                && config.isUnalignedCheckpointsEnabled()
                && config.isUnalignedCheckpointsSplittableTimersEnabled();
    }

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic, e.g. state initialization.
     *
     * <p>The default implementation does nothing.
     *
     * @throws Exception An exception in this method causes the operator to fail.
     */
    @Override
    public void open() throws Exception {
        if (useSplittableTimers()
                && areSplittableTimersConfigured()
                && getTimeServiceManager().isPresent()) {
            this.watermarkProcessor =
                    new MailboxWatermarkProcessor(
                            output, mailboxExecutor, getTimeServiceManager().get());
        }
    }

    @Override
    public void finish() throws Exception {}

    @Override
    public void close() throws Exception {
        if (stateHandler != null) {
            stateHandler.dispose();
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // the default implementation does nothing and accepts the checkpoint
        // this is purely for subclasses to override
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        return stateHandler.snapshotState(
                this,
                Optional.ofNullable(timeServiceManager),
                getOperatorName(),
                checkpointId,
                timestamp,
                checkpointOptions,
                factory,
                isUsingCustomRawKeyedState());
    }

    /**
     * Stream operators with state, which want to participate in a snapshot need to override this
     * hook method.
     *
     * @param context context that provides information and means required for taking a snapshot
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {}

    /**
     * Stream operators with state which can be restored need to override this hook method.
     *
     * @param context context that allows to register different states.
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        stateHandler.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        stateHandler.notifyCheckpointAborted(checkpointId);
    }

    // ------------------------------------------------------------------------
    //  Properties and Services
    // ------------------------------------------------------------------------

    /**
     * Gets the execution config defined on the execution environment of the job to which this
     * operator belongs.
     *
     * @return The job's execution config.
     */
    public ExecutionConfig getExecutionConfig() {
        return container.getExecutionConfig();
    }

    public StreamConfig getOperatorConfig() {
        return config;
    }

    public StreamTask<?, ?> getContainingTask() {
        return container;
    }

    public ClassLoader getUserCodeClassloader() {
        return container.getUserCodeClassLoader();
    }

    /**
     * Return the operator name. If the runtime context has been set, then the task name with
     * subtask index is returned. Otherwise, the simple class name is returned.
     *
     * @return If runtime context is set, then return task name with subtask index. Otherwise return
     *     simple class name.
     */
    protected String getOperatorName() {
        if (runtimeContext != null) {
            return runtimeContext.getTaskInfo().getTaskNameWithSubtasks();
        } else {
            return getClass().getSimpleName();
        }
    }

    /**
     * Returns a context that allows the operator to query information about the execution and also
     * to interact with systems such as broadcast variables and managed state. This also allows to
     * register timers.
     */
    @VisibleForTesting
    public StreamingRuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public <K> KeyedStateBackend<K> getKeyedStateBackend() {
        return stateHandler.getKeyedStateBackend();
    }

    @VisibleForTesting
    public OperatorStateBackend getOperatorStateBackend() {
        return stateHandler.getOperatorStateBackend();
    }

    /**
     * Returns the {@link ProcessingTimeService} responsible for getting the current processing time
     * and registering timers.
     */
    @VisibleForTesting
    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    /**
     * Creates a partitioned state handle, using the state backend configured for this task.
     *
     * @throws IllegalStateException Thrown, if the key/value state was already initialized.
     * @throws Exception Thrown, if the state backend cannot create the key/value state.
     */
    protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    protected <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        return stateHandler.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
    }

    /**
     * Creates a partitioned state handle, using the state backend configured for this task.
     *
     * @throws IllegalStateException Thrown, if the key/value state was already initialized.
     * @throws Exception Thrown, if the state backend cannot create the key/value state.
     */
    protected <S extends State, N> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return stateHandler.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        setKeyContextElement(record, stateKeySelector1);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement2(StreamRecord record) throws Exception {
        setKeyContextElement(record, stateKeySelector2);
    }

    @Internal
    @Override
    public boolean hasKeyContext1() {
        return stateKeySelector1 != null;
    }

    @Internal
    @Override
    public boolean hasKeyContext2() {
        return stateKeySelector2 != null;
    }

    private <T> void setKeyContextElement(StreamRecord<T> record, KeySelector<T, ?> selector)
            throws Exception {
        if (selector != null) {
            Object key = selector.getKey(record.getValue());
            setCurrentKey(key);
        }
    }

    public void setCurrentKey(Object key) {
        stateHandler.setCurrentKey(key);
    }

    public Object getCurrentKey() {
        return stateHandler.getCurrentKey();
    }

    public KeyedStateStore getKeyedStateStore() {
        if (stateHandler == null) {
            return null;
        }
        return stateHandler.getKeyedStateStore().orElse(null);
    }

    protected KeySelector<?, ?> getStateKeySelector1() {
        return stateKeySelector1;
    }

    protected KeySelector<?, ?> getStateKeySelector2() {
        return stateKeySelector2;
    }

    // ------------------------------------------------------------------------
    //  Context and chaining properties
    // ------------------------------------------------------------------------

    @Override
    public final void setChainingStrategy(ChainingStrategy strategy) {
        this.chainingStrategy = strategy;
    }

    @Override
    public final ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    // ------------------------------------------------------------------------
    //  Metrics
    // ------------------------------------------------------------------------

    // ------- One input stream
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        reportOrForwardLatencyMarker(latencyMarker);
    }

    // ------- Two input stream
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        reportOrForwardLatencyMarker(latencyMarker);
    }

    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        reportOrForwardLatencyMarker(latencyMarker);
    }

    protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
        // all operators are tracking latencies
        this.latencyStats.reportLatency(marker);

        // everything except sinks forwards latency markers
        this.output.emitLatencyMarker(marker);
    }

    // ------------------------------------------------------------------------
    //  Watermark handling
    // ------------------------------------------------------------------------

    /**
     * Returns a {@link InternalTimerService} that can be used to query current processing time and
     * event time and to set timers. An operator can have several timer services, where each has its
     * own namespace serializer. Timer services are differentiated by the string key that is given
     * when requesting them, if you call this method with the same key multiple times you will get
     * the same timer service instance in subsequent requests.
     *
     * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
     * When a timer fires, this key will also be set as the currently active key.
     *
     * <p>Each timer has attached metadata, the namespace. Different timer services can have a
     * different namespace type. If you don't need namespace differentiation you can use {@link
     * VoidNamespaceSerializer} as the namespace serializer.
     *
     * @param name The name of the requested timer service. If no service exists under the given
     *     name a new one will be created and returned.
     * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
     * @param triggerable The {@link Triggerable} that should be invoked when timers fire
     * @param <N> The type of the timer namespace.
     */
    public <K, N> InternalTimerService<N> getInternalTimerService(
            String name, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {
        if (timeServiceManager == null) {
            throw new RuntimeException("The timer service has not been initialized.");
        }
        @SuppressWarnings("unchecked")
        InternalTimeServiceManager<K> keyedTimeServiceHandler =
                (InternalTimeServiceManager<K>) timeServiceManager;
        KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
        checkState(keyedStateBackend != null, "Timers can only be used on keyed operators.");
        return keyedTimeServiceHandler.getInternalTimerService(
                name, keyedStateBackend.getKeySerializer(), namespaceSerializer, triggerable);
    }

    public void processWatermark(Watermark mark) throws Exception {
        if (watermarkProcessor != null) {
            watermarkProcessor.emitWatermarkInsideMailbox(mark);
        } else {
            emitWatermarkDirectly(mark);
        }
    }

    private void emitWatermarkDirectly(Watermark mark) throws Exception {
        if (timeServiceManager != null) {
            timeServiceManager.advanceWatermark(mark);
        }
        output.emitWatermark(mark);
    }

    private void processWatermark(Watermark mark, int index) throws Exception {
        if (combinedWatermark.updateWatermark(index, mark.getTimestamp())) {
            processWatermark(new Watermark(combinedWatermark.getCombinedWatermark()));
        }
    }
   //watermark 通过 processWatermark() 来处理
    public void processWatermark1(Watermark mark) throws Exception {
        processWatermark(mark, 0);
    }

    public void processWatermark2(Watermark mark) throws Exception {
        processWatermark(mark, 1);
    }

    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        output.emitWatermarkStatus(watermarkStatus);
    }

    private void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
            throws Exception {
        boolean wasIdle = combinedWatermark.isIdle();
        if (combinedWatermark.updateStatus(index, watermarkStatus.isIdle())) {
            processWatermark(new Watermark(combinedWatermark.getCombinedWatermark()));
        }
        if (wasIdle != combinedWatermark.isIdle()) {
            output.emitWatermarkStatus(watermarkStatus);
        }
    }

    public final void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        processWatermarkStatus(watermarkStatus, 0);
    }

    public final void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        processWatermarkStatus(watermarkStatus, 1);
    }

    @Override
    public OperatorID getOperatorID() {
        return config.getOperatorID();
    }

    protected Optional<InternalTimeServiceManager<?>> getTimeServiceManager() {
        return Optional.ofNullable(timeServiceManager);
    }

    @Experimental
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        output.emitRecordAttributes(
                new RecordAttributesBuilder(Collections.singletonList(recordAttributes)).build());
    }

    @Experimental
    public void processRecordAttributes1(RecordAttributes recordAttributes) {
        lastRecordAttributes1 = recordAttributes;
        output.emitRecordAttributes(
                new RecordAttributesBuilder(
                                Arrays.asList(lastRecordAttributes1, lastRecordAttributes2))
                        .build());
    }

    @Experimental
    public void processRecordAttributes2(RecordAttributes recordAttributes) {
        lastRecordAttributes2 = recordAttributes;
        output.emitRecordAttributes(
                new RecordAttributesBuilder(
                                Arrays.asList(lastRecordAttributes1, lastRecordAttributes2))
                        .build());
    }
}
