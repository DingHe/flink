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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.state.v2.KeyedStateStoreV2;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext}, for streaming
 * operators.
 */
// StreamingRuntimeContext 类是 Flink 中**流式算子（Stream Operator）**执行 用户自定义函数（UDF）时所能访问的运行时上下文实现。
// 它继承自 AbstractRuntimeUDFContext，专门为 Flink 的 DataStream API 及其底层算子提供运行时环境和服务的入口。
// 环境门户： 充当 Flink 任务执行环境（Environment）和用户 UDF 之间的桥梁，允许 UDF 获取任务信息、配置、累加器等。
// 状态访问： 提供了所有类型的**键控状态（Keyed State）**的访问方法（ValueState, ListState, MapState 等），是用户函数进行状态操作的唯一接口。
// 服务集成： 集成了 Flink 的 处理时间服务（ProcessingTimeService） 和度量指标（Metrics）系统，使用户能够注册定时器和报告指标。
// 用户代码在 Flink 流环境中执行时，访问所有运行时服务和状态管理能力的上下文对象。
@Internal
public class StreamingRuntimeContext extends AbstractRuntimeUDFContext {

    /** The task environment running the operator. */
    // 任务执行环境。这是 Flink 运行时提供的底层环境对象，包含了 Job 和 Task 的所有配置、资源和管理接口（如输入分片提供者、全局聚合管理器等）
    private final Environment taskEnvironment;
    // 流配置。一个封装了 Environment 任务配置的专门类，用于提供流作业相关的配置信息，例如是否启用 Checkpointing。
    private final StreamConfig streamConfig;
    // 算子唯一 ID。当前算子的全局唯一标识符的字符串表示。它在同一 Job 内是唯一的，并且跨 Job 提交保持稳定。
    private final String operatorUniqueID;
    // 处理时间服务。用于获取当前机器时间以及注册基于处理时间的定时器。
    private final ProcessingTimeService processingTimeService;
    // 键控状态存储（V1）。
    // 这是用于访问 V1 版本键控状态的接口。它被 AbstractStreamOperator 初始化并注入，是用户 UDF 访问 ValueState 等的关键。对于非键控算子，此属性为 null。
    private @Nullable KeyedStateStore keyedStateStore;
    // 键控状态存储（V2）。
    // 用于访问 V2 版本键控状态的接口（在 FLIP-410 等未来状态管理改进中引入）
    private @Nullable KeyedStateStoreV2 keyedStateStoreV2;
    // 外部资源信息提供者。
    // 用于查询 TaskManager 上配置的外部资源（如 GPU、自定义设备）的相关信息。
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    @VisibleForTesting
    public StreamingRuntimeContext(
            AbstractStreamOperator<?> operator,
            Environment env,
            Map<String, Accumulator<?, ?>> accumulators) {
        this(
                env,
                accumulators,
                operator.getMetricGroup(),
                operator.getOperatorID(),
                operator.getProcessingTimeService(),
                operator.getKeyedStateStore(),
                env.getExternalResourceInfoProvider());
    }

    public StreamingRuntimeContext(
            Environment env,
            Map<String, Accumulator<?, ?>> accumulators,
            OperatorMetricGroup operatorMetricGroup,
            OperatorID operatorID,
            ProcessingTimeService processingTimeService,
            @Nullable KeyedStateStore keyedStateStore,
            ExternalResourceInfoProvider externalResourceInfoProvider) {
        super(
                checkNotNull(env).getJobInfo(),
                checkNotNull(env).getTaskInfo(),
                env.getUserCodeClassLoader(),
                env.getExecutionConfig(),
                accumulators,
                env.getDistributedCacheEntries(),
                operatorMetricGroup);
        this.taskEnvironment = env;
        this.streamConfig = new StreamConfig(env.getTaskConfiguration());
        this.operatorUniqueID = checkNotNull(operatorID).toString();
        this.processingTimeService = processingTimeService;
        this.keyedStateStore = keyedStateStore;
        this.externalResourceInfoProvider = externalResourceInfoProvider;
    }

    public void setKeyedStateStore(@Nullable KeyedStateStore keyedStateStore) {
        this.keyedStateStore = keyedStateStore;
    }

    public void setKeyedStateStoreV2(@Nullable KeyedStateStoreV2 keyedStateStoreV2) {
        this.keyedStateStoreV2 = keyedStateStoreV2;
    }

    // ------------------------------------------------------------------------

    /**
     * Returns the input split provider associated with the operator.
     *
     * @return The input split provider.
     */
    // 返回输入分片提供者。
    // 主要用于 Source 算子，用于在运行时请求下一个数据分片（如文件块或数据库范围）。
    public InputSplitProvider getInputSplitProvider() {
        return taskEnvironment.getInputSplitProvider();
    }
    // 返回处理时间服务实例，
    // 允许注册处理时间定时器和获取当前时间。
    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    /**
     * Returns the global aggregate manager for the current job.
     *
     * @return The global aggregate manager.
     */
    // 返回全局聚合管理器。
    // 允许算子向 JobManager 报告聚合值（如自定义指标或调试信息）
    public GlobalAggregateManager getGlobalAggregateManager() {
        return taskEnvironment.getGlobalAggregateManager();
    }

    /**
     * Returned value is guaranteed to be unique between operators within the same job and to be
     * stable and the same across job submissions.
     *
     * <p>This operation is currently only supported in Streaming (DataStream) contexts.
     *
     * @return String representation of the operator's unique id.
     */
    public String getOperatorUniqueID() {
        return operatorUniqueID;
    }

    /**
     * Returns the task manager runtime info of the task manager running this stream task.
     *
     * @return The task manager runtime info.
     */
    // 返回运行该任务的 TaskManager 的运行时信息（如配置、内存限制等）。
    public TaskManagerRuntimeInfo getTaskManagerRuntimeInfo() {
        return taskEnvironment.getTaskManagerInfo();
    }

    public Configuration getJobConfiguration() {
        return taskEnvironment.getJobConfiguration();
    }
    // 返回 Job 的类型（如 Streaming 或 Batch）。
    public JobType getJobType() {
        return taskEnvironment.getJobType();
    }

    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        return externalResourceInfoProvider.getExternalResourceInfos(resourceName);
    }

    // ------------------------------------------------------------------------
    //  broadcast variables
    // ------------------------------------------------------------------------

    @Override
    public boolean hasBroadcastVariable(String name) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(
            String name, BroadcastVariableInitializer<T, C> initializer) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }

    // ------------------------------------------------------------------------
    //  key/value state
    // ------------------------------------------------------------------------
    // 获取指定描述符的 单值状态（ValueState）
    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getState(stateProperties);
    }
    // 获取指定描述符的 列表状态（ListState）
    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getListState(stateProperties);
    }
    // 获取指定描述符的 归约状态（ReducingState）
    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getReducingState(stateProperties);
    }
    // 获取指定描述符的 聚合状态（AggregatingState）
    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getAggregatingState(stateProperties);
    }
    // 获取指定描述符的 映射状态（MapState）
    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getMapState(stateProperties);
    }
    // 检查状态描述符非空以及返回键控状态存储器
    private KeyedStateStore checkPreconditionsAndGetKeyedStateStore(
            StateDescriptor<?, ?> stateDescriptor) {
        checkNotNull(stateDescriptor, "The state properties must not be null");
        checkNotNull(
                keyedStateStore,
                String.format(
                        "Keyed state '%s' with type %s can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                        stateDescriptor.getName(), stateDescriptor.getType()));
        return keyedStateStore;
    }

    // TODO: Reconstruct this after StateManager is ready in FLIP-410.
    public <T> org.apache.flink.api.common.state.v2.ValueState<T> getValueState(
            org.apache.flink.runtime.state.v2.ValueStateDescriptor<T> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getValueState(stateProperties);
    }

    public <T> org.apache.flink.api.common.state.v2.ListState<T> getListState(
            org.apache.flink.runtime.state.v2.ListStateDescriptor<T> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getListState(stateProperties);
    }

    public <UK, UV> org.apache.flink.api.common.state.v2.MapState<UK, UV> getMapState(
            org.apache.flink.runtime.state.v2.MapStateDescriptor<UK, UV> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getMapState(stateProperties);
    }

    public <T> org.apache.flink.api.common.state.v2.ReducingState<T> getReducingState(
            org.apache.flink.runtime.state.v2.ReducingStateDescriptor<T> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getReducingState(stateProperties);
    }

    public <IN, ACC, OUT>
            org.apache.flink.api.common.state.v2.AggregatingState<IN, OUT> getAggregatingState(
                    org.apache.flink.runtime.state.v2.AggregatingStateDescriptor<IN, ACC, OUT>
                            stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getAggregatingState(stateProperties);
    }
    // 检查状态描述符并返回v2版本的键控状态存储器
    private KeyedStateStoreV2 checkPreconditionsAndGetKeyedStateStoreV2(
            org.apache.flink.runtime.state.v2.StateDescriptor<?> stateDescriptor) {
        checkNotNull(stateDescriptor, "The state properties must not be null");
        checkNotNull(
                keyedStateStoreV2,
                String.format(
                        "Keyed state '%s' with type %s can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                        stateDescriptor.getStateId(), stateDescriptor.getType()));
        return keyedStateStoreV2;
    }

    // ------------------ expose (read only) relevant information from the stream config -------- //

    /**
     * Returns true if checkpointing is enabled for the running job.
     *
     * @return true if checkpointing is enabled.
     */
    public boolean isCheckpointingEnabled() {
        return streamConfig.isCheckpointingEnabled();
    }
}
