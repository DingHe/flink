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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base implementation of KeyedStateBackend. The state can be checkpointed to streams using {@link
 * #snapshot(long, long, CheckpointStreamFactory, CheckpointOptions)}.
 *
 * @param <K> Type of the key by which state is keyed.
 */
// 所有实现键控状态（Keyed State） 存储和管理能力的 Flink 状态后端（如 HeapKeyedStateBackend 和 RocksDBKeyedStateBackend）的抽象基类。
// 提供通用框架： 它实现了 KeyedStateBackend 接口中所有不依赖于底层存储细节的通用逻辑和基础设施，包括 Key 上下文管理、状态缓存、监听器注册、配置解析以及资源清理等。
// 它是一个可 Checkpoint 的 Keyed State Backend 的通用骨架，负责大部分的协调、配置和管理逻辑，将状态的实际存储和快照工作留给具体的子类实现。

public abstract class AbstractKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>,
                InternalCheckpointListener,
                TestableKeyedStateBackend<K>,
                InternalKeyContext<K> {

    /** The key serializer. */
    // Key 序列化器。 用于序列化和反序列化 Key 的 TypeSerializer 实例。
    protected final TypeSerializer<K> keySerializer;

    /** Listeners to changes of ({@link #keyContext}). */
    // Key 选择监听器列表。 存储所有注册的监听器，用于在 Key 上下文切换时接收回调通知。
    private final ArrayList<KeySelectionListener<K>> keySelectionListeners;

    /** So that we can give out state when the user uses the same key. */
    // 键值状态映射。
    // 存储所有已创建/注册的内部键值状态实例（InternalKvState），键是状态名称（StateDescriptor 的名称）
    private final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    /** For caching the last accessed partitioned state. */
    // 上次访问的状态名称。 用于状态缓存优化，避免重复查找状态实例。
    private String lastName;
    // 上次访问的状态实例。
    // 配合 lastName，用于状态缓存优化。
    @SuppressWarnings("rawtypes")
    private InternalKvState lastState;

    /** The number of key-groups aka max parallelism. */
    // Key Group 总数。
    // 即 Flink 作业的最大并行度，决定了 Key 空间的划分粒度。
    protected final int numberOfKeyGroups;

    /** Range of key-groups for which this backend is responsible. */
    // Key Group 范围。
    // 当前 TaskManager 实例负责处理的 Key Group 编号范围。
    protected final KeyGroupRange keyGroupRange;

    /** KvStateRegistry helper for this task. */
    // KvState 注册中心。
    // 用于将 Keyed State 注册到 Queryable State 服务，支持外部查询。
    protected final TaskKvStateRegistry kvStateRegistry;

    /**
     * Registry for all opened streams, so they can be closed if the task using this backend is
     * closed.
     */
    // 可关闭流注册器。
    // 用于注册 Checkpoint 过程中打开的流（Stream），以便在任务取消或关闭时能够安全地清理和关闭这些资源。
    protected CloseableRegistry cancelStreamRegistry;
    // 用户代码类加载器。
    // 用于加载用户定义的类，如自定义序列化器。
    protected final ClassLoader userCodeClassLoader;
    // 执行配置。
    // 包含了运行时配置信息，如是否启用快照压缩等。
    private final ExecutionConfig executionConfig;
    // TTL 时间提供者。
    // 用于获取当前时间戳，支持 状态 Time-To-Live (TTL) 机制。
    protected final TtlTimeProvider ttlTimeProvider;
    // 延迟追踪配置。
    // 包含了状态访问延迟追踪（Metrics）的配置信息。
    protected final LatencyTrackingStateConfig latencyTrackingStateConfig;

    /** Decorates the input and output streams to write key-groups compressed. */
    // 流压缩装饰器。
    // 用于在 Checkpoint 期间对 Key Group 数据流进行压缩/解压缩，通常默认为 Snappy 或不压缩。
    protected final StreamCompressionDecorator keyGroupCompressionDecorator;

    /** The key context for this backend. */
    // Key 上下文。
    // 封装了当前 Key 和 Key Group 索引的实际存储和管理。
    protected final InternalKeyContext<K> keyContext;

    public AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            InternalKeyContext<K> keyContext) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                determineStreamCompression(executionConfig),
                keyContext);
    }

    public AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                Preconditions.checkNotNull(keyContext),
                keyContext.getNumberOfKeyGroups(),
                keyContext.getKeyGroupRange(),
                new HashMap<>(),
                new ArrayList<>(1),
                null,
                null);
    }

    // Copy constructor
    protected AbstractKeyedStateBackend(AbstractKeyedStateBackend<K> abstractKeyedStateBackend) {
        this(
                abstractKeyedStateBackend.kvStateRegistry,
                abstractKeyedStateBackend.keySerializer,
                abstractKeyedStateBackend.userCodeClassLoader,
                abstractKeyedStateBackend.executionConfig,
                abstractKeyedStateBackend.ttlTimeProvider,
                abstractKeyedStateBackend.latencyTrackingStateConfig,
                abstractKeyedStateBackend.cancelStreamRegistry,
                abstractKeyedStateBackend.keyGroupCompressionDecorator,
                abstractKeyedStateBackend.keyContext,
                abstractKeyedStateBackend.numberOfKeyGroups,
                abstractKeyedStateBackend.keyGroupRange,
                abstractKeyedStateBackend.keyValueStatesByName,
                abstractKeyedStateBackend.keySelectionListeners,
                abstractKeyedStateBackend.lastState,
                abstractKeyedStateBackend.lastName);
    }

    @SuppressWarnings("rawtypes")
    private AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName,
            ArrayList<KeySelectionListener<K>> keySelectionListeners,
            InternalKvState lastState,
            String lastName) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
        Preconditions.checkArgument(
                numberOfKeyGroups >= 1, "NumberOfKeyGroups must be a positive number");
        Preconditions.checkArgument(
                numberOfKeyGroups >= keyGroupRange.getNumberOfKeyGroups(),
                "The total number of key groups must be at least the number in the key group range assigned to this backend. "
                        + "The total number of key groups: %s, the number in key groups in range: %s",
                numberOfKeyGroups,
                keyGroupRange.getNumberOfKeyGroups());

        this.kvStateRegistry = kvStateRegistry;
        this.keySerializer = keySerializer;
        this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.keyValueStatesByName = keyValueStatesByName;
        this.executionConfig = executionConfig;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.ttlTimeProvider = Preconditions.checkNotNull(ttlTimeProvider);
        this.latencyTrackingStateConfig = Preconditions.checkNotNull(latencyTrackingStateConfig);
        this.keySelectionListeners = keySelectionListeners;
        this.lastState = lastState;
        this.lastName = lastName;
    }

    private static StreamCompressionDecorator determineStreamCompression(
            ExecutionConfig executionConfig) {
        if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
            return SnappyStreamCompressionDecorator.INSTANCE;
        } else {
            return UncompressedStreamCompressionDecorator.INSTANCE;
        }
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {}

    /**
     * Closes the state backend, releasing all internal resources, but does not delete any
     * persistent checkpoint data.
     */
    @Override
    public void dispose() {

        IOUtils.closeQuietly(cancelStreamRegistry);

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterAll();
        }

        lastName = null;
        lastState = null;
        keyValueStatesByName.clear();
    }

    /** @see KeyedStateBackend */
    @Override
    public void setCurrentKey(K newKey) {
        notifyKeySelected(newKey);
        this.keyContext.setCurrentKey(newKey);
        this.keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups));
    }

    private void notifyKeySelected(K newKey) {
        // we prefer a for-loop over other iteration schemes for performance reasons here.
        for (int i = 0; i < keySelectionListeners.size(); ++i) {
            keySelectionListeners.get(i).keySelected(newKey);
        }
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keySelectionListeners.add(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keySelectionListeners.remove(listener);
    }

    /** @see KeyedStateBackend */
    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    /** @see KeyedStateBackend */
    @Override
    public K getCurrentKey() {
        return this.keyContext.getCurrentKey();
    }

    /** @see KeyedStateBackend */
    public int getCurrentKeyGroupIndex() {
        return this.keyContext.getCurrentKeyGroupIndex();
    }

    /** @see KeyedStateBackend */
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    /** @see KeyedStateBackend */
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    /** @see KeyedStateBackend */
    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception {

        applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                function,
                this::getPartitionedState);
    }


    // 对所有 Key 应用函数。
    // 提供了对所有 Key Group 中所有活跃 Key 执行自定义函数的能力。
    // 它首先获取 Key 流，然后迭代地调用 setCurrentKey 和 function.process。
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            keyStream.forEach(
                    (K key) -> {
                        setCurrentKey(key);
                        try {
                            function.process(key, state);
                        } catch (Throwable e) {
                            // we wrap the checked exception in an unchecked
                            // one and catch it (and re-throw it) later.
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
    // 负责实例化或检索一个状态实例。
    //
    /** @see KeyedStateBackend */
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, V> S getOrCreateKeyedState(
            final TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        checkNotNull(namespaceSerializer, "Namespace serializer");
        checkNotNull(
                keySerializer,
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");

        InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());
        if (kvState == null) {
            if (!stateDescriptor.isSerializerInitialized()) {
                stateDescriptor.initializeSerializerUnlessSet(executionConfig);
            }
            kvState =
                    LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                            TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                                    namespaceSerializer, stateDescriptor, this, ttlTimeProvider),
                            stateDescriptor,
                            latencyTrackingStateConfig);
            keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            // 发布可查询状态。
            // 如果 StateDescriptor 标记为可查询（Queryable），则将状态注册到 kvStateRegistry，使其可供外部查询。
            publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }
        return (S) kvState;
    }

    public void publishQueryableStateIfEnabled(
            StateDescriptor<?, ?> stateDescriptor, InternalKvState<?, ?, ?> kvState) {
        if (stateDescriptor.isQueryable()) {
            if (kvStateRegistry == null) {
                throw new IllegalStateException("State backend has not been initialized for job.");
            }
            String name = stateDescriptor.getQueryableStateName();
            kvStateRegistry.registerKvState(keyGroupRange, name, kvState, userCodeClassLoader);
        }
    }

    /**
     * TODO: NOTE: This method does a lot of work caching / retrieving states just to update the
     * namespace. This method should be removed for the sake of namespaces being lazily fetched from
     * the keyed state backend, or being set on the state directly.
     *
     * @see KeyedStateBackend
     */
    @SuppressWarnings("unchecked")
    @Override
    public <N, S extends State> S getPartitionedState(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, ?> stateDescriptor)
            throws Exception {

        checkNotNull(namespace, "Namespace");

        if (lastName != null && lastName.equals(stateDescriptor.getName())) {
            lastState.setCurrentNamespace(namespace);
            return (S) lastState;
        }

        InternalKvState<K, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
        if (previous != null) {
            lastState = previous;
            lastState.setCurrentNamespace(namespace);
            lastName = stateDescriptor.getName();
            return (S) previous;
        }

        final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

        lastName = stateDescriptor.getName();
        lastState = kvState;
        kvState.setCurrentNamespace(namespace);

        return state;
    }

    @Override
    public void close() throws IOException {
        cancelStreamRegistry.close();
    }

    public LatencyTrackingStateConfig getLatencyTrackingStateConfig() {
        return latencyTrackingStateConfig;
    }

    @VisibleForTesting
    public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
        return keyGroupCompressionDecorator;
    }

    @VisibleForTesting
    public int numKeyValueStatesByName() {
        return keyValueStatesByName.size();
    }

    // TODO remove this once heap-based timers are working with RocksDB incremental snapshots!
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return false;
    }

    public InternalKeyContext<K> getKeyContext() {
        return keyContext;
    }

    public interface PartitionStateFactory {
        <N, S extends State> S get(
                final N namespace,
                final TypeSerializer<N> namespaceSerializer,
                final StateDescriptor<S, ?> stateDescriptor)
                throws Exception;
    }

    @Override
    public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
        keyContext.setCurrentKeyGroupIndex(currentKeyGroupIndex);
    }
}
