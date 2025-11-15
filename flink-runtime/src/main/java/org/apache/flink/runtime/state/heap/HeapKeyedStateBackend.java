/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link CheckpointStreamFactory} upon checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
// HeapKeyedStateBackend 是 Apache Flink 中最基础且最常用的键控状态后端实现之一
// HeapKeyedStateBackend 的核心作用是作为 Flink **Keyed State（键控状态）**的存储和管理机制
// 内存存储 (In-Memory Storage): 它将所有的键控状态数据（如 ValueState, ListState, MapState 等）保存在 **Java 堆内存（Java Heap）**中。
// 键组（Key-Group）管理: 它负责将状态数据根据 Key Group 进行逻辑分组，这是 Flink 实现扩缩容和状态分区的基础。
// Checkpointing 和 Savepointing: 它提供了一套机制，在执行 Checkpoint 或 Savepoint 时，将内存中的状态数据序列化（通常是增量或全量）到外部存储（如 HDFS、S3）中，实现容错。
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);
    // 储了所有支持的状态类型（VALUE, LIST, MAP, AGGREGATING, REDUCING）及其对应的**创建（Create）工厂方法（如 HeapValueState::create）
    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateCreateFactory) HeapValueState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateCreateFactory) HeapListState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateCreateFactory) HeapMapState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateCreateFactory) HeapAggregatingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateCreateFactory) HeapReducingState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    // 存储了所有支持的状态类型及其对应的更新（Update）工厂方法（如 HeapValueState::update）。
    // 用于在状态描述符发生变化时更新已存在的状态实例。
    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateUpdateFactory) HeapValueState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateUpdateFactory) HeapListState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateUpdateFactory) HeapMapState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateUpdateFactory) HeapAggregatingState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateUpdateFactory) HeapReducingState::update))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    /** Map of created Key/Value states. */
    // 已创建的 Key/Value 状态实例映射。
    // 存储了 Flink 任务中实际创建的、可供用户访问的状态对象（如 ValueState<Integer>），键是状态名称。
    private final Map<String, State> createdKVStates;

    /** Map of registered Key/Value states. */
    // 已注册的 Key/Value 状态表映射。
    // 存储了所有已注册的 StateTable 实例，每个 StateTable 负责存储一个状态（如 MyCountState）的所有键和命名空间下的数据。
    // 这是 Heap 后端实际存储数据的核心结构。
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

    /** The configuration for local recovery. */
    // 本地恢复配置。
    // 定义了在 Task 失败时，如何尝试从 TaskManager 节点的本地文件系统恢复状态的策略和路径配置。
    private final LocalRecoveryConfig localRecoveryConfig;

    /** The snapshot strategy for this backend. */
    // Checkpoint 策略。
    // 定义了执行 Checkpoint 或 Savepoint 的具体逻辑（例如，如何遍历状态表并将其写入流中）。
    // 对于 Heap 后端，这通常是 HeapSnapshotStrategy 的实例。
    private final SnapshotStrategy<KeyedStateHandle, ?> checkpointStrategy;

    // 快照执行类型。
    // 定义了快照执行是在同步还是异步**模式下进行（通常是异步）
    private final SnapshotExecutionType snapshotExecutionType;
    // 状态表工厂。
    // 用于创建新的 StateTable 实例。
    private final StateTableFactory<K> stateTableFactory;

    /** Factory for state that is organized as priority queue. */
    // 优先级队列管理器。
    // 负责管理和创建基于堆实现的内部优先级队列（如用于定时器服务）。
    private final HeapPriorityQueuesManager priorityQueuesManager;

    public HeapKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            HeapSnapshotStrategy<K> checkpointStrategy,
            SnapshotExecutionType snapshotExecutionType,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);
        this.registeredKVStates = registeredKVStates;
        this.createdKVStates = new HashMap<>();
        this.localRecoveryConfig = localRecoveryConfig;
        this.checkpointStrategy = checkpointStrategy;
        this.snapshotExecutionType = snapshotExecutionType;
        this.stateTableFactory = stateTableFactory;
        this.priorityQueuesManager =
                new HeapPriorityQueuesManager(
                        registeredPQStates,
                        priorityQueueSetFactory,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups());
        LOG.info("Initializing heap keyed state backend with stream factory.");
    }

    // ------------------------------------------------------------------------
    //  state backend operations
    // ------------------------------------------------------------------------

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return priorityQueuesManager.createOrUpdate(stateName, byteOrderedElementSerializer);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        return priorityQueuesManager.createOrUpdate(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    private <N, V> StateTable<K, N, V> tryRegisterStateTable(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<?, V> stateDesc,
            @Nonnull StateSnapshotTransformFactory<V> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws StateMigrationException {

        @SuppressWarnings("unchecked")
        StateTable<K, N, V> stateTable =
                (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

        TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

        if (stateTable != null) {
            RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo =
                    stateTable.getMetaInfo();

            restoredKvMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<N> previousNamespaceSerializer =
                    restoredKvMetaInfo.getNamespaceSerializer();

            TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
                    restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
            if (namespaceCompatibility.isCompatibleAfterMigration()
                    || namespaceCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For heap backends, the new namespace serializer ("
                                + namespaceSerializer
                                + ") must be compatible with the old namespace serializer ("
                                + previousNamespaceSerializer
                                + ").");
            }

            restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<V> previousStateSerializer = restoredKvMetaInfo.getStateSerializer();

            TypeSerializerSchemaCompatibility<V> stateCompatibility =
                    restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For heap backends, the new state serializer ("
                                + newStateSerializer
                                + ") must not be incompatible with the old state serializer ("
                                + previousStateSerializer
                                + ").");
            }

            restoredKvMetaInfo =
                    allowFutureMetadataUpdates
                            ? restoredKvMetaInfo.withSerializerUpgradesAllowed()
                            : restoredKvMetaInfo;

            stateTable.setMetaInfo(restoredKvMetaInfo);
        } else {
            RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getType(),
                            stateDesc.getName(),
                            namespaceSerializer,
                            newStateSerializer,
                            snapshotTransformFactory);

            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;

            stateTable = stateTableFactory.newStateTable(keyContext, newMetaInfo, keySerializer);
            registeredKVStates.put(stateDesc.getName(), stateTable);
        }

        return stateTable;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeys(namespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeysAndNamespaces();
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        StateTable<K, N, SV> stateTable =
                tryRegisterStateTable(
                        namespaceSerializer,
                        stateDesc,
                        getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory),
                        allowFutureMetadataUpdates);

        @SuppressWarnings("unchecked")
        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState =
                    stateCreateFactory.createState(stateDesc, stateTable, getKeySerializer());
        } else {
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateUpdateFactory.updateState(stateDesc, stateTable, createdState);
        }

        createdKVStates.put(stateDesc.getName(), createdState);
        return createdState;
    }

    private <S extends State, SV> String stateNotSupportedMessage(
            StateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV> StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
            StateDescriptor<?, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
        if (stateDesc instanceof ListStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else if (stateDesc instanceof MapStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else {
            return (StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
        }
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        SnapshotStrategyRunner<KeyedStateHandle, ?> snapshotStrategyRunner =
                new SnapshotStrategyRunner<>(
                        "Heap backend snapshot",
                        checkpointStrategy,
                        cancelStreamRegistry,
                        snapshotExecutionType);
        return snapshotStrategyRunner.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {

        HeapSnapshotResources<K> snapshotResources =
                HeapSnapshotResources.create(
                        registeredKVStates,
                        priorityQueuesManager.getRegisteredPQStates(),
                        keyGroupCompressionDecorator,
                        keyGroupRange,
                        keySerializer,
                        numberOfKeyGroups);

        return new SavepointResources<>(snapshotResources, snapshotExecutionType);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Nothing to do
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // nothing to do
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            // we copy the keys into list to avoid the concurrency problem
            // when state.clear() is invoked in function.process().
            final List<K> keys = keyStream.collect(Collectors.toList());

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            for (K key : keys) {
                setCurrentKey(key);
                function.process(key, state);
            }
        }
    }

    @Override
    public String toString() {
        return "HeapKeyedStateBackend";
    }

    /** Returns the total number of state entries across all keys/namespaces. */
    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int sum = 0;
        for (StateSnapshotRestore state : registeredKVStates.values()) {
            sum += ((StateTable<?, ?, ?>) state).size();
        }
        return sum;
    }

    /** Returns the total number of state entries across all keys for the given namespace. */
    @VisibleForTesting
    public int numKeyValueStateEntries(Object namespace) {
        int sum = 0;
        for (StateTable<?, ?, ?> state : registeredKVStates.values()) {
            sum += state.sizeOfNamespace(namespace);
        }
        return sum;
    }

    @VisibleForTesting
    public LocalRecoveryConfig getLocalRecoveryConfig() {
        return localRecoveryConfig;
    }

    private interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                StateTable<K, N, SV> stateTable,
                TypeSerializer<K> keySerializer)
                throws Exception;
    }

    private interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState)
                throws Exception;
    }
}
