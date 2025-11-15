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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key,
 * as provided through the {@link InternalKeyContext}.
 *
 * @param <K> type of key  <K>: 键的类型
 * @param <N> type of namespace  <N>: 命名空间的类型
 * @param <S> type of state  <S>: 状态的类型
 */
// StateTable 是 Flink 堆状态后端用来在 JVM 内存中组织和管理键控状态数据的基类。
// 它的设计旨在将状态数据根据 Key Group 进行划分，并提供高效的基于 Key/Namespace 的存取操作。
// 分片存储： 它是一个 Keyed State 的逻辑表示，其内部数据结构是一个数组，每个数组元素对应 Flink Key Group 范围内的一个 Key Group 分片。
// Key 上下文依赖： 它的大多数操作（get、put、remove）都依赖于当前 KeyedStateBackend 设置的活跃 Key 上下文 (keyContext)
// StateTable 是在 Flink 内存中存储和操作 Keyed State 数据的核心抽象，它将 Keyed State 的逻辑视图映射到物理 Key Group 存储结构中。

// **<S>**	状态的类型 (State value)。
public abstract class StateTable<K, N, S>
        implements StateSnapshotRestore, Iterable<StateEntry<K, N, S>> {

    /**
     * The key context view on the backend. This provides information, such as the currently active
     * key. 提供键作用域上下文，包括当前活跃的键以及键组范围信息
     */
    protected final InternalKeyContext<K> keyContext;

    /** Combined meta information such as name and serializers for this state. */
    // 状态元信息。 包含了该状态的名称、命名空间序列化器和状态值序列化器等关键元数据。
    protected RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

    /** The serializer of the key.*/
    // 键序列化器。
    // 用于序列化和反序列化 Key。
    protected final TypeSerializer<K> keySerializer;

    /** The current key group range.*/
    // Key Group 范围。
    // 当前 StateTable 实例负责维护的 Key Group 编号范围。
    protected final KeyGroupRange keyGroupRange;

    /**
     * Map for holding the actual state objects. The outer array represents the key-groups. All
     * array positions will be initialized with an empty state map.
     */
    // 分 Key Group 状态映射数组。
    // 这是一个数组，数组的每个元素都是一个抽象的 StateMap，代表一个 Key Group 的状态数据存储。
    // 这是状态数据在内存中的物理存储结构。
    protected final StateMap<K, N, S>[] keyGroupedStateMaps;

    /**
     * @param keyContext the key context provides the key scope for all put/get/delete operations.
     * @param metaInfo the meta information, including the type serializer for state copy-on-write.
     * @param keySerializer the serializer of the key.
     */
    public StateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.metaInfo = Preconditions.checkNotNull(metaInfo);
        this.keySerializer = Preconditions.checkNotNull(keySerializer);

        this.keyGroupRange = keyContext.getKeyGroupRange();

        @SuppressWarnings("unchecked")
        StateMap<K, N, S>[] state =
                (StateMap<K, N, S>[])
                        new StateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
        this.keyGroupedStateMaps = state;
        for (int i = 0; i < this.keyGroupedStateMaps.length; i++) {
            this.keyGroupedStateMaps[i] = createStateMap();
        }
    }

    protected abstract StateMap<K, N, S> createStateMap();

    @Override
    @Nonnull
    public abstract IterableStateSnapshot<K, N, S> stateSnapshot();

    // Main interface methods of StateTable -------------------------------------------------------

    /**
     * Returns whether this {@link StateTable} is empty.
     *
     * @return {@code true} if this {@link StateTable} has no elements, {@code false} otherwise.
     * @see #size()
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the total number of entries in this {@link StateTable}. This is the sum of both
     * sub-tables.
     *
     * @return the number of entries in this {@link StateTable}.
     */
    public int size() {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.size();
        }
        return count;
    }

    /**
     * Returns the state of the mapping for the composite of active key and given namespace.
     * @param namespace the namespace. Not null.
     * @return the states of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    // 获取状态值。
    // 根据 当前 Key (keyContext.getCurrentKey()) 和给定的 namespace，获取对应的状态值。
    public S get(N namespace) {
        return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Returns whether this table contains a mapping for the composite of active key and given
     * namespace.
     *
     * @param namespace the namespace in the composite key to search for. Not null.
     * @return {@code true} if this map contains the specified key/namespace composite key, {@code
     *     false} otherwise.
     */
    // 检查是否存在。
    // 检查当前 Key 和给定 Namespace 的组合是否存在状态条目。
    public boolean containsKey(N namespace) {
        return containsKey(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Maps the composite of active key and given namespace to the specified state.
     *
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     */
    // 写入状态值。
    // 根据 当前 Key 和给定的 namespace，写入或更新状态值 state。
    public void put(N namespace, S state) {
        put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace. This method should
     * be preferred over {@link #removeAndGetOld(N)} when the caller is not interested in the old
     * state.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     */
    // 移除状态。
    // 根据 当前 Key 和给定的 namespace，移除对应的状态条目。
    public void remove(N namespace) {
        remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace, returning the state
     * that was found under the entry.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     * @return the state of the removed mapping or {@code null} if no mapping for the specified key
     *     was found.
     */
    // 移除并获取旧状态。
    // 移除状态条目并返回被移除的旧状态值。
    public S removeAndGetOld(N namespace) {
        return removeAndGetOld(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Applies the given {@link StateTransformationFunction} to the state (1st input argument),
     * using the given value as second input argument. The result of {@link
     * StateTransformationFunction#apply(Object, Object)} is then stored as the new state. This
     * function is basically an optimization for get-update-put pattern.
     *
     * @param namespace the namespace. Not null.
     * @param value the value to use in transforming the state. Can be null.
     * @param transformation the transformation function.
     * @throws Exception if some exception happens in the transformation function.
     */
    // 状态转换。
    // 获取当前状态值，使用 transformation 函数和输入 value 进行计算，然后将结果存回为新的状态值。
    // 这优化了“获取-修改-写入”的模式。
    public <T> void transform(
            N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        K key = keyContext.getCurrentKey();
        checkKeyNamespacePreconditions(key, namespace);

        int keyGroup = keyContext.getCurrentKeyGroupIndex();
        getMapForKeyGroup(keyGroup).transform(key, namespace, value, transformation);
    }

    // For queryable state ------------------------------------------------------------------------

    /**
     * Returns the state for the composite of active key and given namespace. This is typically used
     * by queryable state.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @return the state of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    // 根据key 和 namespacke 获取对应的状态值，key group index 是根据key 计算出来的
    public S get(K key, N namespace) {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
        return get(key, keyGroup, namespace);
    }
    // 获取 Key 流。
    // 返回一个 Stream，包含在指定 namespace 下所有 Key Group 中存在的 Key。
    // 主要用于 Keyed State Backend 的 Key 遍历功能。
    public Stream<K> getKeys(N namespace) {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .filter(entry -> entry.getNamespace().equals(namespace))
                .map(StateEntry::getKey);
    }
    // 获取 Key 和 Namespace 对的流。 返回一个 Stream，包含所有活跃的 (Key, Namespace) 对。
    public Stream<Tuple2<K, N>> getKeysAndNamespaces() {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .map(entry -> Tuple2.of(entry.getKey(), entry.getNamespace()));
    }

    // 获取增量状态访问器。
    // 返回一个增量状态访问器 (StateIncrementalVisitor)，用于在增量 Checkpoint 中高效地访问和迭代状态条目
    public StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return new StateEntryIterator(recommendedMaxNumberOfReturnedRecords);
    }

    // ------------------------------------------------------------------------
    // 找到key group index 对应的StateMap，然后根据key 和 namespace 获取对应的状态
    private S get(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).get(key, namespace);
    }

    private boolean containsKey(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).containsKey(key, namespace);
    }
    // 检查 key 和 namespace 非空
    private void checkKeyNamespacePreconditions(K key, N namespace) {
        Preconditions.checkNotNull(
                key, "No key set. This method should not be called outside of a keyed context.");
        Preconditions.checkNotNull(namespace, "Provided namespace is null.");
    }

    private void remove(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        getMapForKeyGroup(keyGroupIndex).remove(key, namespace);
    }

    private S removeAndGetOld(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).removeAndGetOld(key, namespace);
    }

    // ------------------------------------------------------------------------
    //  access to maps
    // ------------------------------------------------------------------------

    /** Returns the internal data structure. */
    @VisibleForTesting
    public StateMap<K, N, S>[] getState() {
        return keyGroupedStateMaps;
    }
    // 获取key group的起始范围id
    public int getKeyGroupOffset() {
        return keyGroupRange.getStartKeyGroup();
    }
    // 找到key group index 对应的StateMap存储
    @VisibleForTesting
    public StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
        final int pos = indexToOffset(keyGroupIndex);
        if (pos >= 0 && pos < keyGroupedStateMaps.length) {
            return keyGroupedStateMaps[pos];
        } else {
            throw KeyGroupRangeOffsets.newIllegalKeyGroupException(keyGroupIndex, keyGroupRange);
        }
    }

    /** Translates a key-group id to the internal array offset. */
    // 返回key group id在数组中的偏移量
    private int indexToOffset(int index) {
        return index - getKeyGroupOffset();
    }

    // Meta data setter / getter and toString -----------------------------------------------------

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<S> getStateSerializer() {
        return metaInfo.getStateSerializer();
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return metaInfo.getNamespaceSerializer();
    }

    public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
        return metaInfo;
    }

    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        this.metaInfo = metaInfo;
    }

    // Snapshot / Restore -------------------------------------------------------------------------

    public void put(K key, int keyGroup, N namespace, S state) {
        checkKeyNamespacePreconditions(key, namespace);
        getMapForKeyGroup(keyGroup).put(key, namespace, state);
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return Arrays.stream(keyGroupedStateMaps)
                .filter(Objects::nonNull)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .iterator();
    }

    // For testing --------------------------------------------------------------------------------

    @VisibleForTesting
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.sizeOfNamespace(namespace);
        }

        return count;
    }

    @Nonnull
    @Override
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersion) {
        return StateTableByKeyGroupReaders.readerForVersion(this, readVersion);
    }

    // StateEntryIterator
    // ---------------------------------------------------------------------------------------------

    class StateEntryIterator implements StateIncrementalVisitor<K, N, S> {

        final int recommendedMaxNumberOfReturnedRecords;

        int keyGroupIndex;

        StateIncrementalVisitor<K, N, S> stateIncrementalVisitor;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords) {
            this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
            this.keyGroupIndex = 0;
            next();
        }

        private void next() {
            while (keyGroupIndex < keyGroupedStateMaps.length) {
                StateMap<K, N, S> stateMap = keyGroupedStateMaps[keyGroupIndex++];
                StateIncrementalVisitor<K, N, S> visitor =
                        stateMap.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            while (stateIncrementalVisitor == null || !stateIncrementalVisitor.hasNext()) {
                if (keyGroupIndex == keyGroupedStateMaps.length) {
                    return false;
                }
                StateIncrementalVisitor<K, N, S> visitor =
                        keyGroupedStateMaps[keyGroupIndex++].getStateIncrementalVisitor(
                                recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    break;
                }
            }
            return true;
        }

        @Override
        public Collection<StateEntry<K, N, S>> nextEntries() {
            if (!hasNext()) {
                return null;
            }

            return stateIncrementalVisitor.nextEntries();
        }

        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            keyGroupedStateMaps[keyGroupIndex - 1].remove(
                    stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            keyGroupedStateMaps[keyGroupIndex - 1].put(
                    stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }
    }
}
