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
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;

/**
 * Implementation of Flink's in-memory state maps with copy-on-write support. This map does not
 * support null values for key or namespace.
 *
 * <p>{@link CopyOnWriteStateMap} sacrifices some peak performance and memory efficiency for
 * features like incremental rehashing and asynchronous snapshots through copy-on-write.
 * Copy-on-write tries to minimize the amount of copying by maintaining version meta data for both,
 * the map structure and the state objects. However, we must often proactively copy state objects
 * when we hand them to the user.
 *
 * <p>As for any state backend, user should not keep references on state objects that they obtained
 * from state backends outside the scope of the user function calls.
 *
 * <p>Some brief maintenance notes:
 *
 * <p>1) Flattening the underlying data structure from nested maps (namespace) -> (key) -> (state)
 * to one flat map (key, namespace) -> (state) brings certain performance trade-offs. In theory, the
 * flat map has one less level of indirection compared to the nested map. However, the nested map
 * naturally de-duplicates namespace objects for which #equals() is true. This leads to potentially
 * a lot of redundant namespace objects for the flattened version. Those, in turn, can again
 * introduce more cache misses because we need to follow the namespace object on all operations to
 * ensure entry identities. Obviously, copy-on-write can also add memory overhead. So does the meta
 * data to track copy-on-write requirement (state and entry versions on {@link StateMapEntry}).
 *
 * <p>2) A flat map structure is a lot easier when it comes to tracking copy-on-write of the map
 * structure.
 *
 * <p>3) Nested structure had the (never used) advantage that we can easily drop and iterate whole
 * namespaces. This could give locality advantages for certain access pattern, e.g. iterating a
 * namespace.
 *
 * <p>4) Serialization format is changed from namespace-prefix compressed (as naturally provided
 * from the old nested structure) to making all entries self contained as (key, namespace, state).
 *
 * <p>5) Currently, a state map can only grow, but never shrinks on low load. We could easily add
 * this if required.
 *
 * <p>6) Heap based state backends like this can easily cause a lot of GC activity. Besides using G1
 * as garbage collector, we should provide an additional state backend that operates on off-heap
 * memory. This would sacrifice peak performance (due to de/serialization of objects) for a lower,
 * but more constant throughput and potentially huge simplifications w.r.t. copy-on-write.
 *
 * <p>7) We could try a hybrid of a serialized and object based backends, where key and namespace of
 * the entries are both serialized in one byte-array.
 *
 * <p>9) We could consider smaller types (e.g. short) for the version counting and think about some
 * reset strategy before overflows, when there is no snapshot running. However, this would have to
 * touch all entries in the map.
 *
 * <p>This class was initially based on the {@link java.util.HashMap} implementation of the Android
 * JDK, but is now heavily customized towards the use case of map for state entries. IMPORTANT: the
 * contracts for this class rely on the user not holding any references to objects returned by this
 * map beyond the life cycle of per-element operations. Or phrased differently, all get-update-put
 * operations on a mapping should be within one call of processElement. Otherwise, the user must
 * take care of taking deep copies, e.g. for caching purposes.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of value.
 */
// CopyOnWriteStateMap<K, N, S> 是 Flink Heap State Backend 中 用于保存 Keyed State 的核心数据结构 之一（用于非 TTL、非 MapState 的普通键控状态）
// 当 Flink 做 checkpoint 时，后台线程需要从 StateMap 中读取快照数据，但是 此时主线程仍在修改状态（put/remove/update）
// 为了保证 snapshot 的一致性，CopyOnWriteStateMap 使用版本号：
// stateMapVersion — 当前 map 的结构版本（结构变化：扩容、增删 Entry）
// entryVersion — Entry 节点自身的版本（链节点）
// stateVersion — Entry 内部 state 对象的版本（state 值）
// 当 snapshot 开始时：
// snapshotVersion = ++stateMapVersion
// highestRequiredSnapshotVersion = snapshotVersion
// 之后所有 put/remove/update 都会检查：
// 如果某个 entryVersion < highestRequiredSnapshotVersion
//    → 必须复制 entry（copy-on-write）
//如果 stateVersion < snapshotVersion
//    → 必须复制 state 对象（copy-on-write）
// 支持增量 rehash（incremental rehash）
public class CopyOnWriteStateMap<K, N, S> extends StateMap<K, N, S> {

    /** The logger. */
    private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteStateMap.class);

    /**
     * Min capacity (other than zero) for a {@link CopyOnWriteStateMap}. Must be a power of two
     * greater than 1 (and less than 1 << 30).
     */
    // 状态映射的最小容量
    private static final int MINIMUM_CAPACITY = 4;

    /**
     * Max capacity for a {@link CopyOnWriteStateMap}. Must be a power of two >= MINIMUM_CAPACITY.
     */
    // 状态映射的最大容量
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * Default capacity for a {@link CopyOnWriteStateMap}. Must be a power of two, greater than
     * {@code MINIMUM_CAPACITY} and less than {@code MAXIMUM_CAPACITY}.
     */
    // 默认的容量
    public static final int DEFAULT_CAPACITY = 128;

    /**
     * Minimum number of entries that one step of incremental rehashing migrates from the old to the
     * new sub-map.
     */
    // 每次重hash 最小的实体数量
    private static final int MIN_TRANSFERRED_PER_INCREMENTAL_REHASH = 4;

    /** The serializer of the state. */
    // 序列化 state 值 S 用，checkpoint 时需要 copy state。
    protected TypeSerializer<S> stateSerializer;

    /**
     * An empty map shared by all zero-capacity maps (typically from default constructor). It is
     * never written to, and replaced on first put. Its size is set to half the minimum, so that the
     * first resize will create a minimum-sized map.
     */
    private static final StateMapEntry<?, ?, ?>[] EMPTY_TABLE =
            new StateMapEntry[MINIMUM_CAPACITY >>> 1];

    /** Empty entry that we use to bootstrap our {@link CopyOnWriteStateMap.StateEntryIterator}. */
    private static final StateMapEntry<?, ?, ?> ITERATOR_BOOTSTRAP_ENTRY =
            new StateMapEntry<>(new Object(), new Object(), new Object(), 0, null, 0, 0);

    /** Maintains an ordered set of version ids that are still in use by unreleased snapshots. */
    // 存当前系统中 正在使用的 snapshot 的版本号。
    private final TreeSet<Integer> snapshotVersions;

    /**
     * This is the primary entry array (hash directory) of the state map. If no incremental rehash
     * is ongoing, this is the only used table.
     */
    // 状态的主存储表
    private StateMapEntry<K, N, S>[] primaryTable;

    /**
     * We maintain a secondary entry array while performing an incremental rehash. The purpose is to
     * slowly migrate entries from the primary table to this resized table array. When all entries
     * are migrated, this becomes the new primary table.
     */
    // 状态的增量存储表
    private StateMapEntry<K, N, S>[] incrementalRehashTable;

    /** The current number of mappings in the primary talbe. */
    // primaryTable 中 entry 数量。
    private int primaryTableSize;

    /** The current number of mappings in the rehash table. */
    // rehash 过程中，新表 entry 数量。
    private int incrementalRehashTableSize;

    /** The next index for a step of incremental rehashing in the primary table. */
    // 渐进式 rehash 的指针。
    // 每次 hash 操作都推动 rehash
    // 每次移动若干 bucket（至少 4 个 entry）
    private int rehashIndex;

    /** The current version of this map. Used for copy-on-write mechanics. */
    // 当前版本号
    // 每次对 Map 进行写入操作（put, remove）时，这个版本号都会增加，代表 Map 结构的一次更新。
    private int stateMapVersion;

    /** The highest version of this map that is still required by any unreleased snapshot. */
    // 这是当前正在进行的或仍未完成的快照所需要的最老的版本号。
    // 所有小于此版本号的数据，都被认为是被某个正在进行的快照引用的**“旧数据”**，必须保持不变。
    private int highestRequiredSnapshotVersion;

    /**
     * The last namespace that was actually inserted. This is a small optimization to reduce
     * duplicate namespace objects.
     */
    // 如果连续插入同一个 namespace，则重用该对象
    private N lastNamespace;

    /**
     * The {@link CopyOnWriteStateMap} is rehashed when its size exceeds this threshold. The value
     * of this field is generally .75 * capacity, except when the capacity is zero, as described in
     * the EMPTY_TABLE declaration above.
     */
    // 扩容阈值 = capacity * 0.75
    private int threshold;

    /**
     * Incremented by "structural modifications" to allow (best effort) detection of concurrent
     * modification.
     */
    // 结构修改计数，用于 fail-fast 迭代器检测并发修改。
    private int modCount;

    /**
     * Constructs a new {@code StateMap} with default capacity of {@code DEFAULT_CAPACITY}.
     *
     * @param stateSerializer the serializer of the key.
     */
    CopyOnWriteStateMap(TypeSerializer<S> stateSerializer) {
        this(DEFAULT_CAPACITY, stateSerializer);
    }

    /**
     * Constructs a new {@code StateMap} instance with the specified capacity.
     *
     * @param capacity the initial capacity of this hash map.
     * @param stateSerializer the serializer of the key.
     * @throws IllegalArgumentException when the capacity is less than zero.
     */
    @SuppressWarnings("unchecked")
    private CopyOnWriteStateMap(int capacity, TypeSerializer<S> stateSerializer) {
        this.stateSerializer = Preconditions.checkNotNull(stateSerializer);

        // initialized maps to EMPTY_TABLE.
        this.primaryTable = (StateMapEntry<K, N, S>[]) EMPTY_TABLE;
        this.incrementalRehashTable = (StateMapEntry<K, N, S>[]) EMPTY_TABLE;

        // initialize sizes to 0.
        this.primaryTableSize = 0;
        this.incrementalRehashTableSize = 0;

        this.rehashIndex = 0;
        this.stateMapVersion = 0;
        this.highestRequiredSnapshotVersion = 0;
        this.snapshotVersions = new TreeSet<>();

        if (capacity < 0) {
            throw new IllegalArgumentException("Capacity: " + capacity);
        }

        if (capacity == 0) {
            threshold = -1;
            return;
        }

        if (capacity < MINIMUM_CAPACITY) {
            capacity = MINIMUM_CAPACITY;
        } else if (capacity > MAXIMUM_CAPACITY) {
            capacity = MAXIMUM_CAPACITY;
        } else {
            capacity = MathUtils.roundUpToPowerOfTwo(capacity);
        }
        primaryTable = makeTable(capacity);
    }

    // Public API from StateMap
    // ------------------------------------------------------------------------------

    /**
     * Returns the total number of entries in this {@link CopyOnWriteStateMap}. This is the sum of
     * both sub-maps.
     *
     * @return the number of entries in this {@link CopyOnWriteStateMap}.
     */
    // 返回 primary + incremental 两个表的 entry 数。
    @Override
    public int size() {
        return primaryTableSize + incrementalRehashTableSize;
    }

    @Override
    public S get(K key, N namespace) {

        final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
        final int requiredVersion = highestRequiredSnapshotVersion;
        final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
        int index = hash & (tab.length - 1);

        for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
            final K eKey = e.key;
            final N eNamespace = e.namespace;
            if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {

                // copy-on-write check for state
                if (e.stateVersion < requiredVersion) {
                    // copy-on-write check for entry
                    if (e.entryVersion < requiredVersion) {
                        e = handleChainedEntryCopyOnWrite(tab, hash & (tab.length - 1), e);
                    }
                    e.stateVersion = stateMapVersion;
                    e.state = getStateSerializer().copy(e.state);
                }

                return e.state;
            }
        }

        return null;
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
        final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
        int index = hash & (tab.length - 1);

        for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
            final K eKey = e.key;
            final N eNamespace = e.namespace;

            if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void put(K key, N namespace, S value) {
        final StateMapEntry<K, N, S> e = putEntry(key, namespace);

        e.state = value;
        e.stateVersion = stateMapVersion;
    }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        final StateMapEntry<K, N, S> e = putEntry(key, namespace);

        // copy-on-write check for state
        S oldState =
                (e.stateVersion < highestRequiredSnapshotVersion)
                        ? getStateSerializer().copy(e.state)
                        : e.state;

        e.state = state;
        e.stateVersion = stateMapVersion;

        return oldState;
    }

    @Override
    public void remove(K key, N namespace) {
        removeEntry(key, namespace);
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {

        final StateMapEntry<K, N, S> e = removeEntry(key, namespace);

        return e != null
                ?
                // copy-on-write check for state
                (e.stateVersion < highestRequiredSnapshotVersion
                        ? getStateSerializer().copy(e.state)
                        : e.state)
                : null;
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        return StreamSupport.stream(spliterator(), false)
                .filter(entry -> entry.getNamespace().equals(namespace))
                .map(StateEntry::getKey);
    }

    @Override
    public <T> void transform(
            K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {

        final StateMapEntry<K, N, S> entry = putEntry(key, namespace);

        // copy-on-write check for state
        entry.state =
                transformation.apply(
                        (entry.stateVersion < highestRequiredSnapshotVersion)
                                ? getStateSerializer().copy(entry.state)
                                : entry.state,
                        value);
        entry.stateVersion = stateMapVersion;
    }

    // Private implementation details of the API methods
    // ---------------------------------------------------------------

    /** Helper method that is the basis for operations that add mappings. */
    // 该方法是一个底层辅助方法，是所有添加或更新映射（即 put 操作）的基础。
    // 它的核心在于查找现有条目、执行写时复制（COW）以及在找不到时添加新条目。
    private StateMapEntry<K, N, S> putEntry(K key, N namespace) {
        // 计算键和命名空间组合的哈希值 (hash)
        final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
        // 根据哈希值 hash 选择当前的活动哈希表
        final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
        int index = hash & (tab.length - 1);

        for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
            // 如果三者都相等，则找到了目标条目。
            if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

                // copy-on-write check for entry
                // COW 核心检查。如果找到的条目 e 的版本号 (entryVersion) 小于当前最高所需快照版本号
                if (e.entryVersion < highestRequiredSnapshotVersion) {
                    e = handleChainedEntryCopyOnWrite(tab, index, e);
                }

                return e;
            }
        }

        ++modCount;
        if (size() > threshold) {
            doubleCapacity();
        }

        return addNewStateMapEntry(tab, key, namespace, hash);
    }

    /** Helper method that is the basis for operations that remove mappings. */
    private StateMapEntry<K, N, S> removeEntry(K key, N namespace) {

        final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
        final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
        int index = hash & (tab.length - 1);

        for (StateMapEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
                if (prev == null) {
                    tab[index] = e.next;
                } else {
                    // copy-on-write check for entry
                    if (prev.entryVersion < highestRequiredSnapshotVersion) {
                        prev = handleChainedEntryCopyOnWrite(tab, index, prev);
                    }
                    prev.next = e.next;
                }
                ++modCount;
                if (tab == primaryTable) {
                    --primaryTableSize;
                } else {
                    --incrementalRehashTableSize;
                }
                return e;
            }
        }
        return null;
    }

    // Iteration
    // ------------------------------------------------------------------------------------------------------

    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return new StateEntryIterator();
    }

    // Private utility functions for StateMap management
    // -------------------------------------------------------------

    /** @see #releaseSnapshot(StateMapSnapshot) */
    @VisibleForTesting
    void releaseSnapshot(int snapshotVersion) {
        // we guard against concurrent modifications of highestRequiredSnapshotVersion between
        // snapshot and release.
        // Only stale reads of from the result of #releaseSnapshot calls are ok.
        synchronized (snapshotVersions) {
            Preconditions.checkState(
                    snapshotVersions.remove(snapshotVersion),
                    "Attempt to release unknown snapshot version");
            highestRequiredSnapshotVersion =
                    snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
        }
    }

    /**
     * Creates (combined) copy of the table arrays for a snapshot. This method must be called by the
     * same Thread that does modifications to the {@link CopyOnWriteStateMap}.
     */
    @VisibleForTesting
    @SuppressWarnings("unchecked")
    StateMapEntry<K, N, S>[] snapshotMapArrays() {

        // we guard against concurrent modifications of highestRequiredSnapshotVersion between
        // snapshot and release.
        // Only stale reads of from the result of #releaseSnapshot calls are ok. This is why we must
        // call this method
        // from the same thread that does all the modifications to the map.
        synchronized (snapshotVersions) {

            // increase the map version for copy-on-write and register the snapshot
            if (++stateMapVersion < 0) {
                // this is just a safety net against overflows, but should never happen in practice
                // (i.e., only after 2^31 snapshots)
                throw new IllegalStateException(
                        "Version count overflow in CopyOnWriteStateMap. Enforcing restart.");
            }

            highestRequiredSnapshotVersion = stateMapVersion;
            snapshotVersions.add(highestRequiredSnapshotVersion);
        }

        StateMapEntry<K, N, S>[] table = primaryTable;

        // In order to reuse the copied array as the destination array for the partitioned records
        // in
        // CopyOnWriteStateMapSnapshot.TransformedSnapshotIterator, we need to make sure that the
        // copied array
        // is big enough to hold the flattened entries. In fact, given the current rehashing
        // algorithm, we only
        // need to do this check when isRehashing() is false, but in order to get a more robust
        // code(in case that
        // the rehashing algorithm may changed in the future), we do this check for all the case.
        final int totalMapIndexSize = rehashIndex + table.length;
        final int copiedArraySize = Math.max(totalMapIndexSize, size());
        final StateMapEntry<K, N, S>[] copy = new StateMapEntry[copiedArraySize];

        if (isRehashing()) {
            // consider both maps for the snapshot, the rehash index tells us which part of the two
            // maps we need
            final int localRehashIndex = rehashIndex;
            final int localCopyLength = table.length - localRehashIndex;
            // for the primary table, take every index >= rhIdx.
            System.arraycopy(table, localRehashIndex, copy, 0, localCopyLength);

            // for the new table, we are sure that two regions contain all the entries:
            // [0, rhIdx[ AND [table.length / 2, table.length / 2 + rhIdx[
            table = incrementalRehashTable;
            System.arraycopy(table, 0, copy, localCopyLength, localRehashIndex);
            System.arraycopy(
                    table,
                    table.length >>> 1,
                    copy,
                    localCopyLength + localRehashIndex,
                    localRehashIndex);
        } else {
            // we only need to copy the primary table
            System.arraycopy(table, 0, copy, 0, table.length);
        }

        return copy;
    }

    int getStateMapVersion() {
        return stateMapVersion;
    }

    /**
     * Allocate a table of the given capacity and set the threshold accordingly.
     *
     * @param newCapacity must be a power of two
     */
    private StateMapEntry<K, N, S>[] makeTable(int newCapacity) {

        if (newCapacity < MAXIMUM_CAPACITY) {
            threshold = (newCapacity >> 1) + (newCapacity >> 2); // 3/4 capacity
        } else {
            if (size() > MAX_ARRAY_SIZE) {

                throw new IllegalStateException(
                        "Maximum capacity of CopyOnWriteStateMap is reached and the job "
                                + "cannot continue. Please consider scaling-out your job or using a different keyed state backend "
                                + "implementation!");
            } else {

                LOG.warn(
                        "Maximum capacity of 2^30 in StateMap reached. Cannot increase hash map size. This can "
                                + "lead to more collisions and lower performance. Please consider scaling-out your job or using a "
                                + "different keyed state backend implementation!");
                threshold = MAX_ARRAY_SIZE;
            }
        }

        @SuppressWarnings("unchecked")
        StateMapEntry<K, N, S>[] newMap = (StateMapEntry<K, N, S>[]) new StateMapEntry[newCapacity];
        return newMap;
    }

    /** Creates and inserts a new {@link StateMapEntry}. */
    // 该方法负责创建并插入一个新的状态条目（StateMapEntry）到哈希表中。
    // StateMapEntry<K, N, S>[] table: 目标哈希表数组，可以是主表 (primaryTable) 或增量再哈希表 (incrementalRehashTable)
    private StateMapEntry<K, N, S> addNewStateMapEntry(
            StateMapEntry<K, N, S>[] table, K key, N namespace, int hash) {

        // small optimization that aims to avoid holding references on duplicate namespace objects
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        // 通过按位与运算计算出新条目在目标数组 table 中的索引位置
        int index = hash & (table.length - 1);
        StateMapEntry<K, N, S> newEntry =
                new StateMapEntry<>(
                        key, namespace, null, hash, table[index], stateMapVersion, stateMapVersion);
        table[index] = newEntry;

        if (table == primaryTable) {
            ++primaryTableSize;
        } else {
            ++incrementalRehashTableSize;
        }
        return newEntry;
    }

    /**
     * Select the sub-table which is responsible for entries with the given hash code.
     *
     * @param hashCode the hash code which we use to decide about the table that is responsible.
     * @return the index of the sub-table that is responsible for the entry with the given hash
     *     code.
     */
    // 计算应该选择主表还是增量表插入值
    private StateMapEntry<K, N, S>[] selectActiveTable(int hashCode) {
        return (hashCode & (primaryTable.length - 1)) >= rehashIndex
                ? primaryTable
                : incrementalRehashTable;
    }

    /**
     * Doubles the capacity of the hash table. Existing entries are placed in the correct bucket on
     * the enlarged table. If the current capacity is, MAXIMUM_CAPACITY, this method is a no-op.
     * Returns the table, which will be new unless we were already at MAXIMUM_CAPACITY.
     */
    private void doubleCapacity() {

        // There can only be one rehash in flight. From the amount of incremental rehash steps we
        // take, this should always hold.
        Preconditions.checkState(!isRehashing(), "There is already a rehash in progress.");

        StateMapEntry<K, N, S>[] oldMap = primaryTable;

        int oldCapacity = oldMap.length;

        if (oldCapacity == MAXIMUM_CAPACITY) {
            return;
        }

        incrementalRehashTable = makeTable(oldCapacity * 2);
    }

    /** Returns true, if an incremental rehash is in progress. */
    // 如果增量hash表不为null，则表示已经重hash
    @VisibleForTesting
    boolean isRehashing() {
        // if we rehash, the secondary table is not empty
        return EMPTY_TABLE != incrementalRehashTable;
    }

    /**
     * Computes the hash for the composite of key and namespace and performs some steps of
     * incremental rehash if incremental rehashing is in progress.
     */
    private int computeHashForOperationAndDoIncrementalRehash(K key, N namespace) {

        if (isRehashing()) {
            incrementalRehash();
        }

        return compositeHash(key, namespace);
    }

    /** Runs a number of steps for incremental rehashing. */
    // incrementalRehash 方法的作用是增量地将旧的哈希表（primaryTable）中的键值对迁移到正在构建中的新哈希表（incrementalRehashTable），从而逐步完成哈希表扩容（Rehash）操作。
    // 这种增量操作避免了一次性迁移所有数据导致的长时间停顿。
    @SuppressWarnings("unchecked")
    private void incrementalRehash() {

        StateMapEntry<K, N, S>[] oldMap = primaryTable;
        StateMapEntry<K, N, S>[] newMap = incrementalRehashTable;

        int oldCapacity = oldMap.length;
        // 计算新表的掩码。
        // 由于哈希表的长度通常是 2 的幂次方，长度 - 1 可以用于通过按位与运算 (&) 快速计算出哈希值在新表中的索引位置：hash & newMask
        int newMask = newMap.length - 1;
        int requiredVersion = highestRequiredSnapshotVersion;
        // 将再哈希进度索引（下一个要处理的旧表数组索引）赋给局部变量 rhIdx
        int rhIdx = rehashIndex;
        // 初始化局部变量 transferred，用于计数本次增量操作中已迁移的条目数量。
        int transferred = 0;

        // we migrate a certain minimum amount of entries from the old to the new table
        while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {
            // 获取旧哈希表当前索引 rhIdx 处的链表头节点
            StateMapEntry<K, N, S> e = oldMap[rhIdx];

            while (e != null) {
                // copy-on-write check for entry
                // 检查当前条目的版本号（entryVersion）是否小于所需的最高快照版本号（requiredVersion）。
                // 如果小于，说明这个条目正在被一个较早的快照引用，是旧版本。
                if (e.entryVersion < requiredVersion) {
                    // 如果是旧版本，则执行写时复制：
                    // 创建一个新的 StateMapEntry 实例，复制旧条目的内容，并将其版本号更新为当前的 stateMapVersion。后续操作将针对这个新的条目。
                    e = new StateMapEntry<>(e, stateMapVersion);
                }
                StateMapEntry<K, N, S> n = e.next;
                int pos = e.hash & newMask;
                e.next = newMap[pos];
                newMap[pos] = e;
                e = n;
                ++transferred;
            }

            oldMap[rhIdx] = null;
            if (++rhIdx == oldCapacity) {
                // here, the rehash is complete and we release resources and reset fields
                primaryTable = newMap;
                incrementalRehashTable = (StateMapEntry<K, N, S>[]) EMPTY_TABLE;
                primaryTableSize += incrementalRehashTableSize;
                incrementalRehashTableSize = 0;
                rehashIndex = 0;
                return;
            }
        }

        // sync our local bookkeeping the with official bookkeeping fields
        primaryTableSize -= transferred;
        incrementalRehashTableSize += transferred;
        rehashIndex = rhIdx;
    }

    /**
     * Perform copy-on-write for entry chains. We iterate the (hopefully and probably) still cached
     * chain, replace all links up to the 'untilEntry', which we actually wanted to modify.
     */
    // 该方法是 链式写时复制 (Chained Copy-on-Write) 的核心实现。当我们在链表中找到一个需要修改的旧版本条目时，我们不能只复制这一个条目，
    // 还必须复制从链表头到该条目路径上的所有旧版本条目，以维护快照引用的一致性。
    // StateMapEntry<K, N, S>[] tab: 当前操作的哈希表数组（primaryTable 或 incrementalRehashTable
    // int mapIdx: 条目所在的数组索引。
    // StateMapEntry<K, N, S> untilEntry: 目标条目，即在 putEntry 中找到的、需要进行修改的那个条目。
    private StateMapEntry<K, N, S> handleChainedEntryCopyOnWrite(
            StateMapEntry<K, N, S>[] tab, int mapIdx, StateMapEntry<K, N, S> untilEntry) {

        final int required = highestRequiredSnapshotVersion;

        StateMapEntry<K, N, S> current = tab[mapIdx];
        StateMapEntry<K, N, S> copy;
        // 检查链表头节点 current 的版本号是否小于快照所需版本号。如果小于，说明头节点是旧版本，必须复制。
        if (current.entryVersion < required) {
            copy = new StateMapEntry<>(current, stateMapVersion);
            tab[mapIdx] = copy;
        } else {
            // nothing to do, just advance copy to current
            copy = current;
        }

        // we iterate the chain up to 'until entry'
        while (current != untilEntry) {

            // advance current
            current = current.next;

            if (current.entryVersion < required) {
                // copy and advance the current's copy
                copy.next = new StateMapEntry<>(current, stateMapVersion);
                copy = copy.next;
            } else {
                // nothing to do, just advance copy to current
                copy = current;
            }
        }

        return copy;
    }

    @SuppressWarnings("unchecked")
    private static <K, N, S> StateMapEntry<K, N, S> getBootstrapEntry() {
        return (StateMapEntry<K, N, S>) ITERATOR_BOOTSTRAP_ENTRY;
    }

    /** Helper function that creates and scrambles a composite hash for key and namespace. */
    private static int compositeHash(Object key, Object namespace) {
        // create composite key through XOR, then apply some bit-mixing for better distribution of
        // skewed keys.
        return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
    }

    /**
     * Creates a snapshot of this {@link CopyOnWriteStateMap}, to be written in checkpointing. The
     * snapshot integrity is protected through copy-on-write from the {@link CopyOnWriteStateMap}.
     * Users should call {@link #releaseSnapshot(StateMapSnapshot)} after using the returned object.
     *
     * @return a snapshot from this {@link CopyOnWriteStateMap}, for checkpointing.
     */
    @Nonnull
    @Override
    public CopyOnWriteStateMapSnapshot<K, N, S> stateSnapshot() {
        return new CopyOnWriteStateMapSnapshot<>(this);
    }

    /**
     * Releases a snapshot for this {@link CopyOnWriteStateMap}. This method should be called once a
     * snapshot is no more needed, so that the {@link CopyOnWriteStateMap} can stop considering this
     * snapshot for copy-on-write, thus avoiding unnecessary object creation.
     *
     * @param snapshotToRelease the snapshot to release, which was previously created by this state
     *     map.
     */
    @Override
    public void releaseSnapshot(
            StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> snapshotToRelease) {

        CopyOnWriteStateMapSnapshot<K, N, S> copyOnWriteStateMapSnapshot =
                (CopyOnWriteStateMapSnapshot<K, N, S>) snapshotToRelease;

        Preconditions.checkArgument(
                copyOnWriteStateMapSnapshot.isOwner(this),
                "Cannot release snapshot which is owned by a different state map.");

        releaseSnapshot(copyOnWriteStateMapSnapshot.getSnapshotVersion());
    }

    @VisibleForTesting
    Set<Integer> getSnapshotVersions() {
        return snapshotVersions;
    }

    // Meta data setter / getter and toString -----------------------------------------------------

    public TypeSerializer<S> getStateSerializer() {
        return stateSerializer;
    }

    public void setStateSerializer(TypeSerializer<S> stateSerializer) {
        this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
    }

    // StateMapEntry
    // -------------------------------------------------------------------------------------------------

    /**
     * One entry in the {@link CopyOnWriteStateMap}. This is a triplet of key, namespace, and state.
     * Thereby, key and namespace together serve as a composite key for the state. This class also
     * contains some management meta data for copy-on-write, a pointer to link other {@link
     * StateMapEntry}s to a list, and cached hash code.
     *
     * @param <K> type of key.
     * @param <N> type of namespace.
     * @param <S> type of state.
     */
    // StateMapEntry 类代表 CopyOnWriteStateMap 中的一个条目，
    // 它存储了一个三元组（Key、Namespace、State）以及用于支持写时复制（Copy-On-Write, COW）和哈希表链式寻址的元数据。
    @VisibleForTesting
    protected static class StateMapEntry<K, N, S> implements StateEntry<K, N, S> {

        /** The key. Assumed to be immumap and not null. */
        // 键。
        // 被假定为不可变（immutable）且非空。
        @Nonnull final K key;

        /** The namespace. Assumed to be immumap and not null. */
        // 命名空间。
        // 被假定为不可变（immutable）且非空。
        @Nonnull final N namespace;

        /**
         * The state. This is not final to allow exchanging the object for copy-on-write. Can be
         * null.
         */
        // 状态值。
        // 不是 final，允许在写时复制操作中被替换为不同的状态对象。可以为空（null）。
        @Nullable S state;

        /**
         * Link to another {@link StateMapEntry}. This is used to resolve collisions in the {@link
         * CopyOnWriteStateMap} through chaining.
         */
        // 链表指针。
        // 用于将多个条目链接起来，解决哈希冲突（通过链式寻址）。
        @Nullable StateMapEntry<K, N, S> next;

        /**
         * The version of this {@link StateMapEntry}. This is meta data for copy-on-write of the map
         * structure.
         */
        // 条目版本。
        // 这是用于 CopyOnWriteStateMap 结构 的 COW 元数据。
        // 它标记了该条目（包括 key、namespace 和 next 指针）所属的版本。
        int entryVersion;

        /**
         * The version of the state object in this entry. This is meta data for copy-on-write of the
         * state object itself.
         */
        // 状态版本。
        // 这是用于 状态对象 state 本身 的 COW 元数据。它标记了 state 对象最近一次被修改（写入新值）时的版本。
        int stateVersion;

        /** The computed secondary hash for the composite of key and namespace. */
        // 缓存的哈希码。
        // 是 key 和 namespace 复合 计算出的次级哈希值，用于快速定位在哈希表数组中的位置。
        final int hash;

        StateMapEntry(StateMapEntry<K, N, S> other, int entryVersion) {
            this(
                    other.key,
                    other.namespace,
                    other.state,
                    other.hash,
                    other.next,
                    entryVersion,
                    other.stateVersion);
        }

        StateMapEntry(
                @Nonnull K key,
                @Nonnull N namespace,
                @Nullable S state,
                int hash,
                @Nullable StateMapEntry<K, N, S> next,
                int entryVersion,
                int stateVersion) {
            this.key = key;
            this.namespace = namespace;
            this.hash = hash;
            this.next = next;
            this.entryVersion = entryVersion;
            this.state = state;
            this.stateVersion = stateVersion;
        }

        public final void setState(@Nullable S value, int mapVersion) {
            // naturally, we can update the state version every time we replace the old state with a
            // different object
            if (value != state) {
                this.state = value;
                this.stateVersion = mapVersion;
            }
        }

        @Nonnull
        @Override
        public K getKey() {
            return key;
        }

        @Nonnull
        @Override
        public N getNamespace() {
            return namespace;
        }

        @Nullable
        @Override
        public S getState() {
            return state;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof CopyOnWriteStateMap.StateMapEntry)) {
                return false;
            }

            StateEntry<?, ?, ?> e = (StateEntry<?, ?, ?>) o;
            return e.getKey().equals(key)
                    && e.getNamespace().equals(namespace)
                    && Objects.equals(e.getState(), state);
        }

        @Override
        public final int hashCode() {
            return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
        }

        @Override
        public final String toString() {
            return "(" + key + "|" + namespace + ")=" + state;
        }
    }

    // For testing
    // ----------------------------------------------------------------------------------------------------

    @Override
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (StateEntry<K, N, S> entry : this) {
            if (null != entry && namespace.equals(entry.getNamespace())) {
                ++count;
            }
        }
        return count;
    }

    // StateEntryIterator
    // ---------------------------------------------------------------------------------------------

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return new StateIncrementalVisitorImpl(recommendedMaxNumberOfReturnedRecords);
    }

    /** Iterator over state entry chains in a {@link CopyOnWriteStateMap}. */
    class StateEntryChainIterator implements Iterator<StateMapEntry<K, N, S>> {
        StateMapEntry<K, N, S>[] activeTable;
        private int nextMapPosition;
        private final int maxTraversedMapPositions;

        StateEntryChainIterator() {
            this(Integer.MAX_VALUE);
        }

        StateEntryChainIterator(int maxTraversedMapPositions) {
            this.maxTraversedMapPositions = maxTraversedMapPositions;
            this.activeTable = primaryTable;
            this.nextMapPosition = 0;
        }

        @Override
        public boolean hasNext() {
            return size() > 0
                    && (nextMapPosition < activeTable.length || activeTable == primaryTable);
        }

        @Override
        public StateMapEntry<K, N, S> next() {
            StateMapEntry<K, N, S> next;
            // consider both sub-tables to cover the case of rehash
            while (true) { // current is empty
                // try get next in active table or
                // iteration is done over primary and rehash table
                // or primary was swapped with rehash when rehash is done
                next = nextActiveMapPosition();
                if (next != null
                        || nextMapPosition < activeTable.length
                        || activeTable == incrementalRehashTable
                        || activeTable != primaryTable) {
                    return next;
                } else {
                    // switch to rehash (empty if no rehash)
                    activeTable = incrementalRehashTable;
                    nextMapPosition = 0;
                }
            }
        }

        private StateMapEntry<K, N, S> nextActiveMapPosition() {
            StateMapEntry<K, N, S>[] tab = activeTable;
            int traversedPositions = 0;
            while (nextMapPosition < tab.length && traversedPositions < maxTraversedMapPositions) {
                StateMapEntry<K, N, S> next = tab[nextMapPosition++];
                if (next != null) {
                    return next;
                }
                traversedPositions++;
            }
            return null;
        }
    }

    /**
     * Iterator over state entries in a {@link CopyOnWriteStateMap} which does not tolerate
     * concurrent modifications.
     */
    class StateEntryIterator implements Iterator<StateEntry<K, N, S>> {

        private final StateEntryChainIterator chainIterator;
        private StateMapEntry<K, N, S> nextEntry;
        private final int expectedModCount;

        StateEntryIterator() {
            this.chainIterator = new StateEntryChainIterator();
            this.expectedModCount = modCount;
            this.nextEntry = getBootstrapEntry();
            advanceIterator();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public StateEntry<K, N, S> next() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return advanceIterator();
        }

        StateMapEntry<K, N, S> advanceIterator() {
            StateMapEntry<K, N, S> entryToReturn = nextEntry;
            StateMapEntry<K, N, S> next = nextEntry.next;
            if (next == null) {
                next = chainIterator.next();
            }
            nextEntry = next;
            return entryToReturn;
        }
    }

    /** Incremental visitor over state entries in a {@link CopyOnWriteStateMap}. */
    class StateIncrementalVisitorImpl implements InternalKvState.StateIncrementalVisitor<K, N, S> {

        private final StateEntryChainIterator chainIterator;
        private final Collection<StateEntry<K, N, S>> chainToReturn = new ArrayList<>(5);

        StateIncrementalVisitorImpl(int recommendedMaxNumberOfReturnedRecords) {
            chainIterator = new StateEntryChainIterator(recommendedMaxNumberOfReturnedRecords);
        }

        @Override
        public boolean hasNext() {
            return chainIterator.hasNext();
        }

        @Override
        public Collection<StateEntry<K, N, S>> nextEntries() {
            if (!hasNext()) {
                return null;
            }

            chainToReturn.clear();
            for (StateMapEntry<K, N, S> nextEntry = chainIterator.next();
                    nextEntry != null;
                    nextEntry = nextEntry.next) {
                chainToReturn.add(nextEntry);
            }
            return chainToReturn;
        }

        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            CopyOnWriteStateMap.this.remove(stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            CopyOnWriteStateMap.this.put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }
    }
}
