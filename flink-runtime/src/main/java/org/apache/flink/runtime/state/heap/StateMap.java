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
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

import java.util.stream.Stream;

/**
 * Base class for state maps.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
// StateMap 抽象类是 Flink 堆状态后端（HeapKeyedStateBackend） 中用于管理单个 Key Group 内部状态数据的核心抽象
// 存储结构抽象： 它抽象了在给定 Key Group 内，Key 和 Namespace 到实际状态值 (State Value) 的映射关系。一个状态条目总是由 (Key, Namespace) 组成的复合键唯一确定。
// Key Group 粒度操作： 它定义了所有基本的 CRUD（创建、读取、更新、删除）操作，但这些操作的作用域被限定在它所代表的单个 Key Group 内。
// 支持快照机制： 它提供了创建和释放快照 (stateSnapshot/releaseSnapshot) 的方法，是实现堆状态后端 写时复制（Copy-on-Write, COW） 快照机制的关键抽象。
public abstract class StateMap<K, N, S> implements Iterable<StateEntry<K, N, S>> {

    // Main interface methods of StateMap -------------------------------------------------------

    /**
     * Returns whether this {@link StateMap} is empty.
     *
     * @return {@code true} if this {@link StateMap} has no elements, {@code false} otherwise.
     * @see #size()
     */
    // 判断是否为空。
    // 基于 size() == 0 的默认实现。
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the total number of entries in this {@link StateMap}.
     *
     * @return the number of entries in this {@link StateMap}.
     */
    // 获取大小。
    // 返回当前 StateMap（即当前 Key Group）中存储的状态条目总数。
    public abstract int size();

    /**
     * Returns the state for the composite of active key and given namespace.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @return the state of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    // 读取状态值。
    // 根据给定的 Key 和 Namespace 组成的复合键，查找并返回对应的状态值。
    public abstract S get(K key, N namespace);

    /**
     * Returns whether this map contains the specified key/namespace composite key.
     *
     * @param key the key in the composite key to search for. Not null.
     * @param namespace the namespace in the composite key to search for. Not null.
     * @return {@code true} if this map contains the specified key/namespace composite key, {@code
     *     false} otherwise.
     */
    // 检查键是否存在。
    // 检查当前 StateMap 中是否存在由给定 Key 和 Namespace 组成的复合键。
    public abstract boolean containsKey(K key, N namespace);

    /**
     * Maps the specified key/namespace composite key to the specified value. This method should be
     * preferred over {@link #putAndGetOld(K, N, S)} (key, Namespace, State) when the caller is not
     * interested in the old state.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     */
    // 写入/更新状态值。
    // 将给定的状态值 state 写入由 Key 和 Namespace 确定的位置。如果 Key 存在则更新，不存在则新增。
    public abstract void put(K key, N namespace, S state);

    /**
     * Maps the composite of active key and given namespace to the specified state. Returns the
     * previous state that was registered under the composite key.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     * @return the state of any previous mapping with the specified key or {@code null} if there was
     *     no such mapping.
     */

    // 写入并返回旧值。
    // 将新状态值写入，并返回被覆盖的旧状态值。如果之前没有旧值，则返回 null。
    public abstract S putAndGetOld(K key, N namespace, S state);

    /**
     * Removes the mapping for the composite of active key and given namespace. This method should
     * be preferred over {@link #removeAndGetOld(K, N)} when the caller is not interested in the old
     * state.
     *
     * @param key the key of the mapping to remove. Not null.
     * @param namespace the namespace of the mapping to remove. Not null.
     */
    // 移除状态。
    // 移除由 Key 和 Namespace 确定的状态条目。不返回旧值，性能优于 removeAndGetOld。
    public abstract void remove(K key, N namespace);

    /**
     * Removes the mapping for the composite of active key and given namespace, returning the state
     * that was found under the entry.
     *
     * @param key the key of the mapping to remove. Not null.
     * @param namespace the namespace of the mapping to remove. Not null.
     * @return the state of the removed mapping or {@code null} if no mapping for the specified key
     *     was found.
     */
    // 移除并返回旧值。
    // 移除状态条目，并返回被移除的旧状态值。
    public abstract S removeAndGetOld(K key, N namespace);

    /**
     * Applies the given {@link StateTransformationFunction} to the state (1st input argument),
     * using the given value as second input argument. The result of {@link
     * StateTransformationFunction#apply(Object, Object)} is then stored as the new state. This
     * function is basically an optimization for get-update-put pattern.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @param value the value to use in transforming the state. Can be null.
     * @param transformation the transformation function.
     * @throws Exception if some exception happens in the transformation function.
     */
    // 状态转换。
    // 获取当前状态，应用 StateTransformationFunction 进行转换，并将结果作为新状态存回。这是 原子性 的“获取-修改-写入”操作的优化。
    public abstract <T> void transform(
            K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception;

    // For queryable state ------------------------------------------------------------------------
    // 获取 Key 流。
    // 返回一个 Stream，包含当前 Key Group 中，属于指定 namespace 的所有 Key。
    // 主要用于 Queryable State 或遍历 Key 的操作。
    public abstract Stream<K> getKeys(N namespace);
    // 获取增量状态访问器。
    // 返回一个增量访问器，用于在进行增量 Checkpoint 时，批量、高效地获取和遍历状态条目。
    public abstract InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords);

    /**
     * Creates a snapshot of this {@link StateMap}, to be written in checkpointing. Users should
     * call {@link #releaseSnapshot(StateMapSnapshot)} after using the returned object.
     *
     * @return a snapshot from this {@link StateMap}, for checkpointing.
     */
    // 创建状态快照。
    // 在 Checkpoint 的同步阶段调用，创建一个该 StateMap 实例的逻辑快照，用于实现写时复制。
    // 返回的 StateMapSnapshot 包含了快照的元数据和数据副本。
    @Nonnull
    public abstract StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot();

    /**
     * Releases a snapshot for this {@link StateMap}. This method should be called once a snapshot
     * is no more needed.
     *
     * @param snapshotToRelease the snapshot to release, which was previously created by this state
     *     map.
     */
    // 释放状态快照。
    // 在快照数据被异步写入 Checkpoint 存储后调用。
    // 它释放与该快照相关联的临时资源，特别是内存中的写时复制（COW）副本。
    public void releaseSnapshot(
            StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> snapshotToRelease) {}

    // For testing --------------------------------------------------------------------------------
    // 获取命名空间大小（测试用）。
    // 返回给定 namespace 在当前 StateMap（Key Group）中拥有的状态条目数量。主要用于单元测试和调试。
    @VisibleForTesting
    public abstract int sizeOfNamespace(Object namespace);
}
