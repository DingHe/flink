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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.util.Disposable;

import java.util.stream.Stream;

/**
 * A keyed state backend provides methods for managing keyed state.
 *
 * @param <K> The key by which state is keyed.
 */
// KeyedStateBackend 接口定义了一个完整的键控状态管理器所需具备的所有功能。它位于 Flink 运行时，负责管理和维护所有与 Key 相关的状态数据，并为上层 API（如 RichFunction 中的 getRuntimeContext().getState()）提供底层支持。
// 状态生命周期管理： 它负责键控状态（ValueState、ListState 等）的创建、获取和更新
// Key 上下文切换： 它是 Flink 实现 Key-by-Key 处理模型的关键。它允许运行时设置当前正在处理的 Key，确保所有状态操作都作用于正确的 Key。
// 优先级队列管理： 它集成了 PriorityQueueSetFactory 的功能，负责为 Flink 的定时器服务（Timer Service） 创建和管理 Keyed 优先级队列。
// 它是 Flink 状态处理的核心引擎，负责所有 Keyed 数据的存储、访问和上下文管理。

public interface KeyedStateBackend<K>
        extends KeyedStateFactory, PriorityQueueSetFactory, Disposable {

    /**
     * Sets the current key that is used for partitioned state.
     *
     * @param newKey The new current key.
     */
    // 设置当前 Key
    // Flink 运行时（StreamTask）在处理一条数据记录之前，会调用此方法来切换状态访问的上下文，确保后续所有状态操作都针对 newKey 进行。
    void setCurrentKey(K newKey);

    /** @return Current key. */
    // 获取当前 Key。
    // 返回当前正在被处理的 Key。
    K getCurrentKey();

    /** @return Serializer of the key. */
    // 获取 Key 序列化器。
    // 返回用于序列化和反序列化 Key 的 TypeSerializer 实例。
    TypeSerializer<K> getKeySerializer();

    /**
     * Applies the provided {@link KeyedStateFunction} to the state with the provided {@link
     * StateDescriptor} of all the currently active keys.
     *
     * @param namespace the namespace of the state.
     * @param namespaceSerializer the serializer for the namespace.
     * @param stateDescriptor the descriptor of the state to which the function is going to be
     *     applied.
     * @param function the function to be applied to the keyed state.
     * @param <N> The type of the namespace.
     * @param <S> The type of the state.
     */
    // 对所有 Key 应用函数。
    // 允许对当前 KeyedStateBackend 负责的所有 Key 上给定命名空间和状态实例执行一个自定义函数 (KeyedStateFunction)。
    // 主要用于状态迁移、清理或统一更新等维护操作。
    <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception;

    /**
     * @return A stream of all keys for the given state and namespace. Modifications to the state
     *     during iterating over it keys are not supported.
     * @param state State variable for which existing keys will be returned.
     * @param namespace Namespace for which existing keys will be returned.
     */
    // 获取给定状态和命名空间下的所有 Key。
    <N> Stream<K> getKeys(String state, N namespace);

    /**
     * @return A stream of all keys for the given state and namespace. Modifications to the state
     *     during iterating over it keys are not supported. Implementations go not make any ordering
     *     guarantees about the returned tupes. Two records with the same key or namespace may not
     *     be returned near each other in the stream.
     * @param state State variable for which existing keys will be returned.
     */
    // 获取给定状态下的所有 Key 和 Namespace 组合。
    <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state);

    /**
     * Creates or retrieves a keyed state backed by this state backend.
     *
     * @param namespaceSerializer The serializer used for the namespace type of the state
     * @param stateDescriptor The identifier for the state. This contains name and can create a
     *     default state value.
     * @param <N> The type of the namespace.
     * @param <S> The type of the state.
     * @return A new key/value state backed by this backend.
     * @throws Exception Exceptions may occur during initialization of the state and should be
     *     forwarded.
     */
    // 创建或获取键控状态。
    // 它是 Flink 用户调用 getRuntimeContext().getState(StateDescriptor) 时在底层调用的核心方法。
    // 它根据 stateDescriptor 和 namespaceSerializer 来创建一个新的状态实例，或者如果该状态已存在（如从 Checkpoint 恢复），则获取并返回它。
    <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception;

    /**
     * Creates or retrieves a partitioned state backed by this state backend.
     *
     * <p>TODO: NOTE: This method does a lot of work caching / retrieving states just to update the
     * namespace. This method should be removed for the sake of namespaces being lazily fetched from
     * the keyed state backend, or being set on the state directly.
     *
     * @param stateDescriptor The identifier for the state. This contains name and can create a
     *     default state value.
     * @param <N> The type of the namespace.
     * @param <S> The type of the state.
     * @return A new key/value state backed by this backend.
     * @throws Exception Exceptions may occur during initialization of the state and should be
     *     forwarded.
     */
    // 获取分区状态（已弃用说明）
    <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception;
    // 销毁资源
    @Override
    void dispose();

    /**
     * State backend will call {@link KeySelectionListener#keySelected} when key context is switched
     * if supported.
     */
    // 注册 Key 选择监听器。
    // 允许其他组件注册一个监听器，以便在每次调用 setCurrentKey() 切换 Key 上下文时接收到通知回调
    void registerKeySelectionListener(KeySelectionListener<K> listener);

    /**
     * Stop calling listener registered in {@link #registerKeySelectionListener}.
     *
     * @return returns true iff listener was registered before.
     */
    // 取消注册 Key 选择监听器。
    // 停止接收 Key 切换的通知。
    // 返回 true 表示成功移除。
    boolean deregisterKeySelectionListener(KeySelectionListener<K> listener);
    // 状态是否在后端中不可变
    @Deprecated
    default boolean isStateImmutableInStateBackend(CheckpointType checkpointOptions) {
        return false;
    }

    /**
     * Whether it's safe to reuse key-values from the state-backend, e.g for the purpose of
     * optimization.
     *
     * <p>NOTE: this method should not be used to check for {@link InternalPriorityQueue}, as the
     * priority queue could be stored on different locations, e.g RocksDB state-backend could store
     * that on JVM heap if configuring HEAP as the time-service factory.
     *
     * @return returns ture if safe to reuse the key-values from the state-backend.
     */
    // 键值状态是否可以安全重用。
    // 返回一个布尔值，指示状态后端内部存储的键值对是否可以被安全地重用（即，不进行深拷贝）
    // 用于优化读操作，例如在 Heap State Backend 中，如果数据是不可变的，可以直接返回引用。
    default boolean isSafeToReuseKVState() {
        return false;
    }

    /** Listener is given a callback when {@link #setCurrentKey} is called (key context changes). */
    // Key 选择监听器接口。
    // 这是一个函数式接口，定义了 keySelected(K newKey) 回调方法，在 Key 上下文切换时被调用。
    @FunctionalInterface
    interface KeySelectionListener<K> {
        /** Callback when key context is switched. */
        void keySelected(K newKey);
    }
}
