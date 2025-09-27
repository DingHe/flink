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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;

import java.util.Collection;

/**
 * The {@code InternalKvState} is the root of the internal state type hierarchy, similar to the
 * {@link State} being the root of the public API state hierarchy.
 *
 * <p>The internal state classes give access to the namespace getters and setters and access to
 * additional functionality, like raw value access or state merging.
 *
 * <p>The public API state hierarchy is intended to be programmed against by Flink applications. The
 * internal state hierarchy holds all the auxiliary methods that are used by the runtime and not
 * intended to be used by user applications. These internal methods are considered of limited use to
 * users and only confusing, and are usually not regarded as stable across releases.
 *
 * <p>Each specific type in the internal state hierarchy extends the type from the public state
 * hierarchy:
 *
 * <pre>
 *             State
 *               |
 *               +-------------------InternalKvState
 *               |                         |
 *          MergingState                   |
 *               |                         |
 *               +-----------------InternalMergingState
 *               |                         |
 *      +--------+------+                  |
 *      |               |                  |
 * ReducingState    ListState        +-----+-----------------+
 *      |               |            |                       |
 *      +-----------+   +-----------   -----------------InternalListState
 *                  |                |
 *                  +---------InternalReducingState
 * </pre>
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> The type of values kept internally in state
 */
//主要作用是为 Flink 的运行时和状态后端（State Backend）提供一个统一的接口，用于更底层、更精细地操作状态
    //提供序列化器访问：允许访问键、命名空间和值的序列化器（TypeSerializer），这对于状态后端在存储、读取和网络传输时处理原始字节数据至关重要
    //管理命名空间：支持设置和获取当前处理的命名空间（namespace），这是 Flink 窗口（Window）机制等高级功能的基础
    //支持低级操作：提供了获取序列化后的原始值 (getSerializedValue) 和状态增量访问 (getStateIncrementalVisitor) 等方法。这些方法是**状态后端实现和检查点（Checkpointing）**等核心功能所必需的
public interface InternalKvState<K, N, V> extends State {
    //分别返回用于序列化和反序列化键（Key）、**命名空间（Namespace）和值（Value）**的类型序列化器
    /** Returns the {@link TypeSerializer} for the type of key this state is associated to. */
    TypeSerializer<K> getKeySerializer();

    /** Returns the {@link TypeSerializer} for the type of namespace this state is associated to. */
    TypeSerializer<N> getNamespaceSerializer();

    /** Returns the {@link TypeSerializer} for the type of value this state holds. */
    TypeSerializer<V> getValueSerializer();

    /**
     * Sets the current namespace, which will be used when using the state access methods.
     *
     * @param namespace The namespace.
     */
    //在执行状态操作之前，设置当前的命名空间
    void setCurrentNamespace(N namespace);

    /**
     * Returns the serialized value for the given key and namespace.
     *
     * <p>If no value is associated with key and namespace, <code>null</code> is returned.
     *
     * <p><b>TO IMPLEMENTERS:</b> This method is called by multiple threads. Anything stateful (e.g.
     * serializers) should be either duplicated or protected from undesired consequences of
     * concurrent invocations.
     *
     * @param serializedKeyAndNamespace Serialized key and namespace
     * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded
     *     context
     * @param safeNamespaceSerializer A namespace serializer which is safe to be used even in
     *     multi-threaded context
     * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded
     *     context
     * @return Serialized value or <code>null</code> if no value is associated with the key and
     *     namespace.
     * @throws Exception Exceptions during serialization are forwarded
     */
    //根据序列化后的键和命名空间，返回序列化后的原始值
    byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception;

    /**
     * Get global visitor of state entries.
     *
     * @param recommendedMaxNumberOfReturnedRecords hint to the visitor not to exceed this number of
     *     returned records per {@code nextEntries} call, it can still be exceeded by some smaller
     *     constant.
     * @return global iterator over state entries
     */
    //返回一个增量状态访问器
    //允许状态后端分批次（即增量地）遍历和访问状态中的所有键/值对，而不是一次性加载所有状态，从而降低了检查点时的内存和I/O开销
    StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords);

    /**
     * The state entry visitor which supports remove and update of the last returned entries.
     *
     * <p>The visitor should tolerate concurrent modifications. It might trade this tolerance for
     * consistency and return duplicates or not all values (created while visiting) but always state
     * values which exist and up-to-date at the moment of calling {@code nextEntries()}.
     */
    interface StateIncrementalVisitor<K, N, V> {
        /**
         * Whether the visitor potentially has some next entries to return from {@code
         * nextEntries()}.
         */
        //检查是否还有更多状态条目可以访问
        boolean hasNext();

        /**
         * Return some next entries which are available at the moment.
         *
         * <p>If empty collection is returned, it does not mean that the visitor is exhausted but it
         * means that the visitor has done some incremental work advancing and checking internal
         * data structures. The finished state of the visitor has to be checked by {@code hasNext()}
         * method.
         *
         * <p>The returned collection and state values must not be changed internally (there might
         * be no defensive copies in {@code nextEntries()} for performance). It has to be deeply
         * copied if it is to modify, e.g. with the {@code update()} method.
         */
        //返回下一批状态条目
        Collection<StateEntry<K, N, V>> nextEntries();
        //允许在遍历时对状态条目进行删除和更新。这对于状态后端的高级优化（如合并状态）非常重要
        void remove(StateEntry<K, N, V> stateEntry);

        /**
         * Update the value of the last returned entry from the {@code next()} method.
         *
         * @throws IllegalStateException if next() has never been called yet or iteration is over.
         */
        void update(StateEntry<K, N, V> stateEntry, V newValue);
    }
}
