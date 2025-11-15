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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

/**
 * Base class for partitioned {@link State} implementations that are backed by a regular heap hash
 * map. The concrete implementations define how the state is checkpointed.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 */
// Flink 基于堆内存（Heap）的状态后端中，所有 键控状态（Keyed State） 实现的抽象基类。
// 它为所有基于 Java Heap 实现的状态类型（如 ValueState, ListState 等）提供了一个通用的底层框架
public abstract class AbstractHeapState<K, N, SV> implements InternalKvState<K, N, SV> {

    /** Map containing the actual key/value pairs. */
    // 状态的底层存储结构
    protected final StateTable<K, N, SV> stateTable;

    /** The current namespace, which the access methods will refer to. */
    //存储当前正在操作的命名空间。这个值由 Flink 运行时通过 setCurrentNamespace() 方法设置
    protected N currentNamespace;
    //用于序列化和反序列化键、值和命名空间的序列化器
    protected final TypeSerializer<K> keySerializer;

    protected TypeSerializer<SV> valueSerializer;

    protected TypeSerializer<N> namespaceSerializer;
    //存储在创建状态时指定的默认值
    private SV defaultValue;

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     */
    AbstractHeapState(
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<SV> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            SV defaultValue) {

        this.stateTable = Preconditions.checkNotNull(stateTable, "State table must not be null.");
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.defaultValue = defaultValue;
        this.currentNamespace = null;
    }

    // ------------------------------------------------------------------------

    @Override
    public final void clear() {
        stateTable.remove(currentNamespace);
    }

    @Override
    public final void setCurrentNamespace(N namespace) {
        this.currentNamespace =
                Preconditions.checkNotNull(namespace, "Namespace must not be null.");
    }
    //根据传入的序列化键和命名空间，获取序列化后的状态值
    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<SV> safeValueSerializer)
            throws Exception {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);
        //将传入的字节数组反序列化为键和命名空间
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        //获取对应的值
        SV result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

        if (result == null) {
            return null;
        }
        //将获取到的值序列化成字节数组并返回
        return KvStateSerializer.serializeValue(result, safeValueSerializer);
    }

    /** This should only be used for testing. */
    @VisibleForTesting
    public StateTable<K, N, SV> getStateTable() {
        return stateTable;
    }

    protected SV getDefaultValue() {
        if (defaultValue != null) {
            return valueSerializer.copy(defaultValue);
        } else {
            return null;
        }
    }

    protected AbstractHeapState<K, N, SV> setNamespaceSerializer(
            TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    protected AbstractHeapState<K, N, SV> setValueSerializer(TypeSerializer<SV> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    protected AbstractHeapState<K, N, SV> setDefaultValue(SV defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    @Override
    public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return stateTable.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
