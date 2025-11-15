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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Heap-backed partitioned {@link MapState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
// HeapMapState<K, N, UK, UV> 是 Flink 中 MapState (映射状态) 接口的一个实现，它使用 JVM 堆内存（Heap） 作为底层存储
// 基于堆的存储： 它是 Flink HeapStateBackend 的核心组件之一。与 RocksDBMapState 将每个内部条目平铺到磁盘不同，HeapMapState 将整个内部 Map (Map<UK, UV>) 作为一个对象，存储在 JVM 堆内存中，并由父类 StateTable 统一管理。
// 高性能访问： 由于所有状态数据都保存在内存中，对 Map 的读写操作（get, put 等）速度非常快，是 $O(1)$ 的哈希表操作复杂度，适用于读写密集且状态大小适中的应用场景。
// 快照机制： 虽然数据在内存中，但 Flink 的检查点（Checkpoint）机制会负责将这些内存中的 Map 对象序列化并写入外部文件存储（如 HDFS、S3），从而实现容错和持久化。
// 它提供了基于内存的 Map 状态功能，追求极致的访问速度，同时通过 Flink 的 Checkpoint 机制保证数据的安全。
class HeapMapState<K, N, UK, UV> extends AbstractHeapState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     */
    private HeapMapState(
            StateTable<K, N, Map<UK, UV>> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<Map<UK, UV>> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            Map<UK, UV> defaultValue) {
        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);

        Preconditions.checkState(
                valueSerializer instanceof MapSerializer, "Unexpected serializer type.");
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public UV get(UK userKey) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);

        if (userMap == null) {
            return null;
        }

        return userMap.get(userKey);
    }

    @Override
    public void put(UK userKey, UV userValue) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        if (userMap == null) {
            userMap = new HashMap<>();
            stateTable.put(currentNamespace, userMap);
        }

        userMap.put(userKey, userValue);
    }

    @Override
    public void putAll(Map<UK, UV> value) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);

        if (userMap == null) {
            userMap = new HashMap<>();
            stateTable.put(currentNamespace, userMap);
        }

        userMap.putAll(value);
    }

    @Override
    public void remove(UK userKey) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        if (userMap == null) {
            return;
        }

        userMap.remove(userKey);

        if (userMap.isEmpty()) {
            clear();
        }
    }

    @Override
    public boolean contains(UK userKey) {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap != null && userMap.containsKey(userKey);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptySet() : userMap.entrySet();
    }

    @Override
    public Iterable<UK> keys() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptySet() : userMap.keySet();
    }

    @Override
    public Iterable<UV> values() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptySet() : userMap.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptyIterator() : userMap.entrySet().iterator();
    }

    @Override
    public boolean isEmpty() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null || userMap.isEmpty();
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);

        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        Map<UK, UV> result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

        if (result == null) {
            return null;
        }

        final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

        final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
        final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();

        return KvStateSerializer.serializeMap(
                result.entrySet(), dupUserKeySerializer, dupUserValueSerializer);
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new HeapMapState<>(
                        (StateTable<K, N, Map<UK, UV>>) stateTable,
                        keySerializer,
                        (TypeSerializer<Map<UK, UV>>) stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        (Map<UK, UV>) stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState) {
        return (IS)
                ((HeapMapState<K, N, UK, UV>) existingState)
                        .setNamespaceSerializer(stateTable.getNamespaceSerializer())
                        .setValueSerializer(
                                (TypeSerializer<Map<UK, UV>>) stateTable.getStateSerializer())
                        .setDefaultValue((Map<UK, UV>) stateDesc.getDefaultValue());
    }
}
