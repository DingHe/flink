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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A read-only {@link MapState} that does not allow for modifications.
 *
 * <p>This is the result returned when querying Flink's keyed state using the {@link
 * org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and providing
 * an {@link MapStateDescriptor}.
 */
// ImmutableMapState<K, V> 的作用是作为 Flink 可查询状态（Queryable State） 客户端的结果容器，提供一个 只读的 MapState 视图。
// 只读封装： 它实现了 Flink 的 MapState 接口，但会阻止任何修改状态的操作（如 put、remove、clear），并在调用这些方法时抛出异常。
// 可查询状态结果： 当外部客户端（通过 QueryableStateClient）查询 Flink 任务的状态，并且查询的是一个 MapStateDescriptor 时，后端返回的序列化数据会被反序列化并封装成 ImmutableMapState 对象。
// 数据隔离： 它确保外部查询只能查看当前的状态快照，而无法意外或恶意地修改正在运行的 Flink 应用程序的状态数据，从而保证了运行时的数据安全和一致性。
public final class ImmutableMapState<K, V> extends ImmutableState implements MapState<K, V> {

    // 状态数据存储：这是一个 final 字段，用于存储从 Flink 状态后端查询并反序列化得到的底层 Map 数据。
    // 它是此类的核心数据结构，所有读取操作都委托给这个 Map。
    private final Map<K, V> state;

    private ImmutableMapState(final Map<K, V> mapState) {
        this.state = Preconditions.checkNotNull(mapState);
    }

    @Override
    public V get(K key) {
        return state.get(key);
    }

    @Override
    public void put(K key, V value) {
        throw MODIFICATION_ATTEMPT_ERROR;
    }

    @Override
    public void putAll(Map<K, V> map) {
        throw MODIFICATION_ATTEMPT_ERROR;
    }

    @Override
    public void remove(K key) {
        throw MODIFICATION_ATTEMPT_ERROR;
    }

    @Override
    public boolean contains(K key) {
        return state.containsKey(key);
    }

    /**
     * Returns all the mappings in the state in a {@link Collections#unmodifiableSet(Set)}.
     *
     * @return A read-only iterable view of all the key-value pairs in the state.
     */
    @Override
    public Iterable<Map.Entry<K, V>> entries() {
        return Collections.unmodifiableSet(state.entrySet());
    }

    /**
     * Returns all the keys in the state in a {@link Collections#unmodifiableSet(Set)}.
     *
     * @return A read-only iterable view of all the keys in the state.
     */
    @Override
    public Iterable<K> keys() {
        return Collections.unmodifiableSet(state.keySet());
    }

    /**
     * Returns all the values in the state in a {@link
     * Collections#unmodifiableCollection(Collection)}.
     *
     * @return A read-only iterable view of all the values in the state.
     */
    @Override
    public Iterable<V> values() {
        return Collections.unmodifiableCollection(state.values());
    }

    /**
     * Iterates over all the mappings in the state. The iterator cannot remove elements.
     *
     * @return A read-only iterator over all the mappings in the state.
     */
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return Collections.unmodifiableSet(state.entrySet()).iterator();
    }

    @Override
    public boolean isEmpty() {
        return state.isEmpty();
    }

    @Override
    public void clear() {
        throw MODIFICATION_ATTEMPT_ERROR;
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T, S extends State> S createState(
            StateDescriptor<S, T> stateDescriptor, byte[] serializedState) throws IOException {
        MapStateDescriptor<K, V> mapStateDescriptor = (MapStateDescriptor<K, V>) stateDescriptor;
        final Map<K, V> state =
                KvStateSerializer.deserializeMap(
                        serializedState,
                        mapStateDescriptor.getKeySerializer(),
                        mapStateDescriptor.getValueSerializer());
        return (S) new ImmutableMapState<>(state);
    }
}
