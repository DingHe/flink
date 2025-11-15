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
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link BroadcastState Broadcast State} backed a heap-based {@link Map}.
 *
 * @param <K> The key type of the elements in the {@link BroadcastState Broadcast State}.
 * @param <V> The value type of the elements in the {@link BroadcastState Broadcast State}.
 */
// 基于内存（堆）存储： 它使用一个标准的 Java HashMap (backingMap) 来在任务管理器（TaskManager）的堆内存中存储广播状态数据。
// 实现所有广播状态功能： 它实现了 BackendWritableBroadcastState 接口（该接口又继承自 BroadcastState），因此它支持所有的读写操作 (put, get, remove 等)。
// 支持 Checkpointing： 它提供了将内存中的 Map 数据序列化并写入 Checkpoint/Savepoint 的机制 (write 方法)，以确保容错能力。
public class HeapBroadcastState<K, V> implements BackendWritableBroadcastState<K, V> {

    /** Meta information of the state, including state name, assignment mode, and serializer. */
    // 状态元信息。
    // 存储关于此状态的元数据，包括状态的名称、键和值类型对应的序列化器（Serializer） 等信息
    private RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo;

    /** The internal map the holds the elements of the state. */
    // 底层存储 Map。
    // 这是 HeapBroadcastState 的核心数据结构。
    // 它是一个 Java HashMap，负责在 TaskManager 的堆内存中存储实际的广播状态键值对。
    private final Map<K, V> backingMap;

    /** A serializer that allows to perform deep copies of internal map state. */
    // backingMap的序列化器
    private MapSerializer<K, V> internalMapCopySerializer;

    HeapBroadcastState(RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo) {
        this(stateMetaInfo, new HashMap<>());
    }

    private HeapBroadcastState(
            final RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo,
            final Map<K, V> internalMap) {

        this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
        this.backingMap = Preconditions.checkNotNull(internalMap);
        this.internalMapCopySerializer =
                new MapSerializer<>(
                        stateMetaInfo.getKeySerializer(), stateMetaInfo.getValueSerializer());
    }

    private HeapBroadcastState(HeapBroadcastState<K, V> toCopy) {
        this(
                toCopy.stateMetaInfo.deepCopy(),
                toCopy.internalMapCopySerializer.copy(toCopy.backingMap));
    }

    @Override
    public void setStateMetaInfo(RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo) {
        this.internalMapCopySerializer =
                new MapSerializer<>(
                        stateMetaInfo.getKeySerializer(), stateMetaInfo.getValueSerializer());
        this.stateMetaInfo = stateMetaInfo;
    }

    @Override
    public RegisteredBroadcastStateBackendMetaInfo<K, V> getStateMetaInfo() {
        return stateMetaInfo;
    }

    @Override
    public HeapBroadcastState<K, V> deepCopy() {
        return new HeapBroadcastState<>(this);
    }

    @Override
    public void clear() {
        backingMap.clear();
    }

    @Override
    public String toString() {
        return "HeapBroadcastState{"
                + "stateMetaInfo="
                + stateMetaInfo
                + ", backingMap="
                + backingMap
                + ", internalMapCopySerializer="
                + internalMapCopySerializer
                + '}';
    }
    // 将状态写入 Checkpoint/Savepoint
    // 遍历 backingMap 中的所有键值对，使用状态元信息中的序列化器，将数据以 Map 格式写入到提供的文件系统输出流 out 中。
    @Override
    public long write(FSDataOutputStream out) throws IOException {
        long partitionOffset = out.getPos();

        DataOutputView dov = new DataOutputViewStreamWrapper(out);
        dov.writeInt(backingMap.size());
        for (Map.Entry<K, V> entry : backingMap.entrySet()) {
            getStateMetaInfo().getKeySerializer().serialize(entry.getKey(), dov);
            getStateMetaInfo().getValueSerializer().serialize(entry.getValue(), dov);
        }

        return partitionOffset;
    }

    @Override
    public V get(K key) {
        return backingMap.get(key);
    }

    @Override
    public void put(K key, V value) {
        backingMap.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> map) {
        backingMap.putAll(map);
    }

    @Override
    public void remove(K key) {
        backingMap.remove(key);
    }

    @Override
    public boolean contains(K key) {
        return backingMap.containsKey(key);
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return backingMap.entrySet().iterator();
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
        return backingMap.entrySet();
    }

    @Override
    public Iterable<Map.Entry<K, V>> immutableEntries() {
        return Collections.unmodifiableSet(backingMap.entrySet());
    }

    @VisibleForTesting
    public MapSerializer<K, V> getInternalMapCopySerializer() {
        return internalMapCopySerializer;
    }
}
