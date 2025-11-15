/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>State is not stored in this class but in the {@link org.rocksdb.RocksDB} instance that the
 * {@link EmbeddedRocksDBStateBackend} manages and checkpoints.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of values kept internally in state.
 */
// AbstractRocksDBState 是 Flink 基于 RocksDB 状态后端（RocksDB State Backend）中所有具体状态类型（如 RocksDBValueState、RocksDBListState 等）的基类
// 提供通用基础设施： 封装了与 RocksDB 交互所需的公共组件和逻辑，例如序列化器、RocksDB 后端引用、列族句柄等。
// 管理 Key/Namespace 组合： 提供了将 Flink 的 当前 Key、Key Group 和当前 Namespace 组合序列化为 RocksDB 存储所需的复合字节键 (byte[]) 的方法。RocksDB 是一个 Key-Value 存储，它需要一个唯一的字节键来表示 Flink 的三元组 $(Key, Namespace, Value)$。
// 支持状态操作： 实现了状态接口（InternalKvState 和 State）中的通用方法，如 clear() 和 setCurrentNamespace()。

public abstract class AbstractRocksDBState<K, N, V> implements InternalKvState<K, N, V>, State {

    /** Serializer for the namespace. */
    // 命名空间序列化器
    TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    // 状态值序列化器。用于将状态值对象 V 转换为字节流进行存储
    TypeSerializer<V> valueSerializer;

    /** The current namespace, which the next value methods will refer to. */
    // 当前命名空间。由 Flink 运行时设置，所有后续的状态操作（get/put/clear）都将作用于此命名空间。
    private N currentNamespace;

    /** Backend that holds the actual RocksDB instance where we store state. */
    // RocksDB 键控状态后端引用。
    // 这是指向管理实际 RocksDB 实例的后端类的引用。通过它可以访问底层的 RocksDB 数据库 (backend.db)
    protected RocksDBKeyedStateBackend<K> backend;

    /** The column family of this particular instance of state. */
    // 列族句柄。
    // RocksDB 中的列族概念类似于传统数据库中的表。
    // 每个 Flink 状态实例（如一个 ValueState）都对应 RocksDB 中的一个特定的列族。
    protected ColumnFamilyHandle columnFamily;
    // 默认状态值。当尝试获取一个不存在的状态时返回的值
    protected V defaultValue;
    // RocksDB 写入选项。
    // 包含了写入 RocksDB 时的配置（例如是否启用 WAL）
    protected final WriteOptions writeOptions;
    // 共享数据输出视图。一个可重用的序列化工具，用于将对象序列化到字节数组中，避免重复创建对象。
    protected final DataOutputSerializer dataOutputView;
    // 共享数据输入视图。一个可重用的反序列化工具，用于从字节数组中读取数据，避免重复创建对象。
    protected final DataInputDeserializer dataInputView;
    // 共享复合键构建器。
    // 一个优化的工具类，用于高效地将 Key Group、Key 和 Namespace 组合并序列化成 RocksDB 使用的复合字节键。
    private final SerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

    /**
     * Creates a new RocksDB backed state.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    protected AbstractRocksDBState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        this.namespaceSerializer = namespaceSerializer;
        this.backend = backend;

        this.columnFamily = columnFamily;

        this.writeOptions = backend.getWriteOptions();
        this.valueSerializer =
                Preconditions.checkNotNull(valueSerializer, "State value serializer");
        this.defaultValue = defaultValue;

        this.dataOutputView = new DataOutputSerializer(128);
        this.dataInputView = new DataInputDeserializer();
        this.sharedKeyNamespaceSerializer = backend.getSharedRocksKeyBuilder();
    }

    // ------------------------------------------------------------------------
    // 清除当前 Key/Namespace 对应的状态。
    // 它首先序列化当前 Key Group、Key 和 Namespace 组成的键，然后调用 RocksDB 的 delete() 方法，从对应的列族中移除该条目。
    @Override
    public void clear() {
        try {
            backend.db.delete(
                    columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace());
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException("Error while removing entry from RocksDB", e);
        }
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
    }
    // 获取序列化的状态值（主要用于 Queryable State）。
    // 这个方法接收一个包含 Key 和 Namespace 的序列化字节数组，计算 Key Group，构建完整的 RocksDB 键，并从 RocksDB 中获取对应的原始字节值。
    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception {

        // TODO make KvStateSerializer key-group aware to save this round trip and key-group
        // computation
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyAndNamespace.f0, backend.getNumberOfKeyGroups());

        SerializedCompositeKeyBuilder<K> keyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);
        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
        byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
        return backend.db.get(columnFamily, key);
    }
    // 序列化当前 Key Group + Key + Namespace + 额外的用户键。
    // 用于需要将 Key、Namespace 和状态本身的一些属性（如 List 状态的索引）一起作为复合键存储的场景。
    <UK> byte[] serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
            UK userKey, TypeSerializer<UK> userKeySerializer) throws IOException {
        return sharedKeyNamespaceSerializer.buildCompositeKeyNamesSpaceUserKey(
                currentNamespace, namespaceSerializer, userKey, userKeySerializer);
    }
    // 实际执行值的序列化，并将 dataOutputView 的缓冲区副本作为结果返回。
    private <T> byte[] serializeValueInternal(T value, TypeSerializer<T> serializer)
            throws IOException {
        serializer.serialize(value, dataOutputView);
        return dataOutputView.getCopyOfBuffer();
    }
    // 序列化当前 Key Group + Key + Namespace。
    // 这是最常用的方法，用于构造 RocksDB 的主键。它使用 sharedKeyNamespaceSerializer 来高效完成序列化。
    byte[] serializeCurrentKeyWithGroupAndNamespace() {
        return sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(
                currentNamespace, namespaceSerializer);
    }

    byte[] serializeValue(V value) throws IOException {
        return serializeValue(value, valueSerializer);
    }
    // 序列化一个可能为 null 的值。在序列化值之前，先写入一个布尔值标记其是否为 null。
    <T> byte[] serializeValueNullSensitive(T value, TypeSerializer<T> serializer)
            throws IOException {
        dataOutputView.clear();
        dataOutputView.writeBoolean(value == null);
        return serializeValueInternal(value, serializer);
    }

    <T> byte[] serializeValue(T value, TypeSerializer<T> serializer) throws IOException {
        dataOutputView.clear();
        return serializeValueInternal(value, serializer);
    }
    // 状态迁移核心逻辑。
    // 用于在状态序列化器发生变化时，将旧版本序列化的状态值反序列化（使用旧序列化器），然后重新序列化（使用新序列化器），
    // 从而完成状态数据的格式迁移。
    public void migrateSerializedValue(
            DataInputDeserializer serializedOldValueInput,
            DataOutputSerializer serializedMigratedValueOutput,
            TypeSerializer<V> priorSerializer,
            TypeSerializer<V> newSerializer)
            throws StateMigrationException {

        try {
            V value = priorSerializer.deserialize(serializedOldValueInput);
            newSerializer.serialize(value, serializedMigratedValueOutput);
        } catch (Exception e) {
            throw new StateMigrationException("Error while trying to migrate RocksDB state.", e);
        }
    }

    byte[] getKeyBytes() {
        return serializeCurrentKeyWithGroupAndNamespace();
    }
    // 获取给定状态值 V 的字节数组表示。
    byte[] getValueBytes(V value) {
        try {
            dataOutputView.clear();
            valueSerializer.serialize(value, dataOutputView);
            return dataOutputView.getCopyOfBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException("Error while serializing value", e);
        }
    }

    protected V getDefaultValue() {
        if (defaultValue != null) {
            return valueSerializer.copy(defaultValue);
        } else {
            return null;
        }
    }

    protected AbstractRocksDBState<K, N, V> setNamespaceSerializer(
            TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    protected AbstractRocksDBState<K, N, V> setValueSerializer(TypeSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    protected AbstractRocksDBState<K, N, V> setDefaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException(
                "Global state entry iterator is unsupported for RocksDb backend");
    }
}
