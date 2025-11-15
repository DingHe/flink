/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/**
 * {@link ListState} implementation that stores state in RocksDB.
 *
 * <p>{@link EmbeddedRocksDBStateBackend} must ensure that we set the {@link
 * org.rocksdb.StringAppendOperator} on the column family that we use for our state since we use the
 * {@code merge()} call.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
// RocksDBListState<K, N, V> 是 Flink ListState (列表状态) 的一个实现，
// 它将状态数据存储在 RocksDB 键值存储中。它是 Flink EmbeddedRocksDBStateBackend 的核心组件之一。
// 持久化 Keyed State： 它是 Keyed State 的一种，状态的访问与 Flink 的 Key (K) 和 Namespace (N) 相关联。数据存储在磁盘上的 RocksDB 中，保证了状态的持久性和容错性。
// 利用 RocksDB 的 Merge 操作： 区别于其他状态类型在 RocksDB 中直接使用 put()，RocksDBListState 利用 RocksDB 的 merge() 操作和 StringAppendOperator（字符串追加操作符）来实现高效的列表元素追加。每次调用 add() 或 addAll() 并非重新写入整个列表，而是追加序列化的新元素，这对于大型列表状态非常高效。
// 列表序列化： 状态值在 RocksDB 中是作为一串用特殊分隔符（DELIMITER = ','）连接起来的序列化字节存储的。
// 它提供了基于磁盘的 Keyed List State 功能，适用于需要处理巨大状态而不能完全放入内存的 Flink 应用。
class RocksDBListState<K, N, V> extends AbstractRocksDBState<K, N, List<V>>
        implements InternalListState<K, N, V> {

    /** Serializer for the values. */
    // 元素序列化器。用于序列化和反序列化列表中的单个元素 $V$。
    private TypeSerializer<V> elementSerializer;
    // 列表分隔序列化器。
    // 专门负责将列表中的单个序列化元素用分隔符连接起来，以便存储在 RocksDB 的一个值中，并在读取时正确地反序列化回列表。
    private final ListDelimitedSerializer listSerializer;

    /** Separator of StringAppendTestOperator in RocksDB. */
    // 分隔符。
    // 用于在 RocksDB 中分隔列表中各个元素序列化字节的特殊字节（此处为 , 字节）。这是 RocksDB StringAppendOperator 工作的基础。
    private static final byte DELIMITER = ',';

    /**
     * Creates a new {@code RocksDBListState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBListState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<List<V>> valueSerializer,
            List<V> defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);

        ListSerializer<V> castedListSerializer = (ListSerializer<V>) valueSerializer;
        this.elementSerializer = castedListSerializer.getElementSerializer();
        this.listSerializer = new ListDelimitedSerializer();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {
        return valueSerializer;
    }
    // 获取状态（用户 API）。
    // 获取当前 Key 和 Namespace 下的整个列表。
    @Override
    public Iterable<V> get() throws IOException, RocksDBException {
        return getInternal();
    }
    // 1. 序列化当前 Key/Namespace 得到 RocksDB 键。
    // 2. 调用 backend.db.get() 获取 RocksDB 值（即序列化后的列表字节）。
    // 3. 使用 listSerializer.deserializeList() 将字节反序列化为 List<V> 并返回。
    @Override
    public List<V> getInternal() throws IOException, RocksDBException {
        byte[] key = serializeCurrentKeyWithGroupAndNamespace();
        byte[] valueBytes = backend.db.get(columnFamily, key);
        return listSerializer.deserializeList(valueBytes, elementSerializer);
    }
    // 1. 序列化当前 Key/Namespace。
    // 2. 序列化要添加的 $V$ 元素。
    // 3. 调用 backend.db.merge() 将新元素的序列化字节追加到 RocksDB 中现有值的末尾。
    @Override
    public void add(V value) throws IOException, RocksDBException {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

        backend.db.merge(
                columnFamily,
                writeOptions,
                serializeCurrentKeyWithGroupAndNamespace(),
                serializeValue(value, elementSerializer));
    }
    // 合并命名空间。
    // 将多个源 Namespace 的状态合并到目标 Namespace。
    // 主要用于窗口状态合并。
    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        try {
            // create the target full-binary-key
            setCurrentNamespace(target);
            final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();

            // merge the sources to the target
            for (N source : sources) {
                if (source != null) {
                    setCurrentNamespace(source);
                    final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();

                    byte[] valueBytes = backend.db.get(columnFamily, sourceKey);

                    if (valueBytes != null) {
                        backend.db.delete(columnFamily, writeOptions, sourceKey);
                        backend.db.merge(columnFamily, writeOptions, targetKey, valueBytes);
                    }
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
        }
    }
    // 覆盖状态。
    // 用新的列表完全替换旧的列表状态。
    @Override
    public void update(List<V> valueToStore) throws IOException, RocksDBException {
        updateInternal(valueToStore);
    }
    // 内部覆盖状态。
    // 用新的列表完全替换旧的列表状态。
    @Override
    public void updateInternal(List<V> values) throws IOException, RocksDBException {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        if (!values.isEmpty()) {
            backend.db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(),
                    listSerializer.serializeList(values, elementSerializer));
        } else {
            clear();
        }
    }
    // 添加多个元素。
    // 将多个元素追加到列表状态中。
    @Override
    public void addAll(List<V> values) throws IOException, RocksDBException {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        if (!values.isEmpty()) {
            backend.db.merge(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(),
                    listSerializer.serializeList(values, elementSerializer));
        }
    }

    @Override
    public void migrateSerializedValue(
            DataInputDeserializer serializedOldValueInput,
            DataOutputSerializer serializedMigratedValueOutput,
            TypeSerializer<List<V>> priorSerializer,
            TypeSerializer<List<V>> newSerializer)
            throws StateMigrationException {

        Preconditions.checkArgument(priorSerializer instanceof ListSerializer);
        Preconditions.checkArgument(newSerializer instanceof ListSerializer);

        TypeSerializer<V> priorElementSerializer =
                ((ListSerializer<V>) priorSerializer).getElementSerializer();

        TypeSerializer<V> newElementSerializer =
                ((ListSerializer<V>) newSerializer).getElementSerializer();

        try {
            while (serializedOldValueInput.available() > 0) {
                V element =
                        ListDelimitedSerializer.deserializeNextElement(
                                serializedOldValueInput, priorElementSerializer);
                newElementSerializer.serialize(element, serializedMigratedValueOutput);
                if (serializedOldValueInput.available() > 0) {
                    serializedMigratedValueOutput.write(DELIMITER);
                }
            }
        } catch (Exception e) {
            throw new StateMigrationException(
                    "Error while trying to migrate RocksDB list state.", e);
        }
    }

    @Override
    protected RocksDBListState<K, N, V> setValueSerializer(
            TypeSerializer<List<V>> valueSerializer) {
        super.setValueSerializer(valueSerializer);
        this.elementSerializer = ((ListSerializer<V>) valueSerializer).getElementSerializer();
        return this;
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBListState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        (TypeSerializer<List<E>>) registerResult.f1.getStateSerializer(),
                        (List<E>) stateDesc.getDefaultValue(),
                        backend);
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBListState<K, N, E>) existingState)
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(
                                (TypeSerializer<List<E>>) registerResult.f1.getStateSerializer())
                        .setDefaultValue((List<E>) stateDesc.getDefaultValue());
    }

    static class StateSnapshotTransformerWrapper<T> implements StateSnapshotTransformer<byte[]> {
        private final StateSnapshotTransformer<T> elementTransformer;
        private final TypeSerializer<T> elementSerializer;
        private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;
        private final ListDelimitedSerializer listSerializer;
        private final DataInputDeserializer in = new DataInputDeserializer();

        StateSnapshotTransformerWrapper(
                StateSnapshotTransformer<T> elementTransformer,
                TypeSerializer<T> elementSerializer) {
            this.elementTransformer = elementTransformer;
            this.elementSerializer = elementSerializer;
            this.listSerializer = new ListDelimitedSerializer();
            this.transformStrategy =
                    elementTransformer instanceof CollectionStateSnapshotTransformer
                            ? ((CollectionStateSnapshotTransformer<?>) elementTransformer)
                                    .getFilterStrategy()
                            : CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
        }

        @Override
        @Nullable
        public byte[] filterOrTransform(@Nullable byte[] value) {
            if (value == null) {
                return null;
            }
            List<T> result = new ArrayList<>();
            in.setBuffer(value);
            T next;
            int prevPosition = 0;
            try {
                while ((next =
                                ListDelimitedSerializer.deserializeNextElement(
                                        in, elementSerializer))
                        != null) {
                    T transformedElement = elementTransformer.filterOrTransform(next);
                    if (transformedElement != null) {
                        if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
                            return Arrays.copyOfRange(value, prevPosition, value.length);
                        } else {
                            result.add(transformedElement);
                        }
                    }
                    prevPosition = in.getPosition();
                }
                return result.isEmpty()
                        ? null
                        : listSerializer.serializeList(result, elementSerializer);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Failed to serialize transformed list", e);
            }
        }
    }
}
