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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collection;

/**
 * An {@link AggregatingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key
 * @param <N> The type of the namespace
 * @param <T> The type of the values that aggregated into the state
 * @param <ACC> The type of the value stored in the state (the accumulator type)
 * @param <R> The type of the value returned from the state
 */
// RocksDBAggregatingState<K, N, T, ACC, R> 是 Flink 聚合状态 (AggregatingState) 的具体实现，它使用 RocksDB 作为底层存储。
// 实现聚合逻辑： 它负责将流入的数据元素 (T)，
// 通过用户提供的 AggregateFunction 持续地合并到一个中间结果（累加器 ACC）中，并将这个累加器存储在 RocksDB 磁盘上。
// 持久化 Keyed State： 作为 Keyed State 的一种，状态的存取依赖于 Flink 的 Key (K) 和 Namespace (N)。这种基于磁盘的存储适用于需要处理巨大累计状态的应用。
// 支持窗口合并： 它实现了 InternalAggregatingState 接口，包含处理 Flink 窗口合并场景所需的 mergeNamespaces 逻辑。在合并时，它会取出源 Namespace 的累加器，使用 AggregateFunction 的 merge() 方法将其与目标 Namespace 的累加器进行合并，最终将合并结果写回 RocksDB。
// 实时转换输出： 在用户调用 get() 方法时，它读取 RocksDB 中的累加器 (ACC)，并立即使用 AggregateFunction 的 getResult() 方法将其转换为最终的输出结果 (R) 返回。
// <T>	输入元素的类型（添加到状态中的数据）。
// <ACC>	累加器 (Accumulator) 的类型（实际存储在 RocksDB 中的中间结果）
// <R>	最终结果 (Result) 的类型（get() 方法返回的类型）。
class RocksDBAggregatingState<K, N, T, ACC, R>
        extends AbstractRocksDBAppendingState<K, N, T, ACC, R>
        implements InternalAggregatingState<K, N, T, ACC, R> {

    /** User-specified aggregation function. */
    // 用户定义的聚合函数
    private AggregateFunction<T, ACC, R> aggFunction;

    /**
     * Creates a new {@code RocksDBAggregatingState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param aggFunction The aggregate function used for aggregating state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBAggregatingState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> valueSerializer,
            ACC defaultValue,
            AggregateFunction<T, ACC, R> aggFunction,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
        this.aggFunction = aggFunction;
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
    public TypeSerializer<ACC> getValueSerializer() {
        return valueSerializer;
    }
    // 1. 调用父类的 getInternal() 从 RocksDB 中读取累加器 (ACC)。
    // 2. 如果累加器存在，调用 aggFunction.getResult(accumulator) 将累加器转换为最终结果 R 并返回。
    // 3. 如果累加器为 null，返回 null。
    @Override
    public R get() throws IOException, RocksDBException {
        ACC accumulator = getInternal();
        if (accumulator == null) {
            return null;
        }
        return aggFunction.getResult(accumulator);
    }
    // 1. 读取当前 Key 和 Namespace 下的现有累加器 (ACC)。
    // 2. 如果累加器为 null，调用 aggFunction.createAccumulator() 创建一个新的累加器。
    // 3. 调用 aggFunction.add(value, accumulator) 将新值合并到累加器中。
    // 4. 调用父类的 updateInternal() 将新的累加器覆盖写入 RocksDB。
    @Override
    public void add(T value) throws IOException, RocksDBException {
        byte[] key = getKeyBytes();
        ACC accumulator = getInternal(key);
        accumulator = accumulator == null ? aggFunction.createAccumulator() : accumulator;
        updateInternal(key, aggFunction.add(value, accumulator));
    }
    // 1. 遍历所有源 Namespace (sources)。
    // 2. 从 RocksDB 读取每个源 Namespace 对应的累加器，并使用 aggFunction.merge(current, value) 将它们累积合并到一个 current 累加器中，并删除源状态。
    // 3. 如果 current 累加器非空，读取目标 Namespace (target) 的现有累加器。
    // 4. 再次调用 aggFunction.merge() 将源累积结果和目标现有结果合并。
    // 5. 将最终合并的累加器写入目标 Namespace 的 RocksDB 键下。
    @Override
    public void mergeNamespaces(N target, Collection<N> sources)
            throws IOException, RocksDBException {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        ACC current = null;

        // merge the sources to the target
        for (N source : sources) {
            if (source != null) {
                setCurrentNamespace(source);
                final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();
                final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);

                if (valueBytes != null) {
                    backend.db.delete(columnFamily, writeOptions, sourceKey);
                    dataInputView.setBuffer(valueBytes);
                    ACC value = valueSerializer.deserialize(dataInputView);

                    if (current != null) {
                        current = aggFunction.merge(current, value);
                    } else {
                        current = value;
                    }
                }
            }
        }

        // if something came out of merging the sources, merge it or write it to the target
        if (current != null) {
            setCurrentNamespace(target);
            // create the target full-binary-key
            final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();
            final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

            if (targetValueBytes != null) {
                // target also had a value, merge
                dataInputView.setBuffer(targetValueBytes);
                ACC value = valueSerializer.deserialize(dataInputView);

                current = aggFunction.merge(current, value);
            }

            // serialize the resulting value
            dataOutputView.clear();
            valueSerializer.serialize(current, dataOutputView);

            // write the resulting value
            backend.db.put(columnFamily, writeOptions, targetKey, dataOutputView.getCopyOfBuffer());
        }
    }

    RocksDBAggregatingState<K, N, T, ACC, R> setAggFunction(
            AggregateFunction<T, ACC, R> aggFunction) {
        this.aggFunction = aggFunction;
        return this;
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBAggregatingState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        ((AggregatingStateDescriptor<?, SV, ?>) stateDesc).getAggregateFunction(),
                        backend);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBAggregatingState<K, N, ?, SV, ?>) existingState)
                        .setAggFunction(
                                ((AggregatingStateDescriptor) stateDesc).getAggregateFunction())
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(registerResult.f1.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue());
    }
}
