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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
// 为所有基于 RocksDB 存储的、支持增量添加元素或累加值的 Keyed State 提供了通用的基础设施和基本操作。具体的状态类型，如 ListState (列表状态), ReducingState (归约状态),
// 和 AggregatingState (聚合状态) 的 RocksDB 实现，都会继承这个抽象类。
// <IN>	可以增量添加到状态中的元素的类型（例如 ListState 中的单个元素）。
// <SV>	状态的序列化值 (Serialized Value) 类型，即存储在 RocksDB 中的最终类型（例如 ListState 对应 List<V>）。
// <OUT>	可以从状态中检索出来的当前累计结果的类型（通常与 SV 相同，或通过转换得到）。
abstract class AbstractRocksDBAppendingState<K, N, IN, SV, OUT>
        extends AbstractRocksDBState<K, N, SV>
        implements InternalAppendingState<K, N, IN, SV, OUT> {

    /**
     * Creates a new RocksDB backend appending state.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    protected AbstractRocksDBAppendingState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> valueSerializer,
            SV defaultValue,
            RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
    }
    // 获取内部状态（当前 Key/Namespace）。
    // 从 RocksDB 中读取当前 Key 和 Namespace 对应的状态值。
    @Override
    public SV getInternal() throws IOException, RocksDBException {
        return getInternal(getKeyBytes());
    }
    // 获取内部状态（指定键）。
    // 给定 RocksDB 完整键字节，获取对应的状态值。
    SV getInternal(byte[] key) throws IOException, RocksDBException {
        byte[] valueBytes = backend.db.get(columnFamily, key);
        if (valueBytes == null) {
            return null;
        }
        dataInputView.setBuffer(valueBytes);
        return valueSerializer.deserialize(dataInputView);
    }

    @Override
    public void updateInternal(SV valueToStore) throws RocksDBException {
        updateInternal(getKeyBytes(), valueToStore);
    }

    void updateInternal(byte[] key, SV valueToStore) throws RocksDBException {
        // write the new value to RocksDB
        backend.db.put(columnFamily, writeOptions, key, getValueBytes(valueToStore));
    }
}
