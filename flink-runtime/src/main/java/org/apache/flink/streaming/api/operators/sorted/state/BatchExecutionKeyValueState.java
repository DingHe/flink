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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
// BatchExecutionKeyValueState<K, N, T> 类是 Flink 内部用于批处理执行模式下，实现 ValueState（值状态）的类。
// 该类的作用是为 Flink 批处理作业（特别是处理按键排序的数据流）提供一个高效、内存优化的 ValueState 实现。
// 单 Key 状态管理： 继承自 AbstractBatchExecutionKeyState，这意味着它一次只管理一个 Key 的状态。在批处理模式中，数据是按 Key 分组和排序的，当处理完一个 Key 的所有数据后，该状态实例会被清除并用于下一个 Key。
// 内存 Namespace 隔离： 它依赖父类的 HashMap (valuesForNamespaces) 来管理当前 Key 在不同 Namespace（通常代表不同的窗口）下的状态值，确保状态隔离。

/** A {@link ValueState} which keeps value for a single key at a time. */
class BatchExecutionKeyValueState<K, N, T> extends AbstractBatchExecutionKeyState<K, N, T>
        implements InternalValueState<K, N, T> {

    BatchExecutionKeyValueState(
            T defaultValue,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<T> stateTypeSerializer) {
        super(defaultValue, keySerializer, namespaceSerializer, stateTypeSerializer);
    }
    // 获取当前状态值
    @Override
    public T value() {
        return getOrDefault();
    }

    @Override
    public void update(T value) {
        setCurrentNamespaceValue(value);
    }

    @SuppressWarnings("unchecked")
    static <T, K, N, SV, S extends State, IS extends S> IS create(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc) {
        return (IS)
                new BatchExecutionKeyValueState<>(
                        stateDesc.getDefaultValue(),
                        keySerializer,
                        namespaceSerializer,
                        stateDesc.getSerializer());
    }
}
