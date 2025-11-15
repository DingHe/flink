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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A read-only {@link ValueState} that does not allow for modifications.
 *
 * <p>This is the result returned when querying Flink's keyed state using the {@link
 * org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and providing
 * an {@link ValueStateDescriptor}.
 */
// 查询结果封装： 当外部客户端（例如使用 QueryableStateClient）向正在运行的 Flink 任务查询一个 ValueState 的值时，
// Flink 会将序列化后的状态数据发送给客户端。客户端使用这个类来封装和反序列化接收到的数据。
// 只读视图： 它实现了 Flink 的 ValueState<V> 接口，但其目的不是用于修改状态。它是对查询结果的一个不可变（Immutable） 的只读视图。
// 防止修改： 由于查询状态是离线的（即客户端通过网络获取到的只是一个状态的快照），对它进行任何修改都没有意义，也不会同步回 Flink 任务。因此，该类通过抛出异常来禁止所有修改操作 (update 和 clear)。

public final class ImmutableValueState<V> extends ImmutableState implements ValueState<V> {
    // 存储反序列化后的状态值
    private final V value;

    private ImmutableValueState(V value) {
        this.value = Preconditions.checkNotNull(value);
    }

    @Override
    public V value() {
        return value;
    }
    // 更新状态值（Write - 禁用）。
    @Override
    public void update(V newValue) {
        throw MODIFICATION_ATTEMPT_ERROR;
    }
    // 清除状态值（Write - 禁用）。
    @Override
    public void clear() {
        throw MODIFICATION_ATTEMPT_ERROR;
    }
    // 状态创建工厂方法
    // stateDescriptor：状态描述符，用于获取正确的序列化器。
    // serializedState：从 Flink TaskManager 接收到的序列化字节数组。
    @SuppressWarnings("unchecked")
    public static <V, S extends State> S createState(
            StateDescriptor<S, V> stateDescriptor, byte[] serializedState) throws IOException {
        final V state =
                KvStateSerializer.deserializeValue(
                        serializedState, stateDescriptor.getSerializer());
        return (S) new ImmutableValueState<>(state);
    }
}
