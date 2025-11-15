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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * A read-only view of the {@link BroadcastState}.
 *
 * <p>Although read-only, the user code should not modify the value returned by the {@link
 * #get(Object)} or the entries of the immutable iterator returned by the {@link
 * #immutableEntries()}, as this can lead to inconsistent states. The reason for this is that we do
 * not create extra copies of the elements for performance reasons.
 *
 * @param <K> The key type of the elements in the {@link ReadOnlyBroadcastState}.
 * @param <V> The value type of the elements in the {@link ReadOnlyBroadcastState}.
 */
// 提供了一个只读视图来访问 Flink 广播流（Broadcast Stream） 中的状态
// 实现广播模式： 在 Flink 的 Broadcast State Pattern 中，一个数据流（通常是主数据流）与一个广播流进行连接（Connect）。
// 只读访问： 该接口被用于非广播流所连接的算子（通常是 BroadcastProcessFunction 或 KeyedBroadcastProcessFunction）中。这些算子只能读取广播状态的数据，而不能修改它，以确保状态在所有并行的算子实例中保持一致。
@PublicEvolving
public interface ReadOnlyBroadcastState<K, V> extends State {

    /**
     * Returns the current value associated with the given key.
     *
     * <p>The user code must not modify the value returned, as this can lead to inconsistent states.
     *
     * @param key The key of the mapping
     * @return The value of the mapping with the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 获取指定 Key 对应的当前值。
    V get(K key) throws Exception;

    /**
     * Returns whether there exists the given mapping.
     *
     * @param key The key of the mapping
     * @return True if there exists a mapping whose key equals to the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 检查指定 Key 是否存在。
    // 用于快速判断广播状态中是否包含给定键的映射关系，避免不必要的 get() 调用。
    boolean contains(K key) throws Exception;

    /**
     * Returns an immutable {@link Iterable} over the entries in the state.
     *
     * <p>The user code must not modify the entries of the returned immutable iterator, as this can
     * lead to inconsistent states.
     */
    // 获取所有状态条目的只读迭代器。
    Iterable<Map.Entry<K, V>> immutableEntries() throws Exception;
}
