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

import java.util.List;

/**
 * {@link State} interface for partitioned list state in Operations. The state is accessed and
 * modified by user functions, and checkpointed consistently by the system as part of the
 * distributed snapshots.
 *
 * <p>The state can be a keyed list state or an operator list state.
 *
 * <p>When it is a keyed list state, it is accessed by functions applied on a {@code KeyedStream}.
 * The key is automatically supplied by the system, so the function always sees the value mapped to
 * the key of the current element. That way, the system can handle stream and state partitioning
 * consistently together.
 *
 * <p>When it is an operator list state, the list is a collection of state items that are
 * independent from each other and eligible for redistribution across operator instances in case of
 * changed operator parallelism.
 *
 * @param <T> Type of values that this list state keeps.
 */
// ListState<T> 是 Flink 状态接口体系中用于存储元素列表的核心接口，它支持分区列表状态（Partitioned List State）
// 存储集合： 允许用户将多个元素（类型为 $T$）顺序地添加到状态中，并以列表或可迭代对象（Iterable<T>）的形式获取所有元素。
// 支持两种状态类型：
//Keyed List State（分区列表状态）： 状态的存取依赖于当前处理数据流的 Key。每个 Key 都有自己独立的列表。
// Operator List State（算子列表状态）： 状态独立于 Key，属于算子实例本身。在算子的并行度发生变化时，这些状态项可以被 Flink 重新分配给新的算子实例，以实现弹性伸缩。
@PublicEvolving
public interface ListState<T> extends MergingState<T, Iterable<T>> {

    /**
     * Updates the operator state accessible by {@link #get()} by updating existing values to the
     * given list of values. The next time {@link #get()} is called (for the same state partition)
     * the returned state will represent the updated list.
     *
     * <p>If an empty list is passed in, the state value will be null.
     *
     * <p>Null value passed in or any null value in list is not allowed.
     *
     * @param values The new values for the state.
     * @throws Exception The method may forward exception thrown internally (by I/O or functions, or
     *     sanity check for null value).
     */
    // 用给定的整个 List 替换当前 Key 和 Namespace 下的所有现有状态。
    // 如果传入空列表，状态将被清空 (null)。
    void update(List<T> values) throws Exception;

    /**
     * Updates the operator state accessible by {@link #get()} by adding the given values to
     * existing list of values. The next time {@link #get()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>If an empty list is passed in, the state value remains unchanged.
     *
     * <p>Null value passed in or any null value in list is not allowed.
     *
     * @param values The new values to be added to the state.
     * @throws Exception The method may forward exception thrown internally (by I/O or functions, or
     *     sanity check for null value).
     */
    // 追加列表：将给定的 List 中的所有元素追加到现有状态的末尾。如果传入空列表，状态保持不变。
    void addAll(List<T> values) throws Exception;
}
