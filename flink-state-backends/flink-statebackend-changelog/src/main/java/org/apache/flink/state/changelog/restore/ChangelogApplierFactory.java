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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * {@link StateChangeApplier} factory. It's purpose is to decouple restore/apply logic from state
 * logic.
 */
// ChangelogApplierFactory 接口是 Flink 状态变更日志（State Changelog）机制中的一个关键组件，用于解耦状态恢复/应用逻辑与具体状态实现逻辑。
// 核心作用是根据不同的内部 Keyed State 类型，创建相应的 StateChangeApplier（状态变更应用器）实例。
// 在 Flink 的状态变更日志（State Changelog）模式下，算子状态的变更会被记录下来。当 Flink Job 从 Checkpoint/Savepoint 恢复时，系统需要将这些记录的变更重新应用到状态后端。
// 解耦 (Decoupling)： 状态后端的实现（例如 Heap 或 RocksDB）不需要直接知道如何应用变更日志；它们只需要通过这个工厂来获取应用器。
// 插件化 (Pluggable)： 允许不同的状态后端（或不同的恢复机制）提供自己的 ChangelogApplierFactory 实现，从而定制状态变更的应用方式。


@Internal
public interface ChangelogApplierFactory {
    // 为内部 Map State 创建状态变更应用器。
    <K, N, UK, UV> KvStateChangeApplier<K, N> forMap(
            InternalMapState<K, N, UK, UV> map, InternalKeyContext<K> keyContext);
    // 为内部 Value State 创建状态变更应用器。
    <K, N, T> KvStateChangeApplier<K, N> forValue(
            InternalValueState<K, N, T> value, InternalKeyContext<K> keyContext);
    // 为内部 List State 创建状态变更应用器。
    <K, N, T> KvStateChangeApplier<K, N> forList(
            InternalListState<K, N, T> list, InternalKeyContext<K> keyContext);
    // 为内部 Reducing State 创建状态变更应用器。
    <K, N, T> KvStateChangeApplier<K, N> forReducing(
            InternalReducingState<K, N, T> reducing, InternalKeyContext<K> keyContext);
    // 为内部 Aggregating State 创建状态变更应用器。
    <K, N, IN, SV, OUT> KvStateChangeApplier<K, N> forAggregating(
            InternalAggregatingState<K, N, IN, SV, OUT> aggregating,
            InternalKeyContext<K> keyContext);
    // 为内部 优先级队列状态 创建状态变更应用器。
    <T> StateChangeApplier forPriorityQueue(
            KeyGroupedInternalPriorityQueue<T> priorityQueue, TypeSerializer<T> serializer);
}
