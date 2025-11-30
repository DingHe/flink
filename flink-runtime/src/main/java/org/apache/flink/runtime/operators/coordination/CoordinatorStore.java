/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * {@link CoordinatorStore} can be used for sharing some information among {@link
 * OperatorCoordinator} instances. Motivating example is/was combining/aggregating latest watermark
 * emitted by different sources in order to do the watermark alignment.
 *
 * <p>Implementations of this interface must ensure that all operations are atomic.
 */
// CoordinatorStore 接口是 Flink 中设计用于 OperatorCoordinator 实例之间共享信息的通用存储抽象。
// 信息共享： 它提供了一个线程安全的、类似 Map 的存储机制，允许属于同一作业的不同 OperatorCoordinator 实例（例如，不同的 Source 协调器或 UDF 协调器）之间进行通信和状态共享。
// 原子操作： 该接口强调所有操作（如 putIfAbsent、compute）都必须是原子性的 (@ThreadSafe)，以确保在并发环境中（多个协调器线程访问同一个存储）数据的一致性。
// 动机示例： 正如注释中所述，一个典型的应用场景是水印对齐。
// 不同的 Source 任务（拥有各自的 SourceCoordinator）需要将其最新的水印信息共享和聚合，以便 JobMaster 能够计算出全局的最小水印或进行对齐。
// 背景： 在 Flink 的新协调模型中，OperatorCoordinator 在 JobMaster 侧运行，负责管理和恢复其算子实例的状态。
// CoordinatorStore 允许这些逻辑上独立的协调器实例之间建立数据层面的联系。
@ThreadSafe
@Internal
public interface CoordinatorStore {
    // 检查键是否存在。
    // 返回存储中是否包含指定的键。
    boolean containsKey(Object key);
    // 检查键是否存在。
    // 返回存储中是否包含指定的键。
    Object get(Object key);
    // 原子性插入（如果不存在）。
    // 只有当键在存储中不存在时，才将给定的值与键关联并插入。
    Object putIfAbsent(Object key, Object value);
    // 原子性计算（如果存在）。
    // 如果指定的键存在，则使用键和当前值调用提供的 remappingFunction 来计算新值，并替换旧值。
    // 如果计算结果为 null，则移除该键。
    // 如果键不存在，则不执行任何操作并返回 null。
    Object computeIfPresent(Object key, BiFunction<Object, Object, Object> remappingFunction);
    // 原子性计算（通用）。
    // 使用提供的 mappingFunction 计算新值，并将新值与指定的键关联。
    Object compute(Object key, BiFunction<Object, Object, Object> mappingFunction);
    // 原子性应用函数。
    // 针对指定键所关联的值执行提供的 consumer 函数，并返回函数的结果 (R)
    <R> R apply(Object key, Function<Object, R> consumer);
}
