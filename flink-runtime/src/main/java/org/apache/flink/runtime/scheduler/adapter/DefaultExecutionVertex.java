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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.util.IterableUtils;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link SchedulingExecutionVertex}. */
// DefaultExecutionVertex 是 Flink 调度器适配层 (scheduler adapter) 的一部分，它的作用是为调度策略提供一个具体、可操作的顶点视图。
// 核心作用是实现 SchedulingExecutionVertex 接口中定义的各种方法，从而为 Flink 的调度策略 (Scheduling Strategy) 提供一个具体的、可查询的执行顶点对象。
// 存在是为了将调度策略与 Flink 运行时中的核心且复杂的 ExecutionVertex 对象进行解耦。DefaultExecutionVertex 充当了适配器（Adapter），
// 它不直接存储 ExecutionVertex 的所有复杂状态，而是通过引用或函数来访问所需信息（如状态），从而保持自身轻量化，并让调度器能够高效运行。
class DefaultExecutionVertex implements SchedulingExecutionVertex {
    // 顶点的唯一标识。
    // 对应 Flink 物理执行图中的一个任务实例的 ID。
    private final ExecutionVertexID executionVertexId;
    // 生产的结果分区列表。
    // 该顶点执行完成后会产生的数据分区列表。
    private final List<DefaultResultPartition> producedResults;
    // 状态的提供者。
    // 关键设计。
    // 它不是直接存储状态，而是通过一个 Supplier 函数式接口来实时获取外部 ExecutionVertex 的状态，避免了状态的冗余存储和同步问题。
    private final Supplier<ExecutionState> stateSupplier;
    // 消费的分区组列表。
    // 该顶点执行所需的输入依赖（上游生产的结果分区）。
    private final List<ConsumedPartitionGroup> consumedPartitionGroups;
    // 结果分区检索函数。
    // 关键设计。
    // 一个函数，用于根据给定的结果分区 ID（即输入 ID），从外部结构中获取对应的 DefaultResultPartition 实例。
    // 这用于动态解析输入依赖。
    private final Function<IntermediateResultPartitionID, DefaultResultPartition>
            resultPartitionRetriever;

    DefaultExecutionVertex(
            ExecutionVertexID executionVertexId,
            List<DefaultResultPartition> producedPartitions,
            Supplier<ExecutionState> stateSupplier,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {
        this.executionVertexId = checkNotNull(executionVertexId);
        this.stateSupplier = checkNotNull(stateSupplier);
        this.producedResults = checkNotNull(producedPartitions);
        this.consumedPartitionGroups = checkNotNull(consumedPartitionGroups);
        this.resultPartitionRetriever = checkNotNull(resultPartitionRetriever);
    }

    @Override
    public ExecutionVertexID getId() {
        return executionVertexId;
    }

    @Override
    public ExecutionState getState() {
        return stateSupplier.get();
    }

    @Override
    public Iterable<DefaultResultPartition> getConsumedResults() {
        return IterableUtils.flatMap(consumedPartitionGroups, resultPartitionRetriever);
    }

    @Override
    public List<ConsumedPartitionGroup> getConsumedPartitionGroups() {
        return consumedPartitionGroups;
    }

    @Override
    public Iterable<DefaultResultPartition> getProducedResults() {
        return producedResults;
    }
}
