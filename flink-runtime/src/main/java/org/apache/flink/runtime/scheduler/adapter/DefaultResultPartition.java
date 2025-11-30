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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link SchedulingResultPartition}. */
// DefaultResultPartition 类是接口 SchedulingResultPartition 的默认实现，代表了 Flink 调度拓扑中的一条数据流边。
// 调度视角下的数据流： 它抽象了由上游任务（DefaultExecutionVertex）产生，并被下游任务消费的中间结果分区 (Intermediate Result Partition)。
// 依赖判断核心： 它是调度器判断任务启动依赖的关键对象。调度器通过查询它的状态（ResultPartitionState）来确定数据是否已经准备好（例如，上游是否已完成）。
// 适配器模式： 与 DefaultExecutionVertex 类似，它是一个适配器，将底层复杂、实时变化的执行图结果分区信息，转换为调度策略所需的高效、简化的视图。
// 它使用 Supplier 函数式接口来动态获取状态和消费者信息，从而避免了数据冗余和同步问题。
class DefaultResultPartition implements SchedulingResultPartition {
    // 分区的唯一 ID。
    // 标识这个结果分区在整个 Flink 作业中的唯一身份。
    private final IntermediateResultPartitionID resultPartitionId;
    // 所属数据集 ID。
    // 标识该分区属于哪个逻辑中间结果集（IntermediateDataSet）。
    // 一个数据集可能包含多个分区。
    private final IntermediateDataSetID intermediateDataSetId;
    // 分区类型。
    // 定义了数据的传输和存储方式，例如：BLOCKING（阻塞/批处理）、PIPELINED_BOUNDED（有界流式）、PIPELINED_UNBOUNDED（无界流式）。
    private final ResultPartitionType partitionType;
    // 状态提供者。
    // 核心设计。 用于实时获取结果分区的当前状态（如 CREATED 可消费，ALL_DATA_PRODUCED 所有的数据已经产生）。
    private final Supplier<ResultPartitionState> resultPartitionStateSupplier;
    // 生产者顶点。
    // 生产该结果分区的上游任务实例。这是一个非 final 属性，通过 setProducer 方法设置，用于双向关联。
    private DefaultExecutionVertex producer;
    // 消费者组提供者 (顶点)。
    // 用于动态获取消费该结果分区的所有下游顶点组。
    private final Supplier<List<ConsumerVertexGroup>> consumerVertexGroupsSupplier;

    private final Supplier<List<ConsumedPartitionGroup>> consumerPartitionGroupSupplier;

    DefaultResultPartition(
            IntermediateResultPartitionID partitionId,
            IntermediateDataSetID intermediateDataSetId,
            ResultPartitionType partitionType,
            Supplier<ResultPartitionState> resultPartitionStateSupplier,
            Supplier<List<ConsumerVertexGroup>> consumerVertexGroupsSupplier,
            Supplier<List<ConsumedPartitionGroup>> consumerPartitionGroupSupplier) {
        this.resultPartitionId = checkNotNull(partitionId);
        this.intermediateDataSetId = checkNotNull(intermediateDataSetId);
        this.partitionType = checkNotNull(partitionType);
        this.resultPartitionStateSupplier = checkNotNull(resultPartitionStateSupplier);
        this.consumerVertexGroupsSupplier = checkNotNull(consumerVertexGroupsSupplier);
        this.consumerPartitionGroupSupplier = checkNotNull(consumerPartitionGroupSupplier);
    }

    @Override
    public IntermediateResultPartitionID getId() {
        return resultPartitionId;
    }

    @Override
    public IntermediateDataSetID getResultId() {
        return intermediateDataSetId;
    }

    @Override
    public ResultPartitionType getResultType() {
        return partitionType;
    }

    @Override
    public ResultPartitionState getState() {
        return resultPartitionStateSupplier.get();
    }

    @Override
    public DefaultExecutionVertex getProducer() {
        return producer;
    }

    @Override
    public List<ConsumerVertexGroup> getConsumerVertexGroups() {
        return checkNotNull(consumerVertexGroupsSupplier.get());
    }

    @Override
    public List<ConsumedPartitionGroup> getConsumedPartitionGroups() {
        return consumerPartitionGroupSupplier.get();
    }

    void setProducer(DefaultExecutionVertex vertex) {
        producer = checkNotNull(vertex);
    }
}
