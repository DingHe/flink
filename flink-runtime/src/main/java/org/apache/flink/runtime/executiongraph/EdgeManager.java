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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Class that manages all the connections between tasks. */
// EdgeManager 类是 Flink 执行图（ExecutionGraph）中的一个核心工具，其作用是管理执行顶点（任务实例）之间的数据依赖关系，即执行图中的边（Edge）。
// 生产者到消费者： 跟踪每个中间结果分区 (IntermediateResultPartitionID) 被哪些消费者顶点组 (ConsumerVertexGroup) 所消费。
// 消费者到输入： 跟踪每个执行顶点 (ExecutionVertexID) 需要消费哪些被消费分区组 (ConsumedPartitionGroup) 作为输入。
// 分区到分区组： 维护分区与其所属的 ConsumedPartitionGroup 之间的映射，方便通过单个分区 ID 查找其所属的逻辑组。
// 通过这些映射，EdgeManager 使得 Flink 调度器能够快速查询和遍历任务之间的数据流依赖关系，这对于调度决策、故障恢复和资源清理至关重要。
public class EdgeManager {
    // 分区 -> 消费者映射。
    // 存储了每个中间结果分区 ID 被哪些下游消费者顶点组所消费。
    // 用于判断上游分区完成后，哪些下游任务组可以被调度或通知。
    private final Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers =
            new HashMap<>();
    // 顶点 -> 输入映射。
    // 存储了每个执行顶点 ID 需要消费哪些被消费分区组（作为输入）。用于判断任务的输入依赖是否满足。
    private final Map<ExecutionVertexID, List<ConsumedPartitionGroup>> vertexConsumedPartitions =
            new HashMap<>();
    // 分区 -> 分区组映射。 存储了每个中间结果分区 ID 所属的被消费分区组。
    // 一个分区可以属于多个 ConsumedPartitionGroup 实例（虽然不常见，但结构允许），这个映射保证了通过单个分区 ID 也能找到其逻辑分组。
    private final Map<IntermediateResultPartitionID, List<ConsumedPartitionGroup>>
            consumedPartitionsById = new HashMap<>();
    // 连接生产者分区和消费者组。
    // 在 partitionConsumers 映射中，为给定的上游分区 ID 注册一个新的下游消费者顶点组 (ConsumerVertexGroup)。
    public void connectPartitionWithConsumerVertexGroup(
            IntermediateResultPartitionID resultPartitionId,
            ConsumerVertexGroup consumerVertexGroup) {

        checkNotNull(consumerVertexGroup);

        List<ConsumerVertexGroup> groups =
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId);
        groups.add(consumerVertexGroup);
    }
    // 连接消费者顶点和输入分区组。
    // 在 vertexConsumedPartitions 映射中，为给定的消费者顶点 ID 注册一个新的输入被消费分区组 (ConsumedPartitionGroup)。
    public void connectVertexWithConsumedPartitionGroup(
            ExecutionVertexID executionVertexId, ConsumedPartitionGroup consumedPartitionGroup) {

        checkNotNull(consumedPartitionGroup);

        final List<ConsumedPartitionGroup> consumedPartitions =
                getConsumedPartitionGroupsForVertexInternal(executionVertexId);

        consumedPartitions.add(consumedPartitionGroup);
    }
    // 返回或者创建分区消费者组
    private List<ConsumerVertexGroup> getConsumerVertexGroupsForPartitionInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertexInternal(
            ExecutionVertexID executionVertexId) {
        return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
    }

    public List<ConsumerVertexGroup> getConsumerVertexGroupsForPartition(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId));
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertex(
            ExecutionVertexID executionVertexId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsForVertexInternal(executionVertexId));
    }
    // 注册被消费分区组。
    // 遍历 ConsumedPartitionGroup 中的所有分区 ID，并将该 ConsumedPartitionGroup 实例注册到 consumedPartitionsById 映射中。
    // 这保证了可以通过组内的任一分区 ID 访问到整个分区组。
    public void registerConsumedPartitionGroup(ConsumedPartitionGroup group) {
        for (IntermediateResultPartitionID partitionId : group) {
            consumedPartitionsById
                    .computeIfAbsent(partitionId, ignore -> new ArrayList<>())
                    .add(group);
        }
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsByIdInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return consumedPartitionsById.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsByIdInternal(resultPartitionId));
    }

    public int getNumberOfConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return getConsumedPartitionGroupsByIdInternal(resultPartitionId).size();
    }
}
