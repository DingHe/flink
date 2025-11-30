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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Group of consumer {@link ExecutionVertexID}s. One such a group corresponds to one {@link
 * ConsumedPartitionGroup}.
 */
// ConsumerVertexGroup（消费者顶点组）是 Flink 调度策略 (SchedulingStrategy) 内部使用的核心概念，
// 用于逻辑上将消费同一组上游结果分区（即同一个 ConsumedPartitionGroup）的所有下游执行顶点 (ExecutionVertexID) 关联起来。
// 逻辑分组： 将消费相同上游数据的一组并行任务实例（执行顶点）聚合在一起。
// 映射桥梁： 作为连接下游消费者（ExecutionVertexID）与上游被消费数据（ConsumedPartitionGroup）的桥梁，简化调度器对数据依赖关系的跟踪。
// 描述分区类型： 记录这组消费者消费的数据分区类型（如 PIPELINED, BLOCKING 等），这对调度决策至关重要。
public class ConsumerVertexGroup implements Iterable<ExecutionVertexID> {
    // 消费者顶点列表。
    // 存储了所有消费同一组上游分区的下游并行任务的唯一标识符 (ExecutionVertexID)。
    private final List<ExecutionVertexID> vertices;
    // 结果分区类型。
    // 记录这组消费者所依赖的上游中间结果分区的类型（例如，BLOCKING, PIPELINED）。
    // 这是调度策略做出决策的关键信息。
    private final ResultPartitionType resultPartitionType;
    // 被消费的分区组。
    // 引用了这组消费者正在消费的上游结果分区组的实例。
    // 在对象创建时可以为 null，但必须在后续设置。
    @Nullable private ConsumedPartitionGroup consumedPartitionGroup;

    private ConsumerVertexGroup(
            List<ExecutionVertexID> vertices, ResultPartitionType resultPartitionType) {
        this.vertices = vertices;
        this.resultPartitionType = resultPartitionType;
    }

    public static ConsumerVertexGroup fromMultipleVertices(
            List<ExecutionVertexID> vertices, ResultPartitionType resultPartitionType) {
        return new ConsumerVertexGroup(vertices, resultPartitionType);
    }

    public static ConsumerVertexGroup fromSingleVertex(
            ExecutionVertexID vertex, ResultPartitionType resultPartitionType) {
        return new ConsumerVertexGroup(Collections.singletonList(vertex), resultPartitionType);
    }

    public ResultPartitionType getResultPartitionType() {
        return resultPartitionType;
    }

    @Override
    public Iterator<ExecutionVertexID> iterator() {
        return vertices.iterator();
    }

    public int size() {
        return vertices.size();
    }

    public boolean isEmpty() {
        return vertices.isEmpty();
    }

    public ExecutionVertexID getFirst() {
        return iterator().next();
    }

    public ConsumedPartitionGroup getConsumedPartitionGroup() {
        return checkNotNull(consumedPartitionGroup, "ConsumedPartitionGroup is not properly set.");
    }

    public void setConsumedPartitionGroup(ConsumedPartitionGroup consumedPartitionGroup) {
        checkState(this.consumedPartitionGroup == null);
        this.consumedPartitionGroup = checkNotNull(consumedPartitionGroup);
    }
}
