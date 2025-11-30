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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.runtime.topology.Topology;

import java.util.List;

/** Topology of {@link SchedulingExecutionVertex}. */
// SchedulingTopology 接口是 Flink 调度器（Scheduler）用来查看和操作整个作业执行图的最高级抽象。
// 它是 Flink 调度策略（例如自适应批处理、流式调度）的核心输入。
// 调度策略的视图： SchedulingTopology 接口提供了 Flink 执行拓扑图（ExecutionGraph）的一个轻量级、面向调度的视图。
// 它将底层复杂的 ExecutionGraph 映射成了一系列简化且易于查询的 SchedulingExecutionVertex 和 SchedulingResultPartition。
public interface SchedulingTopology
        extends Topology<
                ExecutionVertexID,
                IntermediateResultPartitionID,
                SchedulingExecutionVertex,
                SchedulingResultPartition,
                SchedulingPipelinedRegion> {

    /**
     * Looks up the {@link SchedulingExecutionVertex} for the given {@link ExecutionVertexID}.
     *
     * @param executionVertexId identifying the respective scheduling vertex
     * @return The respective scheduling vertex
     * @throws IllegalArgumentException If the vertex does not exist
     */
    // 根据执行顶点 ID 查找对应的调度执行顶点对象。
    // 用于获取拓扑中特定的 SchedulingExecutionVertex 实例。
    SchedulingExecutionVertex getVertex(ExecutionVertexID executionVertexId);

    /**
     * Looks up the {@link SchedulingResultPartition} for the given {@link
     * IntermediateResultPartitionID}.
     *
     * @param intermediateResultPartitionId identifying the respective scheduling result partition
     * @return The respective scheduling result partition
     * @throws IllegalArgumentException If the partition does not exist
     */
    // 根据中间结果分区 ID 查找对应的调度结果分区对象。
    // 用于获取拓扑中特定的 SchedulingResultPartition 实例，这是调度器判断输入依赖是否满足的入口。
    SchedulingResultPartition getResultPartition(
            IntermediateResultPartitionID intermediateResultPartitionId);

    /**
     * Register a scheduling topology listener. The listener will be notified by {@link
     * SchedulingTopologyListener#notifySchedulingTopologyUpdated(SchedulingTopology, List)} when
     * the scheduling topology is updated.
     *
     * @param listener the registered listener.
     */
    // 注册一个调度拓扑监听器。
    // 允许外部组件（例如 Flink 的 调度策略 或其他监控服务）注册一个 SchedulingTopologyListener 实例。
    // 这提供了一种观察者模式机制，确保所有依赖拓扑信息的组件都能及时获得更新通知。
    void registerSchedulingTopologyListener(SchedulingTopologyListener listener);
}
