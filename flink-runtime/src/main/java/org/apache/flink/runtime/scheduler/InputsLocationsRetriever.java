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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Component to retrieve the inputs locations of an {@link ExecutionVertex}. */
// （输入位置检索器）是 Flink 调度器中用于查询任务 输入数据依赖 和 输入数据所在位置 的关键组件。
// 调度依赖检查： 它允许调度器查询一个任务需要消费哪些上游结果分区 (ConsumedPartitionGroup)，以及这些分区是由哪些具体的上游任务 (Producer) 产生的。
// 数据本地性优化： 它还允许调度器查询上游任务当前部署在哪个 TaskManager 上。这是为了实现数据本地性调度（Data Locality Scheduling）：
// 如果一个任务能够读取与其输入数据在同一 TaskManager 上的数据，则应优先将该任务调度到该 TaskManager 上，以减少网络传输，提高效率。

public interface InputsLocationsRetriever {

    /**
     * Get the consumed result partition groups of an execution vertex.
     *
     * @param executionVertexId identifies the execution vertex
     * @return the consumed result partition groups
     */
    // 获取指定执行顶点需要消费的所有结果分区组。
    // 参数： executionVertexId，需要查询输入依赖的下游任务 ID。
    // 在 Flink 中，一个输入可能由多个上游分区构成，这些分区被逻辑地归为一组。调度器用此信息来检查所有输入依赖是否已满足（即上游数据是否已就绪）。
    Collection<ConsumedPartitionGroup> getConsumedPartitionGroups(
            ExecutionVertexID executionVertexId);

    /**
     * Get the producer execution vertices of a consumed result partition group.
     *
     * @param consumedPartitionGroup the consumed result partition group
     * @return the ids of producer execution vertices
     */
    // 获取特定输入分区组的所有生产者（即上游任务）。
    // 参数： consumedPartitionGroup，一个输入分区组对象
    Collection<ExecutionVertexID> getProducersOfConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup);

    /**
     * Get the task manager location future for an execution vertex.
     *
     * @param executionVertexId identifying the execution vertex
     * @return the task manager location future
     */
    // 获取指定执行顶点（即上游生产者）当前或预期的 TaskManager 部署位置。
    Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
            ExecutionVertexID executionVertexId);
}
