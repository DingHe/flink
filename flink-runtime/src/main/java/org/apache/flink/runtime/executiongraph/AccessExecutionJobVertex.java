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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

/**
 * Common interface for the runtime {@link ExecutionJobVertex} and {@link
 * ArchivedExecutionJobVertex}.
 */
// Flink 运行时数据结构 ExecutionJobVertex 和其归档版本 ArchivedExecutionJobVertex 的通用访问接口。
// 统一视图： 无论作业是在运行中（由 ExecutionJobVertex 实时代表）还是在归档历史中（由 ArchivedExecutionJobVertex 代表），外部组件（如 Web UI、REST API 或度量系统）都可以通过这个接口获取一致的元数据和聚合状态。
// 抽象层： 它抽象了底层实现细节，允许调用者专注于获取 Job Vertex 级别的信息，如并行度、资源配置和所有并行子任务的聚合状态。
// Job 级别聚合： 它提供了对所有并行子任务 (ExecutionVertex) 状态的聚合视图。
public interface AccessExecutionJobVertex {
    /**
     * Returns the name for this job vertex.
     *
     * @return name for this job vertex.
     */
    // 获取作业顶点名称。
    // 返回该逻辑算子（如 Map、Filter、Source）的用户友好名称。常用于日志和监控。
    String getName();

    /**
     * Returns the parallelism for this job vertex.
     *
     * @return parallelism for this job vertex.
     */
    // 获取并行度。
    // 返回该作业顶点配置的并行度，即它将拥有的并行子任务 (ExecutionVertex) 数量。
    int getParallelism();

    /**
     * Returns the max parallelism for this job vertex.
     *
     * @return max parallelism for this job vertex.
     */
    // 获取最大并行度。
    // 返回该作业顶点允许的最大并行度。这在动态扩展（Rescaling）时非常重要。
    int getMaxParallelism();

    /**
     * Returns the slot sharing group for this job vertex.
     *DataStream<String> processed = source.map(new MyMapFunction());    processed.getTransformation().setSlotSharingGroup("processingGroup");
     * @return slot sharing group for this job vertex.
     */
    // 获取槽位共享组。
    // 返回该作业顶点所属的槽位共享组。该组内的任务可以共享 TaskManager 上的一个物理 Slot 资源。
    SlotSharingGroup getSlotSharingGroup();

    /**
     * Returns the resource profile for this job vertex.
     *
     * @return resource profile for this job vertex.  包括 CPU、内存等资源需求
     */
    // 获取资源配置。
    // 返回该顶点所有并行子任务所需的资源配置文件，通常包括 CPU 核心数、任务堆内存等。
    ResourceProfile getResourceProfile();

    /**
     * Returns the {@link JobVertexID} for this job vertex.
     *
     * @return JobVertexID for this job vertex.
     */
    // 获取作业顶点 ID。
    // 返回该逻辑顶点在整个作业图中的唯一标识符。
    JobVertexID getJobVertexId();

    /**
     * Returns all execution vertices for this job vertex.
     * @return all execution vertices for this job vertex
     */
    // 获取所有执行顶点。
    // 返回该逻辑顶点下所有并行子任务实例（ExecutionVertex）的只读访问接口数组。
    AccessExecutionVertex[] getTaskVertices();

    /**
     * Returns the aggregated {@link ExecutionState} for this job vertex.
     *
     * @return aggregated state for this job vertex
     */
    // 获取聚合状态。 返回该 Job Vertex 所有并行子任务状态的聚合视图（如 RUNNING、FINISHED）。
    // 例如，如果所有子任务都完成了，聚合状态就是 FINISHED；如果有任何一个失败，聚合状态可能是 FAILED 或 FAILING。
    ExecutionState getAggregateState();

    /**
     * Returns the aggregated user-defined accumulators as strings.
     *
     * @return aggregated user-defined accumulators as strings.
     */
    // 获取聚合的用户累计器结果。
    // 返回所有并行子任务报告的用户自定义累计器（Accumulators）的最终聚合结果，并以字符串格式表示。
    StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified();
}
