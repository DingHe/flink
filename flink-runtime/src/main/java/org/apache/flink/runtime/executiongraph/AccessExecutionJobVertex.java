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
 * ArchivedExecutionJobVertex}.  表示执行图中的一个逻辑顶点（JobVertex）及其运行时相关信息
 */
public interface AccessExecutionJobVertex {
    /**
     * Returns the name for this job vertex.
     *
     * @return name for this job vertex.  通常用来标识作业的逻辑阶段，比如 "Source: Kafka", "Sink: HDFS"
     */
    String getName();

    /**
     * Returns the parallelism for this job vertex.
     *
     * @return parallelism for this job vertex.  决定数据处理的吞吐量和并行处理能力
     */
    int getParallelism();

    /**
     * Returns the max parallelism for this job vertex.
     *
     * @return max parallelism for this job vertex.
     */
    int getMaxParallelism();

    /**
     * Returns the slot sharing group for this job vertex.
     *DataStream<String> processed = source.map(new MyMapFunction());    processed.getTransformation().setSlotSharingGroup("processingGroup");
     * @return slot sharing group for this job vertex.  槽共享组定义了哪些顶点可以共享同一个 TaskManager 插槽，共享插槽的任务在一个 TaskManager 中运行，减少了跨网络的通信开销，同时优化了资源利用
     */
    SlotSharingGroup getSlotSharingGroup();

    /**
     * Returns the resource profile for this job vertex.
     *
     * @return resource profile for this job vertex.  包括 CPU、内存等资源需求
     */
    ResourceProfile getResourceProfile();

    /**
     * Returns the {@link JobVertexID} for this job vertex.
     *
     * @return JobVertexID for this job vertex.
     */
    JobVertexID getJobVertexId();

    /**
     * Returns all execution vertices for this job vertex.
     * ExecutionVertex 是 Flink 的调度和运行时的核心单位，它对应于一个并行任务实例（即一个子任务），一个 JobVertex 的并行度为 N，就会对应 N 个 ExecutionVertex
     * @return all execution vertices for this job vertex
     */
    AccessExecutionVertex[] getTaskVertices();

    /**
     * Returns the aggregated {@link ExecutionState} for this job vertex.
     *
     * @return aggregated state for this job vertex  这是所有执行顶点（ExecutionVertex）状态的整体视图
     */
    ExecutionState getAggregateState();

    /**
     * Returns the aggregated user-defined accumulators as strings.
     *
     * @return aggregated user-defined accumulators as strings.
     */
    StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified();
}
