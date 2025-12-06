/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.topology.PipelinedRegion;

/** Pipelined region on execution level, i.e., {@link ExecutionGraph} level. */
// SchedulingPipelinedRegion 接口是 Flink 调度器在 ExecutionGraph 级别对 **流水线区域（Pipelined Region）**的抽象和扩展。
// 定义调度单元：在 Flink 的批处理模式（或现代流处理的 Region 调度）中，JobGraph/ExecutionGraph 被划分为互不依赖的区域。SchedulingPipelinedRegion 就是这些区域之一，它表示一个可以整体性、流水线式地启动和运行的子图。
// 细化区域边界：它继承了通用的 PipelinedRegion，并用调度层特定的类型（如 ExecutionVertexID, SchedulingExecutionVertex 等）进行参数化，确保所有操作都发生在正确的调度上下文中。
// 识别阻塞依赖：它提供了获取区域外部非流水线（阻塞）依赖和由调度器释放的依赖的方法，这对于控制区域的启动顺序、资源释放以及实现高效的**区域级调度（Region-based Scheduling）**至关重要。
public interface SchedulingPipelinedRegion
        extends PipelinedRegion<
                ExecutionVertexID,
                IntermediateResultPartitionID,
                SchedulingExecutionVertex,
                SchedulingResultPartition> {
    /**
     * Get all distinct blocking {@link ConsumedPartitionGroup}s.
     *
     * @return set of {@link ConsumedPartitionGroup}s
     */
    // 获取所有不以流水线方式（即阻塞式）消耗的分区组。
    Iterable<ConsumedPartitionGroup> getAllNonPipelinedConsumedPartitionGroups();

    /**
     * Get all distinct releaseByScheduler {@link ConsumedPartitionGroup}s.
     *
     * @return set of {@link ConsumedPartitionGroup}s
     */
    // 获取所有需要由调度器明确释放的消耗分区组。
    Iterable<ConsumedPartitionGroup> getAllReleaseBySchedulerConsumedPartitionGroups();
}
