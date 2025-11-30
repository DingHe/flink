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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.topology.Vertex;

import java.util.List;

/** Scheduling representation of {@link ExecutionVertex}. */
// 为了让你更好地理解，我们需要区分“执行图中的实际对象”和“调度器眼中的对象”。这个接口属于**调度器（Scheduler）**的视角。
// SchedulingExecutionVertex 是 Flink 执行顶点 (ExecutionVertex) 在调度策略中的抽象表示。
// 在 Flink 运行时，ExecutionVertex 是一个非常庞大且复杂的对象，它包含了任务执行的所有细节（如所在的 TaskManager、Slot、Checkpoint 状态、统计信息等）。
// 然而，调度策略（SchedulingStrategy）（决定何时启动下一个任务的逻辑）并不需要知道所有这些细节。
// 它只需要关心：“这个任务现在的状态是什么？”以及“它的输入数据准备好了吗？”。
// SchedulingExecutionVertex 就是为了满足调度策略的需求而抽离出来的接口，它屏蔽了底层的复杂性。
// 它是调度器用来判断“一个任务是否可以被调度执行”的依据对象。
// 该接口定义了调度器最关心的两个核心信息：状态和输入依赖。


public interface SchedulingExecutionVertex
        extends Vertex<
                ExecutionVertexID,
                IntermediateResultPartitionID,
                SchedulingExecutionVertex,
                SchedulingResultPartition> {

    /**
     * Gets the state of the execution vertex.
     *
     * @return state of the execution vertex
     */
    // 获取当前执行顶点的生命周期状态。
    ExecutionState getState();

    /**
     * Gets the {@link ConsumedPartitionGroup}s.
     *
     * @return list of {@link ConsumedPartitionGroup}s
     */
    // 获取该顶点需要消费的所有分区组（Partition Groups）
    List<ConsumedPartitionGroup> getConsumedPartitionGroups();
}
