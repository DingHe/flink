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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Set;

/**
 * Component which encapsulates the scheduling logic. It can react to execution state changes and
 * partition consumable events. Moreover, it is responsible for resolving task failures.
 */
// SchedulingStrategy 接口在 Flink 的新调度框架中扮演着核心调度逻辑的封装者角色。
// 定义调度规则： 它抽象了作业中任务（ExecutionVertex）应该何时启动、如何响应运行时事件的逻辑。
// 响应状态变化： 它负责监听 Flink 作业运行时发生的关键事件，例如任务状态改变（如从 DEPLOYING 到 RUNNING）或数据分区变得可用。
// 处理故障恢复： 它包含了解决任务失败并决定哪些任务需要重新启动的逻辑。
// 不同的 Flink 调度模式（如经典的批处理调度、流式调度、以及Pipelined Region 调度等）会实现这个接口以提供特定的调度行为。
// 例如，批处理可能使用阶段性（Staged）调度，而流处理可能使用**所有任务立即调度（Eager）**的策略。
public interface SchedulingStrategy {

    /** Called when the scheduling is started (initial scheduling operation). */
    // 启动调度
    // 在 JobMaster 开始执行作业时首次调用。
    // 它触发调度策略的初始调度操作，例如对于流作业，可能会立即调度所有任务；对于批处理作业，可能会调度第一个执行阶段的任务。
    void startScheduling();

    /**
     * Called whenever vertices need to be restarted (due to task failure).
     *
     * @param verticesToRestart The tasks need to be restarted
     */
    // 重启任务。
    // 在发生任务失败或作业重启时调用。它接收一个需要重启的执行顶点 ID 集合。
    // 调度策略需要根据自身的恢复逻辑（例如，是否需要重启整个 Region 或仅重启失败任务），将这些顶点重新调度到 TaskManager 上。
    void restartTasks(Set<ExecutionVertexID> verticesToRestart);

    /**
     * Called whenever an {@link Execution} changes its state.
     *
     * @param executionVertexId The id of the task
     * @param executionState The new state of the execution
     */
    // 响应执行状态变化。
    // 当某个任务的执行状态 (ExecutionState) 发生变化时调用。调度策略会根据这个新状态决定下一步操作。例如：
    // 状态变为 FAILED 时：触发故障恢复逻辑。
    void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState);

    /**
     * Called whenever an {@link IntermediateResultPartition} becomes consumable.
     *
     * @param resultPartitionId The id of the result partition
     */
    // 响应分区可用事件。
    // 当一个中间结果分区 (IntermediateResultPartition) 的数据变得可供下游任务消费时调用。
    // 这对于流式调度或基于数据流动的批处理调度（如 Pipeline 调度）非常重要，它通常会触发消费该分区的下游任务的部署。
    void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId);

    /**
     * Schedules all vertices and excludes any vertices that are already finished or whose inputs
     * are not yet ready.
     */
    // 如果可能，调度所有顶点。
    // 用于指示调度策略尝试调度所有尚未完成且输入已就绪的执行顶点。
    // 如果某个调度策略（如基于批处理的阶段性调度）不应支持一次性调度所有顶点，则会抛出 UnsupportedOperationException（这是默认实现）。
    default void scheduleAllVerticesIfPossible() {
        throw new UnsupportedOperationException();
    }
}
