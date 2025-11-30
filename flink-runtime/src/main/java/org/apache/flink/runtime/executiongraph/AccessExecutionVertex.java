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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Optional;

/** Common interface for the runtime {@link ExecutionVertex} and {@link ArchivedExecutionVertex}. */
// AccessExecutionVertex 是 Flink 执行图 (ExecutionGraph) 中的一个公共接口，
// 用于提供对 一个并行子任务（即 JobVertex 的一个并行实例）状态和元数据的统一访问。
// 该接口的作用是为 Flink 的监控、调度和恢复组件提供一个标准化的、统一的视图，来访问单个并行子任务的生命周期信息，无论该信息是来自：
// 当前运行时的执行顶点 (ExecutionVertex)：代表 JobGraph 中一个任务的一个并行实例。
// 已归档的历史执行顶点 (ArchivedExecutionVertex)：代表已完成 Job 的历史记录中的一个并行实例。
// 它代表了 JobGraph 中一个 JobVertex 的特定并行实例（如 MapFunction (1/4)），并聚合了该子任务的所有执行尝试信息和当前状态。
public interface AccessExecutionVertex {
    /**
     * Returns the name of this execution vertex in the format "myTask (2/7)".
     *
     * @return name of this execution vertex
     */
    // 获取包含子任务索引的任务名称。
    // 返回一个格式化的字符串，清晰地标识出任务名称及其并行度索引，例如 "myTask (2/7)"。这对于用户界面和日志记录非常有用。
    String getTaskNameWithSubtaskIndex();

    /**
     * Returns the subtask index of this execution vertex.
     *
     * @return subtask index of this execution vertex.
     */
    // 获取并行子任务索引。
    // 返回该执行顶点在整个任务组（JobVertex）中的并行索引（从 0 开始）。
    int getParallelSubtaskIndex();

    /**
     * Returns the current execution for this execution vertex.
     *
     * @return current execution
     */
    // 获取当前执行尝试。
    // 返回最近或当前正在活动的 AccessExecution 实例。这个实例包含了该子任务当前这次执行尝试的详细信息（如尝试 ID、状态时间戳等）。
    AccessExecution getCurrentExecutionAttempt();

    /**
     * Returns the current executions for this execution vertex. The returned collection must
     * contain the current execution attempt.
     *
     * @return current executions
     */
    // 获取当前活动的执行尝试集合。
    // 返回该执行顶点当前所有活动的执行尝试集合。在不支持并发执行尝试（supportsConcurrentExecutionAttempts）的场景中，
    // 集合中通常只包含一个元素，即 getCurrentExecutionAttempt() 返回的结果。
    <T extends AccessExecution> Collection<T> getCurrentExecutions();

    /**
     * Returns the current {@link ExecutionState} for this execution vertex.
     *
     * @return execution state for this execution vertex
     */
    // 获取执行状态。
    // 返回该执行顶点当前的聚合 ExecutionState（例如：DEPLOYING、RUNNING、FAILED）。
    // 这个状态通常由当前活动的执行尝试的状态决定。
    ExecutionState getExecutionState();

    /**
     * Returns the timestamp for the given {@link ExecutionState}.
     *
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     */
    // 获取指定状态的时间戳。
    // 返回该并行子任务进入指定 ExecutionState 的时间戳。
    long getStateTimestamp(ExecutionState state);

    /**
     * Returns the exception that caused the job to fail. This is the first root exception that was
     * not recoverable and triggered job failure.
     *
     * @return failure exception wrapped in an {@code Optional} of {@link ErrorInfo}, or an empty
     *     {@link Optional} if no exception was caught.
     */
    Optional<ErrorInfo> getFailureInfo();

    /**
     * Returns the {@link TaskManagerLocation} for this execution vertex.
     *
     * @return taskmanager location for this execution vertex.
     */
    TaskManagerLocation getCurrentAssignedResourceLocation();

    /**
     * Returns the execution history.
     *
     * @return the execution history
     */
    ExecutionHistory getExecutionHistory();
}
