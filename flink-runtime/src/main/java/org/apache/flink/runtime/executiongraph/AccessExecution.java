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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Optional;

/** Common interface for the runtime {@link Execution} and {@link ArchivedExecution}. */
// AccessExecution 是 Flink runtime.executiongraph 包中的一个公共接口，
// 旨在提供对 Flink 任务**单次执行尝试（Execution Attempt）**状态和元数据的统一访问方式。
// 该接口的主要目的是在 Flink 的 执行图 (ExecutionGraph) 架构中，提供一个标准化的视图来查看任务的运行时信息，无论是当前正在运行的执行 (Execution) 还是已归档的历史执行 (ArchivedExecution)。
// Execution： 代表任务当前正在 TaskManager 上运行的活动尝试。
// ArchivedExecution： 代表任务已完成、已失败或已被取消的历史尝试记录。
// 通过实现这个接口，Flink 的监控、调度和故障排查组件可以无需区分是实时查看还是回顾历史，使用统一的方法来获取任务尝试的关键信息。
//
public interface AccessExecution {
    /**
     * Returns the {@link ExecutionAttemptID} for this Execution.
     *
     * @return ExecutionAttemptID for this execution
     */
    // 获取执行尝试 ID。
    // 返回该任务这次执行尝试的唯一标识符 ExecutionAttemptID。
    // 在任务失败重试时，ExecutionAttemptID 会变化，但父级任务 ID (ExecutionVertexID) 保持不变
    ExecutionAttemptID getAttemptId();

    /**
     * Returns the attempt number for this execution.
     *
     * @return attempt number for this execution.
     */
    // 获取尝试编号。
    // 返回该任务的执行尝试次数（从 0 开始计数）。如果任务是第一次运行，则返回 0；如果是第一次重试，则返回 1，以此类推。
    int getAttemptNumber();

    /**
     * Returns the timestamps for every {@link ExecutionState}.
     * @return timestamps for each state
     */
    // 获取状态起始时间戳数组。
    // 返回一个数组，
    // 记录任务在每次进入特定 ExecutionState (如 CREATED, DEPLOYING, RUNNING 等) 时的 Unix 时间戳。
    // 用于计算任务在每个状态中的持续时间。
    long[] getStateTimestamps();

    /**
     * Returns the end timestamps for every {@link ExecutionState}.
     * @return timestamps for each state
     */
    // 获取状态结束时间戳数组。
    // 返回一个数组，记录任务在每次退出特定 ExecutionState 时的 Unix 时间戳。
    // 辅助分析任务在不同状态停留的时长。
    long[] getStateEndTimestamps();

    /**
     * Returns the current {@link ExecutionState} for this execution.
     * @return execution state for this execution
     */
    // 获取当前执行状态。
    // 返回该执行尝试当前的 ExecutionState（例如：RUNNING、FINISHED、FAILED）。
    ExecutionState getState();

    /**
     * Returns the {@link TaskManagerLocation} for this execution.
     * @return taskmanager location for this execution.
     */
    // 获取分配的资源位置。
    // 返回运行该任务的 TaskManagerLocation 信息，包括 TaskManager 的主机地址和端口等。
    // 用于查看任务执行在哪个物理或逻辑节点上。
    TaskManagerLocation getAssignedResourceLocation();

    /**
     * Returns the exception that caused the job to fail. This is the first root exception that was
     * not recoverable and triggered job failure.
     * @return an {@code Optional} of {@link ErrorInfo} containing the {@code Throwable} and the
     *     time it was registered if an error occurred. If no error occurred an empty {@code
     *     Optional} will be returned.
     */
    // 获取失败信息
    // 返回一个 Optional<ErrorInfo>。如果任务失败，则包含导致失败的异常信息和失败时间；
    // 如果任务未失败，则返回空 Optional
    Optional<ErrorInfo> getFailureInfo();

    /**
     * Returns the timestamp for the given {@link ExecutionState}.
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     */
    // 获取指定状态的起始时间戳。
    long getStateTimestamp(ExecutionState state);

    /**
     * Returns the end timestamp for the given {@link ExecutionState}.
     *
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     */
    // 获取指定状态的结束时间戳。
    long getStateEndTimestamp(ExecutionState state);

    /**
     * Returns the user-defined accumulators as strings.
     * @return user-defined accumulators as strings.
     */
    // 获取用户累加器结果。
    StringifiedAccumulatorResult[] getUserAccumulatorsStringified();

    /**
     * Returns the subtask index of this execution.
     * @return subtask index of this execution.
     */
    // 获取并行子任务索引。
    // 返回该执行尝试在整个 JobVertex 的并行实例中的索引（从 0 开始）。例如，如果并行度为 10，则索引范围为 0 到 9。
    int getParallelSubtaskIndex();
    // 返回该任务执行的输入/输出（I/O）相关的性能指标，如吞吐量、反压状态、记录数等。用于性能监控和调优。
    IOMetrics getIOMetrics();
}
