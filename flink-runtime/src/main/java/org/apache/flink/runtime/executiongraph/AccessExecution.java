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
public interface AccessExecution {
    /**
     * Returns the {@link ExecutionAttemptID} for this Execution.
     *
     * @return ExecutionAttemptID for this execution
     */
    ExecutionAttemptID getAttemptId();

    /**
     * Returns the attempt number for this execution.
     *
     * @return attempt number for this execution. 返回该执行的尝试编号，从 0 开始
     */
    int getAttemptNumber();

    /**
     * Returns the timestamps for every {@link ExecutionState}.
     *记录任务在每种状态（如 DEPLOYING, RUNNING, FAILED 等）的开始时间，用于任务跟踪和监控
     * @return timestamps for each state
     */
    long[] getStateTimestamps();

    /**
     * Returns the end timestamps for every {@link ExecutionState}.
     *辅助分析任务在不同状态停留的时长
     * @return timestamps for each state
     */
    long[] getStateEndTimestamps();

    /**
     * Returns the current {@link ExecutionState} for this execution.
     *返回此执行的当前状态
     * @return execution state for this execution
     */
    ExecutionState getState();

    /**
     * Returns the {@link TaskManagerLocation} for this execution.
     *用于查看任务执行在哪个物理或逻辑节点上，便于排查调度问题
     * @return taskmanager location for this execution.
     */
    TaskManagerLocation getAssignedResourceLocation();

    /**
     * Returns the exception that caused the job to fail. This is the first root exception that was
     * not recoverable and triggered job failure.
     *该方法返回一个包含失败异常和失败时间的 Optional，如果任务未失败则返回空
     * @return an {@code Optional} of {@link ErrorInfo} containing the {@code Throwable} and the
     *     time it was registered if an error occurred. If no error occurred an empty {@code
     *     Optional} will be returned.
     */
    Optional<ErrorInfo> getFailureInfo();

    /**
     * Returns the timestamp for the given {@link ExecutionState}.
     *返回指定状态的结束时间戳
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     */
    long getStateTimestamp(ExecutionState state);

    /**
     * Returns the end timestamp for the given {@link ExecutionState}.
     *
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     */
    long getStateEndTimestamp(ExecutionState state);

    /**
     * Returns the user-defined accumulators as strings.
     *用户在任务中可能自定义一些指标（如统计某个数据流的总量），此方法用于返回这些结果
     * @return user-defined accumulators as strings.
     */
    StringifiedAccumulatorResult[] getUserAccumulatorsStringified();

    /**
     * Returns the subtask index of this execution.
     *Flink 的任务通常是并行运行的，每个子任务都有唯一的索引。此方法帮助确定当前执行的是哪个子任务
     * @return subtask index of this execution.
     */
    int getParallelSubtaskIndex();
   //分析任务的输入输出性能（如吞吐量、延迟等），用于性能调优
    IOMetrics getIOMetrics();
}
