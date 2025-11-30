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

import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import static org.apache.flink.util.Preconditions.checkNotNull;

// 要回顾一下 TaskExecutionState。
// TaskExecutionState 是 TaskManager 汇报给 JobManager 的“状态报告”。
// 而 TaskExecutionStateTransition 是 JobManager 内部处理这个报告时使用的**“增强版指令包”。
// 它不仅包含状态报告本身，还附加了“接下来该做什么”**的操作指令。
// 封装状态更新：它内部包裹了 TaskExecutionState，包含了任务的基础信息（ID、新状态、异常、指标等）。
/** Wraps {@link TaskExecutionState}, along with actions to take if it is FAILED state. */
public class TaskExecutionStateTransition {
    // 被包装的原始数据，
    // 包含了 ExecutionAttemptID（尝试ID）、ExecutionState（如 RUNNING, FAILED）、异常信息以及累加器和 IO 指标。它是真正承载“发生了什么”的数据源。
    private final TaskExecutionState taskExecutionState;

    /**
     * Indicating whether to send a RPC call to remove task from TaskManager. True if the failure is
     * fired by JobManager and the execution is already deployed. Otherwise it should be false.
     */
    // 是否取消远程任务标记。
    // true: 表示这次状态变更是由 JobManager 主动发起的（例如超时判断），且任务在 TaskManager 上可能还在运行。因此，系统后续需要发起一个 RPC 调用通知 TaskManager 停止该任务。
    // false: 表示这次状态变更来自 TaskManager 的汇报（它自己挂了），或者任务还未部署。此时不需要多此一举去取消它。
    private final boolean cancelTask;
    // 是否释放分区标记
    // true: 表示在处理这次状态变更（通常是失败或取消）时，该任务产生的所有中间结果分区（Result Partitions）应该被立即清理释放。
    private final boolean releasePartitions;

    public TaskExecutionStateTransition(final TaskExecutionState taskExecutionState) {
        this(taskExecutionState, false, false);
    }

    public TaskExecutionStateTransition(
            final TaskExecutionState taskExecutionState,
            final boolean cancelTask,
            final boolean releasePartitions) {

        this.taskExecutionState = checkNotNull(taskExecutionState);
        this.cancelTask = cancelTask;
        this.releasePartitions = releasePartitions;
    }

    public Throwable getError(ClassLoader userCodeClassloader) {
        return taskExecutionState.getError(userCodeClassloader);
    }

    public ExecutionAttemptID getID() {
        return taskExecutionState.getID();
    }

    public ExecutionState getExecutionState() {
        return taskExecutionState.getExecutionState();
    }

    public AccumulatorSnapshot getAccumulators() {
        return taskExecutionState.getAccumulators();
    }

    public IOMetrics getIOMetrics() {
        return taskExecutionState.getIOMetrics();
    }

    public boolean getCancelTask() {
        return cancelTask;
    }

    public boolean getReleasePartitions() {
        return releasePartitions;
    }
}
