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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@code ArchivedExecutionVertex} is a readonly representation of {@link ExecutionVertex}. */
// ArchivedExecutionVertex 类是 Flink 执行图 (ExecutionGraph) 中 ExecutionVertex 的一个只读（Readonly）、**可序列化（Serializable）**的快照或归档表示。
// 当 Flink 的 JobManager/Dispatcher 需要将作业的运行时状态发送给外部请求者（如 Web UI、REST API 或其他监控组件）时，它不能直接发送正在运行中的、可能随时发生变化的 ExecutionVertex 对象。
// ArchivedExecutionVertex 提供了以下功能：
// 快照（Snapshot）： 它在创建时捕获了原始 ExecutionVertex 的所有关键信息（包括当前执行尝试和历史记录）。
// 线程安全： 保证外部访问者获取到的信息是不可变的，不会因为任务在后台的运行和状态变更而引发并发问题。
// 归档记录： 作为 AccessExecutionVertex 接口的实现，它使得查看历史或当前任务状态的操作得到了统一。
// 它是 Flink 任务中单个并行子任务（例如 MapFunction (1/4)）在其生命周期某一点的静态、不可变的数据记录。
public class ArchivedExecutionVertex implements AccessExecutionVertex, Serializable {

    private static final long serialVersionUID = -6708241535015028576L;
    // 子任务索引。
    // 缓存了该执行顶点在整个 JobVertex 中的并行索引（例如 0 到 $N-1$）。
    private final int subTaskIndex;
    // 执行历史记录。
    // 存储了该子任务自创建以来所有执行尝试的完整历史记录的副本。使用副本是为了确保归档对象是不可变的。
    private final ExecutionHistory executionHistory;

    /** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations. */
    private final String taskNameWithSubtask;
    // 当前执行尝试的归档。
    private final ArchivedExecution currentExecution; // this field must never be null
    // 当前执行尝试集合的归档。
    // 包含当前所有活动的（或最近完成的）执行尝试的归档列表。在构造函数中，它会遍历并归档所有并发的 Execution 实例。
    private final Collection<AccessExecution> currentExecutions;

    // ------------------------------------------------------------------------

    public ArchivedExecutionVertex(ExecutionVertex vertex) {
        this.subTaskIndex = vertex.getParallelSubtaskIndex();
        this.executionHistory = getCopyOfExecutionHistory(vertex);
        this.taskNameWithSubtask = vertex.getTaskNameWithSubtaskIndex();

        Execution vertexCurrentExecution = vertex.getCurrentExecutionAttempt();
        ArrayList<AccessExecution> currentExecutionList =
                new ArrayList<>(vertex.getCurrentExecutions().size());
        currentExecution = vertexCurrentExecution.archive();
        currentExecutionList.add(currentExecution);
        for (Execution execution : vertex.getCurrentExecutions()) {
            if (execution != vertexCurrentExecution) {
                currentExecutionList.add(execution.archive());
            }
        }
        currentExecutions = Collections.unmodifiableList(currentExecutionList);
    }

    @VisibleForTesting
    public ArchivedExecutionVertex(
            int subTaskIndex,
            String taskNameWithSubtask,
            ArchivedExecution currentExecution,
            ExecutionHistory executionHistory) {
        this.subTaskIndex = subTaskIndex;
        this.taskNameWithSubtask = checkNotNull(taskNameWithSubtask);
        this.currentExecution = checkNotNull(currentExecution);
        this.executionHistory = checkNotNull(executionHistory);
        this.currentExecutions = Collections.singletonList(currentExecution);
    }

    // --------------------------------------------------------------------------------------------
    //   Accessors
    // --------------------------------------------------------------------------------------------

    @Override
    public String getTaskNameWithSubtaskIndex() {
        return this.taskNameWithSubtask;
    }

    @Override
    public int getParallelSubtaskIndex() {
        return this.subTaskIndex;
    }

    @Override
    public ArchivedExecution getCurrentExecutionAttempt() {
        return currentExecution;
    }

    @Override
    public Collection<AccessExecution> getCurrentExecutions() {
        return currentExecutions;
    }

    @Override
    public ExecutionState getExecutionState() {
        return currentExecution.getState();
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        return currentExecution.getStateTimestamp(state);
    }

    @Override
    public Optional<ErrorInfo> getFailureInfo() {
        return currentExecution.getFailureInfo();
    }

    @Override
    public TaskManagerLocation getCurrentAssignedResourceLocation() {
        return currentExecution.getAssignedResourceLocation();
    }

    @Override
    public ExecutionHistory getExecutionHistory() {
        return executionHistory;
    }

    static ExecutionHistory getCopyOfExecutionHistory(ExecutionVertex executionVertex) {
        return new ExecutionHistory(executionVertex.getExecutionHistory());
    }
}
