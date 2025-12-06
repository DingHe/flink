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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The slot assignment for an {@link Execution}. */
// 用于封装一个特定的任务尝试 (Execution) 与其被分配的执行资源槽 (LogicalSlot) 之间的关联关系。
// 核心功能： 记录哪个任务正在等待或已经获得了哪个资源槽。
// 异步性： 由于资源槽的分配通常是一个异步过程（需要向资源管理器请求或等待 TaskManager 响应），因此它使用 CompletableFuture 来表示资源槽的获取状态。
class ExecutionSlotAssignment {
    // 任务尝试 ID。
    // 标识了尝试占用此资源槽的那个具体的任务执行实例。
    // 一个任务可能有多次执行尝试（例如失败重试），每个尝试都有唯一的 ID。
    private final ExecutionAttemptID executionAttemptId;
    // 表示异步获取 LogicalSlot 的结果。
    // 当这个 Future 完成时，意味着该任务已经成功获取了执行资源槽，可以开始部署任务了。
    private final CompletableFuture<LogicalSlot> logicalSlotFuture;

    ExecutionSlotAssignment(
            ExecutionAttemptID executionAttemptId,
            CompletableFuture<LogicalSlot> logicalSlotFuture) {
        this.executionAttemptId = checkNotNull(executionAttemptId);
        this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
    }

    ExecutionAttemptID getExecutionAttemptId() {
        return executionAttemptId;
    }

    CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
        return logicalSlotFuture;
    }
}
