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

import java.util.List;
import java.util.Map;

/** Component responsible for assigning slots to a collection of {@link Execution}. */
// 专门负责管理和执行将执行资源槽 (Slot) 分配给具体任务执行实例 (Execution) 的过程。
// 调度核心： 当 Flink 决定运行一批任务时，它会告诉这个分配器：“请为这些任务找到可用的资源。”
// 输入： 待执行的任务尝试 ID 列表。
// 输出： 每个任务尝试 ID 对应的 ExecutionSlotAssignment，这是一个异步对象，承诺未来会提供任务执行所需的 LogicalSlot。
public interface ExecutionSlotAllocator {

    /**
     * Allocate slots for the given executions.
     *
     * @param executionAttemptIds executions to allocate slots for
     * @return Map of slot assignments to the executions
     */
    // 量请求资源槽位，为指定的任务执行实例分配资源
    // executionAttemptIds (List<ExecutionAttemptID>): 待分配槽位的任务尝试 ID 列表。
    Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds);

    /**
     * Cancel the ongoing slot request of the given {@link Execution}.
     *
     * @param executionAttemptId identifying the {@link Execution} of which the slot request should
     *     be canceled.
     */
    // 取消指定任务的正在进行的槽位请求。
    void cancel(ExecutionAttemptID executionAttemptId);
}
