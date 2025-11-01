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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.concurrent.CompletableFuture;

/** Payload interface for {@link org.apache.flink.runtime.taskexecutor.slot.TaskSlot}. */
// 主要用于代表一个实际被放置在 TaskSlot 中运行或即将运行的“任务”或“组件”
// 当 JobMaster/ResourceManager 决定在一个 TaskSlot 上启动一个任务时，实际被“装入”TaskSlot 的就是实现了 TaskSlotPayload 接口的对象。
// 抽象层： 它为 TaskSlot 提供了一个通用的、与具体任务类型（如 Task）解耦的接口。
public interface TaskSlotPayload {
    // 获取这个有效载荷所属的 Job 的唯一标识符
    JobID getJobID();
    // 获取这个有效载荷（通常是一个 Flink 任务）的 ExecutionAttempt 的唯一标识符。这是一个最细粒度的标识符，代表一次特定的任务尝试
    ExecutionAttemptID getExecutionId();
    // 获取分配给这个 TaskSlot 的 Slot 资源分配的唯一标识符
    AllocationID getAllocationId();
    // 获取一个 Future，用于监听有效载荷（任务）的终止状态
    CompletableFuture<?> getTerminationFuture();

    /**
     * Fail the payload with the given throwable. This operation should eventually complete the
     * termination future.
     *
     * @param cause of the failure
     */
    // 从外部强制使有效载荷（任务）失败
    void failExternally(Throwable cause);
}
