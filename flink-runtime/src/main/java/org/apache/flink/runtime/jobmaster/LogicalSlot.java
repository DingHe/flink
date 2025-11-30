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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * A logical slot represents a resource on a TaskManager into which a single task can be deployed.
 */
// Flink JobMaster 用来抽象和管理 TaskManager 上的计算资源的核心概念
// 在 Flink 中，**资源槽（Slot）**是 TaskManager 上用于执行一个或多个任务（Subtask）的资源单位（主要是内存）。
// LogicalSlot 接口则代表了 JobMaster 逻辑上分配并持有的一个可供单个任务部署的资源槽。
// 资源抽象： 它将底层的 TaskManager 资源抽象成一个可用于部署任务的容器，使得调度器无需关心资源的物理细节。
// 任务绑定： 它是 Execution（任务的单次执行尝试，即 Payload）的载体。一个 LogicalSlot 只能绑定一个任务（Payload）。
// 资源信息提供： 它提供了访问底层资源所需的信息，如 TaskManager 的位置、与 TaskManager 通信的网关 (TaskManagerGateway)，以及资源的本地性信息 (Locality)。
// LogicalSlot 是 Flink JobMaster 在 TaskManager 上拥有的一个“预约好的、可部署任务”的逻辑占位符。
public interface LogicalSlot {
    // 已终止的载荷占位符。
    // 一个特殊的 Payload 实例，用于表示一个已终止或无效的载荷，它不执行任何操作，并且其终止 Future 已经完成。
    Payload TERMINATED_PAYLOAD =
            new Payload() {

                private final CompletableFuture<?> completedTerminationFuture =
                        CompletableFuture.completedFuture(null);

                @Override
                public void fail(Throwable cause) {
                    // ignore
                }

                @Override
                public CompletableFuture<?> getTerminalStateFuture() {
                    return completedTerminationFuture;
                }
            };

    /**
     * Return the TaskManager location of this slot.
     * @return TaskManager location of this slot
     */
    // 获取 TaskManager 位置。
    // 返回该逻辑槽所在的 TaskManager 的网络地址和唯一标识符。用于网络通信和本地性判断。
    TaskManagerLocation getTaskManagerLocation();

    /**
     * Return the TaskManager gateway to talk to the TaskManager.
     * @return TaskManager gateway to talk to the TaskManager
     */
    // 获取 TaskManager 网关。
    // 返回用于与该 TaskManager 进行 RPC 通信的接口。JobMaster 通过此网关向 TaskManager 发送部署、取消等指令。
    TaskManagerGateway getTaskManagerGateway();

    /**
     * Gets the locality of this slot.
     *
     * @return locality of this slot
     */
    // 获取本地性信息。
    // 返回该资源槽的本地性级别（例如，LOCAL、HOST_LOCAL、NON_LOCAL）。这是调度器优化任务部署的重要依据
    Locality getLocality();

    /**
     * True if the slot is alive and has not been released.
     *
     * @return True if the slot is alive, otherwise false if the slot is released
     */
    // 检查存活状态。
    // 返回该逻辑槽是否仍然有效且未被释放（例如，如果 TaskManager 已经宕机，则返回 false）。
    boolean isAlive();

    /**
     * Tries to assign a payload to this slot. One can only assign a single payload once.
     *
     * @param payload to be assigned to this slot.
     * @return true if the payload could be assigned, otherwise false
     */
    // 尝试分配载荷（任务）。
    // 尝试将一个任务（Payload，通常是 Execution 实例）绑定到该槽。
    // 一个槽只能成功绑定一个 Payload，如果已分配，则返回 false。
    boolean tryAssignPayload(Payload payload);

    /**
     * Returns the set payload or null if none.
     *
     * @return Payload of this slot of null if none
     */
    // 获取当前载荷（任务）。
    // 返回当前绑定到该逻辑槽的 Payload（任务实例），如果没有绑定则返回 null。
    @Nullable
    Payload getPayload();

    /**
     * Releases this slot.
     *
     * @return Future which is completed once the slot has been released, in case of a failure it is
     *     completed exceptionally
     * @deprecated Added because extended the actual releaseSlot method with cause parameter.
     */
    // 释放逻辑槽（无原因）。
    // 释放该逻辑槽资源。这是一个默认方法，调用带 cause 参数的重载方法，原因设为 null。
    default CompletableFuture<?> releaseSlot() {
        return releaseSlot(null);
    }

    /**
     * Releases this slot.
     *
     * @param cause why the slot was released or null if none
     * @return future which is completed once the slot has been released
     */
    CompletableFuture<?> releaseSlot(@Nullable Throwable cause);

    /**
     * Gets the allocation id of this slot. Multiple logical slots can share the same allocation id.
     *
     * @return allocation id of this slot
     */
    // 获取分配 ID。
    // 返回该资源槽所属的资源分配的唯一 ID。
    // 在 Flink 中，一个 TaskManager 可能会被分配一个大的资源块（由 AllocationID 标识），而该资源块可以被划分为多个 LogicalSlot。
    AllocationID getAllocationId();

    /**
     * Gets the slot request id uniquely identifying the request with which this slot has been
     * allocated.
     *
     * @return Unique id identifying the slot request with which this slot was allocated
     */
    // 获取槽请求 ID。
    // 返回用于请求该逻辑槽的唯一 ID。
    // 用于跟踪资源请求与分配的对应关系。
    SlotRequestId getSlotRequestId();


    // LogicalSlot 同样规定了其所能承载的 payload ,
    // LogicalSlot.Payload 接口的实现类是 Execution，也就是需要被调度执行的一个 task
    /** Payload for a logical slot. */
    interface Payload {

        /**
         * Fail the payload with the given cause.
         *
         * @param cause of the failure
         */
        void fail(Throwable cause);

        /**
         * Gets the terminal state future which is completed once the payload has reached a terminal
         * state.
         *
         * @return Terminal state future
         */
        CompletableFuture<?> getTerminalStateFuture();
    }
}
