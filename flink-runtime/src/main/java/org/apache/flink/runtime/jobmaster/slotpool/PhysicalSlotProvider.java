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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The provider serves physical slot requests. */
// PhysicalSlotProvider 接口是 Flink **槽位池（SlotPool）**内部的关键抽象，它定义了作业管理器（JobMaster）**获取物理执行资源（槽位，Slot）**的服务契约
// 统一的资源申请入口：它为 JobMaster 提供了统一的 API，用于向资源层（可以是 SlotPool 内部已有的空闲槽位，也可以是远程的资源管理器 ResourceManager）请求物理槽位。
// 管理槽位生命周期：它不仅负责分配槽位，也负责处理槽位请求的取消，并在请求成功后释放相应的槽位资源。
// 支持异步操作：槽位分配是一个异步过程，该接口所有分配方法都返回 CompletableFuture，代表未来某个时间点才能获得的槽位结果。
public interface PhysicalSlotProvider {

    /**
     * Submit requests to allocate physical slots.
     *
     * <p>The physical slot can be either allocated from the slots, which are already available for
     * the job, or a new one can be requested from the resource manager.
     *
     * @param physicalSlotRequests physicalSlotRequest slot requirements
     * @return futures of the allocated slots
     */
    // 提交物理槽位分配请求。
    // 它是 JobMaster 批量请求所需资源的主要入口。
    // 参数 physicalSlotRequests
    // 一个 PhysicalSlotRequest 对象的集合，每个对象封装了一个槽位的具体要求（如资源配置文件、所属的 SlotRequestId 等）。
    Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests);

    /**
     * Cancels the slot request with the given {@link SlotRequestId}.
     *
     * <p>If the request is already fulfilled with a physical slot, the slot will be released.
     *
     * @param slotRequestId identifying the slot request to cancel
     * @param cause of the cancellation
     */
    // 取消特定的槽位分配请求。
    // 当任务被取消或失败时，JobMaster 会调用此方法来撤销对资源的请求。
    void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause);

    /**
     * Disables batch slot request timeout check. Invoked when someone else wants to take over the
     * timeout check responsibility.
     */
    // 禁用批量槽位请求的内部超时检查机制。
    void disableBatchSlotRequestTimeoutCheck();
}
