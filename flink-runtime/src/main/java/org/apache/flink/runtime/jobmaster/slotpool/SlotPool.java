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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The Interface of a slot pool that manages slots. */
// 统一管理资源槽位： 维护 JobMaster 从 TaskManager 处获得的所有可用和已占用的物理资源槽位 (PhysicalSlot)。
// 对接资源管理器： 负责与 ResourceManager 通信，发起新的资源请求，以获取额外的 TaskManager 资源。
// 服务调度器： 响应调度器（Scheduler）的槽位请求，根据资源需求和本地性偏好，立即分配已有的槽位或请求新的槽位。
// SlotPool 是 JobMaster 端的资源库存和请求中心。
public interface SlotPool extends AllocatedSlotActions, AutoCloseable {

    // ------------------------------------------------------------------------
    //  lifecycle
    // ------------------------------------------------------------------------
    // 启动槽位池。
    // 初始化 Slot Pool，使其开始运行。
    // 它接收 JobMaster 的 ID 和地址，这些信息对于 TaskManager 和 ResourceManager 的通信是必需的。
    void start(JobMasterId jobMasterId, String newJobManagerAddress) throws Exception;
    // 关闭槽位池。
    // 清理并释放 Slot Pool 持有的所有资源。
    // 这通常涉及取消所有待处理的槽位请求，并释放所有已分配的物理槽位。
    void close();

    // ------------------------------------------------------------------------
    //  resource manager connection
    // ------------------------------------------------------------------------

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     *
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    // 连接到 ResourceManager。
    // 建立 Slot Pool 与 ResourceManager 之间的通信。连接成功后，Slot Pool 才能向 ResourceManager 请求新的资源。
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    // 断开与 ResourceManager 的连接。
    // 终止与当前 ResourceManager 的连接。
    // 所有待处理的资源请求都会被取消，但 Slot Pool 内部已获得的槽位仍然可以继续服务。
    void disconnectResourceManager();

    // ------------------------------------------------------------------------
    //  registering / un-registering TaskManagers and slots
    // ------------------------------------------------------------------------

    /**
     * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPool}.
     *
     * @param resourceID identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    // 注册 TaskManager。
    // 将一个 TaskManager 注册到 Slot Pool，表明这个 TaskManager 正在为 JobMaster 提供资源。
    boolean registerTaskManager(ResourceID resourceID);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPool}.
     *
     * @param resourceId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    // 释放 TaskManager。
    // 从 Slot Pool 中注销一个 TaskManager，通常是因为 TaskManager 故障或断开连接。所有该 TaskManager 上的槽位都将被释放。
    boolean releaseTaskManager(final ResourceID resourceId, final Exception cause);

    /**
     * Offers multiple slots to the {@link SlotPool}. The slot offerings can be individually
     * accepted or rejected by returning the collection of accepted slot offers.
     *
     * @param taskManagerLocation from which the slot offers originate
     * @param taskManagerGateway to talk to the slot offerer
     * @param offers slot offers which are offered to the {@link SlotPool}
     * @return A collection of accepted slot offers. The remaining slot offers are implicitly
     *     rejected.
     */
    // 处理槽位提供。
    // TaskManager 通过此方法向 Slot Pool 提供（Offer）其可用的物理槽位。
    // Slot Pool 根据内部需求和策略，选择性地接受并返回接受的槽位列表。未接受的槽位将被隐式拒绝。
    Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers);

    // ------------------------------------------------------------------------
    //  allocating and disposing slots
    // ------------------------------------------------------------------------

    /**
     * Returns all free slot tracker.
     *
     * @return all free slot tracker
     */
    // 获取空闲槽位追踪器。
    // 提供一个接口来追踪当前 Slot Pool 中所有空闲槽位的状态。
    FreeSlotTracker getFreeSlotTracker();

    /**
     * Returns a list of {@link SlotInfo} objects about all slots that are currently allocated in
     * the slot pool.
     *
     * @return a list of {@link SlotInfo} objects about all slots that are currently allocated in
     *     the slot pool.
     */
    // 获取已分配槽位信息。
    // 返回当前 Slot Pool 中所有已分配（即被任务使用）的槽位的元数据信息。
    Collection<SlotInfo> getAllocatedSlotsInformation();

    /**
     * Allocates the available slot with the given allocation id under the given request id for the
     * given requirement profile. The slot must be able to fulfill the requirement profile,
     * otherwise an {@link IllegalStateException} will be thrown.
     *
     * @param slotRequestId identifying the requested slot
     * @param allocationID the allocation id of the requested available slot
     * @param requirementProfile resource profile of the requirement for which to allocate the slot
     * @return the previously available slot with the given allocation id, if a slot with this
     *     allocation id exists
     */
    // 分配已可用的槽位。
    // 尝试从 Slot Pool 中已有的空闲槽位中，根据 AllocationID 和 ResourceProfile 匹配并分配一个槽位。
    // 如果找到并满足要求，返回 PhysicalSlot，否则返回 Optional.empty()。
    Optional<PhysicalSlot> allocateAvailableSlot(
            SlotRequestId slotRequestId,
            AllocationID allocationID,
            ResourceProfile requirementProfile);

    /**
     * Request the allocation of a new slot from the resource manager. This method will not return a
     * slot from the already available slots from the pool, but instead will add a new slot to that
     * pool that is immediately allocated and returned.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested slot
     * @param timeout timeout for the allocation procedure
     * @return a newly allocated slot that was previously not available.
     */
    // 请求新的普通槽位。
    // 向 ResourceManager 请求一个全新的槽位。
    // 这个请求是异步的，返回一个 CompletableFuture。此方法通常用于流式或要求快速分配的场景。
    default CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            @Nullable Duration timeout) {
        return requestNewAllocatedSlot(
                slotRequestId, resourceProfile, Collections.emptyList(), timeout);
    }

    /**
     * Request the allocation of a new slot from the resource manager. This method will not return a
     * slot from the already available slots from the pool, but instead will add a new slot to that
     * pool that is immediately allocated and returned.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested slot
     * @param preferredAllocations preferred allocations for the new allocated slot
     * @param timeout timeout for the allocation procedure
     * @return a newly allocated slot that was previously not available.
     */
    CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            @Nullable Duration timeout);

    /**
     * Requests the allocation of a new batch slot from the resource manager. Unlike the normal
     * slot, a batch slot will only time out if the slot pool does not contain a suitable slot.
     * Moreover, it won't react to failure signals from the resource manager.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested batch slot
     * @return a future which is completed with newly allocated batch slot
     */
    // 请求新的批处理槽位。
    default CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            SlotRequestId slotRequestId, ResourceProfile resourceProfile) {
        return requestNewAllocatedBatchSlot(
                slotRequestId, resourceProfile, Collections.emptyList());
    }

    CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations);

    /**
     * Disables batch slot request timeout check. Invoked when someone else wants to take over the
     * timeout check responsibility.
     */
    // 禁用批处理槽位超时检查。
    // 在某些场景下（如弹性或自适应调度），可能由外部组件接管超时检查职责，此时调用此方法禁用 Slot Pool 内部的超时机制。
    void disableBatchSlotRequestTimeoutCheck();

    /**
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    // 创建已分配槽位报告。
    // 生成一个报告，列出指定 TaskManager 上当前被 JobMaster 分配和使用的所有槽位信息。
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);

    /**
     * Sets whether the underlying job is currently restarting or not.
     *
     * @param isJobRestarting whether the job is restarting or not
     */
    // 设置作业重启状态。
    void setIsJobRestarting(boolean isJobRestarting);
}
