/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Slot pool interface which uses Flink's declarative resource management protocol to acquire
 * resources.
 * <p>In order to acquire new resources, users need to increase the required resources. Once they no
 * longer need the resources, users need to decrease the required resources so that superfluous
 * resources can be returned.
 */
// DeclarativeSlotPool（声明式槽位池）接口是 Flink 在声明式资源管理协议下使用的槽位管理组件。
// 与传统的命令式管理（每次需要时才请求一个槽位）不同，声明式管理要求用户（调度器）声明所需的总资源量 (ResourceRequirements)。
// DeclarativeSlotPool 负责维护这个资源需求清单，并确保实际持有的资源（已获得的槽位）满足或接近这个需求。
// 用户通过 increaseResourceRequirementsBy 和 decreaseResourceRequirementsBy 来间接触发资源获取或释放。
public interface DeclarativeSlotPool {

    /**
     * Increases the resource requirements by increment.
     * @param increment increment by which to increase the resource requirements
     */
    // 增加资源需求。
    // 声明式操作。
    // 增加 Slot Pool 对资源的总需求量。这会触发 Slot Pool 内部逻辑，可能向 ResourceManager 请求新的资源。
    void increaseResourceRequirementsBy(ResourceCounter increment);

    /**
     * Decreases the resource requirements by decrement.
     * @param decrement decrement by which to decrease the resource requirements
     */
    // 减少资源需求。
    // 声明式操作。
    // 减少 Slot Pool 对资源的总需求量。这可能导致 Slot Pool 释放多余的、闲置的槽位。
    void decreaseResourceRequirementsBy(ResourceCounter decrement);

    /**
     * Sets the resource requirements to the given resourceRequirements.
     * @param resourceRequirements new resource requirements
     */
    // 直接将 Slot Pool 对资源的总需求量设置为指定值。
    void setResourceRequirements(ResourceCounter resourceRequirements);

    /**
     * Returns the current resource requirements.
     * @return current resource requirements
     */
    // 返回当前 Slot Pool 声明的、尚未被已分配槽位满足的资源需求清单。
    Collection<ResourceRequirement> getResourceRequirements();

    /**
     * Offers slots to this slot pool. The slot pool is free to accept as many slots as it needs.
     * @param offers offers containing the list of slots offered to this slot pool
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return collection of accepted slots; the other slot offers are implicitly rejected
     */
    // 接收和处理槽位提供（JobMaster发起）。
    // 当 TaskManager 主动将槽位提供给 JobMaster 时调用。
    // 只有当前 Slot Pool 需要（即满足资源需求 ResourceRequirements）的槽位才会被接受。
    Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime);

    /**
     * Registers the given set of slots at the slot pool. The slot pool will try to accept all slots
     * unless the slot is unavailable (for example, the TaskManger is blocked).
     *
     * <p>The difference from {@link #offerSlots} is that this method allows accepting slots which
     * exceed the currently required, but the {@link #offerSlots} only accepts those slots that are
     * currently required.
     * @param slots slots to register
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return the successfully registered slots; the other slot offers are implicitly rejected
     */
    // 注册槽位（可超出需求）。
    // 专门用于注册一组槽位。与 offerSlots 的主要区别是，此方法可以接受超出当前声明资源需求的槽位，并将它们作为空闲资源保留在 Slot Pool 中。
    Collection<SlotOffer> registerSlots(
            Collection<? extends SlotOffer> slots,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime);

    /**
     * Returns the free slot tracker.
     *
     * @return free slot tracker
     */
    // 获取空闲槽位追踪器。
    // 返回一个用于查询和管理当前空闲槽位集合的组件。
    FreeSlotTracker getFreeSlotTracker();

    /**
     * Returns the slot information for all slots (free and allocated slots).
     * @return collection of slot information
     */
    // 返回 Slot Pool 中所有槽位的元数据，包括空闲和已分配的。
    Collection<? extends SlotInfo> getAllSlotsInformation();

    /**
     * Checks whether the slot pool contains a slot with the given {@link AllocationID} and if it is
     * free.
     * @param allocationId allocationId specifies the slot to check for
     * @return {@code true} if the slot pool contains a free slot registered under the given
     *     allocation id; otherwise {@code false}
     */
    // 检查 Slot Pool 中是否存在指定的、且当前处于空闲状态的槽位。
    boolean containsFreeSlot(AllocationID allocationId);

    /**
     * Reserves the free slot identified by the given allocationId and maps it to the given
     * requiredSlotProfile.
         * @param allocationId allocationId identifies the free slot to allocate
     * @param requiredSlotProfile requiredSlotProfile specifying the resource requirement
     * @return a PhysicalSlot representing the allocated slot
     * @throws IllegalStateException if no free slot with the given allocationId exists or if the
     *     specified slot cannot fulfill the requiredSlotProfile
     */
    // 预定并分配空闲槽位。
    // 根据 AllocationID 从空闲槽位中找到并分配（预定）一个物理槽位给任务。这个槽位必须满足 requiredSlotProfile 定义的资源要求。
    PhysicalSlot reserveFreeSlot(AllocationID allocationId, ResourceProfile requiredSlotProfile);

    /**
     * Frees the reserved slot identified by the given allocationId. If no slot with allocationId
     * exists, then the call is ignored.
     *
     * <p>Whether the freed slot is returned to the owning TaskExecutor is implementation dependent.
     * @param allocationId allocationId identifying the slot to release
     * @param cause cause for releasing the slot; can be {@code null}
     * @param currentTime currentTime when the slot was released
     * @return the resource requirements that the slot was fulfilling
     */
    // 释放已预定的槽位。
    // 将一个先前被 reserveFreeSlot 分配的槽位释放回空闲状态。返回值是该槽位之前满足的资源计数，用于调整总资源需求。
    ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime);

    /**
     * Releases all slots belonging to the owning TaskExecutor if it has been registered.
     * @param owner owner identifying the owning TaskExecutor
     * @param cause cause for failing the slots
     * @return the resource requirements that all slots were fulfilling; empty if all slots were
     *     currently free
     */
    // 释放 TaskExecutor 的所有槽位。
    // 释放属于指定 TaskExecutor 的所有槽位（包括空闲和已分配的）。
    ResourceCounter releaseSlots(ResourceID owner, Exception cause);

    /**
     * Releases the slot specified by allocationId if one exists.
     * 释放指定的插槽
     * @param allocationId allocationId identifying the slot to fail
     * @param cause cause for failing the slot
     * @return the resource requirements that the slot was fulfilling; empty if the slot was
     *     currently free
     */
    // 释放指定的槽位。
    // 释放具有特定 AllocationID 的槽位。
    ResourceCounter releaseSlot(AllocationID allocationId, Exception cause);

    /**
     * Returns whether the slot pool has a slot registered which is owned by the given TaskExecutor.
     * @param owner owner identifying the TaskExecutor for which to check whether the slot pool has
     *     some slots registered
     * @return true if the given TaskExecutor has a slot registered at the slot pool
     */
    // 检查 TaskExecutor 是否有槽位。
    // 检查指定的 TaskExecutor 是否在 Slot Pool 中注册有任何槽位（无论空闲还是已分配）。
    boolean containsSlots(ResourceID owner);

    /**
     * Releases slots which have exceeded the idle slot timeout and are no longer needed to fulfill
     * the resource requirements.
     * @param currentTimeMillis current time
     */
    // 释放空闲超时槽位。
    // 释放那些超过了空闲超时时间且当前不再需要满足声明资源需求的槽位，以避免资源浪费。
    void releaseIdleSlots(long currentTimeMillis);

    /**
     * Registers a listener which is called whenever new slots become available.
     * @param listener which is called whenever new slots become available
     */
    // 注册新的槽位可用监听器。
    // 注册一个回调接口，当有新的槽位（例如，从 TaskManager 接收或从已分配状态释放）变成可用时，会通知该监听器。
    void registerNewSlotsListener(NewSlotsListener listener);

    /**
     * Listener interface for newly available slots.
     *
     * <p>Implementations of the {@link DeclarativeSlotPool} will call {@link
     * #notifyNewSlotsAreAvailable} whenever newly offered slots are accepted or if an allocated
     * slot should become free after it is being {@link #freeReservedSlot freed}.
     */
    interface NewSlotsListener {

        /**
         * Notifies the listener about newly available slots.
         *
         * <p>This method will be called whenever newly offered slots are accepted or if an
         * allocated slot should become free after it is being {@link #freeReservedSlot freed}.
         *
         * @param newlyAvailableSlots are the newly available slots
         */
        void notifyNewSlotsAreAvailable(Collection<? extends PhysicalSlot> newlyAvailableSlots);
    }

    /** No-op {@link NewSlotsListener} implementation. */
    enum NoOpNewSlotsListener implements NewSlotsListener {
        INSTANCE;

        @Override
        public void notifyNewSlotsAreAvailable(
                Collection<? extends PhysicalSlot> newlyAvailableSlots) {}
    }
}
