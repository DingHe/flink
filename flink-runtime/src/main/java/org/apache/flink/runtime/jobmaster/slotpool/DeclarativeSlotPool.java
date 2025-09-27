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
 * 该接口的目的是允许用户根据需要增加或减少资源要求，并能够有效地分配和释放计算资源（即“插槽”）
 * <p>In order to acquire new resources, users need to increase the required resources. Once they no
 * longer need the resources, users need to decrease the required resources so that superfluous
 * resources can be returned.
 */
public interface DeclarativeSlotPool {

    /**
     * Increases the resource requirements by increment.
     *  增加资源需求。通过传递一个 ResourceCounter 增加需要的资源量
     * @param increment increment by which to increase the resource requirements
     */
    void increaseResourceRequirementsBy(ResourceCounter increment);

    /**
     * Decreases the resource requirements by decrement.
     * 减少资源需求。通过传递一个 ResourceCounter 减少所需的资源量
     * @param decrement decrement by which to decrease the resource requirements
     */
    void decreaseResourceRequirementsBy(ResourceCounter decrement);

    /**
     * Sets the resource requirements to the given resourceRequirements.
     * 设置精确的资源需求
     * @param resourceRequirements new resource requirements
     */
    void setResourceRequirements(ResourceCounter resourceRequirements);

    /**
     * Returns the current resource requirements.
     * 获取当前的资源需求
     * @return current resource requirements
     */
    Collection<ResourceRequirement> getResourceRequirements();

    /**
     * Offers slots to this slot pool. The slot pool is free to accept as many slots as it needs.
     * 接收并处理传入的资源插槽请求，提供给 SlotPool
     * @param offers offers containing the list of slots offered to this slot pool
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return collection of accepted slots; the other slot offers are implicitly rejected
     */
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
     * 注册一组插槽，允许这些插槽在 SlotPool 中使用，超出当前需求的插槽也可以注册
     * @param slots slots to register
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return the successfully registered slots; the other slot offers are implicitly rejected
     */
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
    FreeSlotTracker getFreeSlotTracker();

    /**
     * Returns the slot information for all slots (free and allocated slots).
     * 返回所有插槽的信息，包括已分配和空闲的插槽
     * @return collection of slot information
     */
    Collection<? extends SlotInfo> getAllSlotsInformation();

    /**
     * Checks whether the slot pool contains a slot with the given {@link AllocationID} and if it is
     * free.
     * 检查指定的 allocationId 是否在 SlotPool 中有空闲插槽
     * @param allocationId allocationId specifies the slot to check for
     * @return {@code true} if the slot pool contains a free slot registered under the given
     *     allocation id; otherwise {@code false}
     */
    boolean containsFreeSlot(AllocationID allocationId);

    /**
     * Reserves the free slot identified by the given allocationId and maps it to the given
     * requiredSlotProfile.
     * 根据 allocationId 预定一个空闲插槽，并将其分配给指定的资源配置
     * @param allocationId allocationId identifies the free slot to allocate
     * @param requiredSlotProfile requiredSlotProfile specifying the resource requirement
     * @return a PhysicalSlot representing the allocated slot
     * @throws IllegalStateException if no free slot with the given allocationId exists or if the
     *     specified slot cannot fulfill the requiredSlotProfile
     */
    PhysicalSlot reserveFreeSlot(AllocationID allocationId, ResourceProfile requiredSlotProfile);

    /**
     * Frees the reserved slot identified by the given allocationId. If no slot with allocationId
     * exists, then the call is ignored.
     *
     * <p>Whether the freed slot is returned to the owning TaskExecutor is implementation dependent.
     * 释放一个已经预定的插槽，指定原因和释放时间
     * @param allocationId allocationId identifying the slot to release
     * @param cause cause for releasing the slot; can be {@code null}
     * @param currentTime currentTime when the slot was released
     * @return the resource requirements that the slot was fulfilling
     */
    ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime);

    /**
     * Releases all slots belonging to the owning TaskExecutor if it has been registered.
     * 释放指定 TaskExecutor 所拥有的所有插槽
     * @param owner owner identifying the owning TaskExecutor
     * @param cause cause for failing the slots
     * @return the resource requirements that all slots were fulfilling; empty if all slots were
     *     currently free
     */
    ResourceCounter releaseSlots(ResourceID owner, Exception cause);

    /**
     * Releases the slot specified by allocationId if one exists.
     * 释放指定的插槽
     * @param allocationId allocationId identifying the slot to fail
     * @param cause cause for failing the slot
     * @return the resource requirements that the slot was fulfilling; empty if the slot was
     *     currently free
     */
    ResourceCounter releaseSlot(AllocationID allocationId, Exception cause);

    /**
     * Returns whether the slot pool has a slot registered which is owned by the given TaskExecutor.
     * 检查是否有插槽属于指定的 TaskExecuto
     * @param owner owner identifying the TaskExecutor for which to check whether the slot pool has
     *     some slots registered
     * @return true if the given TaskExecutor has a slot registered at the slot pool
     */
    boolean containsSlots(ResourceID owner);

    /**
     * Releases slots which have exceeded the idle slot timeout and are no longer needed to fulfill
     * the resource requirements.
     * 释放所有超过空闲超时的插槽，释放不再需要的资源
     * @param currentTimeMillis current time
     */
    void releaseIdleSlots(long currentTimeMillis);

    /**
     * Registers a listener which is called whenever new slots become available.
     * 注册一个监听器，当新的插槽变得可用时会调用它
     * @param listener which is called whenever new slots become available
     */
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
