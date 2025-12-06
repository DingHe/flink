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
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.Collection;
import java.util.Optional;
// AllocatedSlotPool（已分配槽位池）是一个低层级的接口，它的核心职责是作为 JobMaster 中所有已获得的物理槽位 (AllocatedSlot) 的集中式仓库和生命周期管理器。
// 维护一组 AllocatedSlot 实例，并提供添加、移除、查询、预定和释放这些物理资源槽位的基础操作。
// 管理的是 JobMaster 已经从 TaskManager 成功获取的物理资源，而不是更高层级的逻辑槽位请求。
// 负责**“库存管理”**的组件，记录了 JobMaster 当前拥有哪些 TaskManager 上的哪些物理资源，以及这些资源是空闲还是已被预定。


/** The slot pool is responsible for maintaining a set of {@link AllocatedSlot AllocatedSlots}. */
public interface AllocatedSlotPool {

    /**
     * Adds the given collection of slots to the slot pool.
     *
     * @param slots slots to add to the slot pool
     * @param currentTime currentTime when the slots have been added to the slot pool
     * @throws IllegalStateException if the slot pool already contains a to be added slot
     */
    // 添加槽位。
    // 将从 TaskManager 获得的 AllocatedSlot 集合添加到槽位池中。
    // currentTime 记录槽位加入的时间点。如果槽位池中已存在相同的槽位（通过 AllocationID 识别），则会抛出异常。
    void addSlots(Collection<AllocatedSlot> slots, long currentTime);

    /**
     * Removes the slot with the given allocationId from the slot pool.
     *
     * @param allocationId allocationId identifying the slot to remove from the slot pool
     * @return the removed slot if there was a slot with the given allocationId; otherwise {@link
     *     Optional#empty()}
     */
    // 移除单个槽位。
    // 根据 AllocationID 将指定的槽位从池中移除（无论其是否被预定）。如果槽位存在则返回被移除的槽位，否则返回空。
    Optional<AllocatedSlot> removeSlot(AllocationID allocationId);

    /**
     * Removes all slots belonging to the owning TaskExecutor identified by owner.
     *
     * @param owner owner identifies the TaskExecutor whose slots shall be removed
     * @return the collection of removed slots and for each slot whether it was currently free
     */
    // 移除 TaskExecutor 的所有槽位。
    // 移除属于指定 TaskExecutor (owner) 的所有槽位。返回一个包含所有被移除槽位及其预定状态的集合。
    AllocatedSlotsAndReservationStatus removeSlots(ResourceID owner);

    /**
     * Checks whether the slot pool contains at least one slot belonging to the specified owner.
     *
     * @param owner owner for which to check whether the slot pool contains slots
     * @return {@code true} if the slot pool contains a slot from the given owner; otherwise {@code
     *     false}
     */
    // 检查 TaskExecutor 是否有槽位。
    boolean containsSlots(ResourceID owner);

    /**
     * Checks whether the slot pool contains a slot with the given allocationId.
     *
     * @param allocationId allocationId identifying the slot for which to check whether it is
     *     contained
     * @return {@code true} if the slot pool contains the slot with the given allocationId;
     *     otherwise {@code false}
     */
    // 检查槽位是否存在。
    boolean containsSlot(AllocationID allocationId);

    /**
     * Checks whether the slot pool contains a slot with the given {@link AllocationID} and if it is
     * free.
     *
     * @param allocationId allocationId specifies the slot to check for
     * @return {@code true} if the slot pool contains a free slot registered under the given
     *     allocation id; otherwise {@code false}
     */
    // 检查空闲槽位是否存在。
    // 检查槽位池中是否包含具有指定 AllocationID 且当前处于空闲状态的槽位。
    boolean containsFreeSlot(AllocationID allocationId);

    /**
     * Reserves the free slot specified by the given allocationId.
     *
     * @param allocationId allocationId identifying the free slot to reserve
     * @return the {@link AllocatedSlot} which has been reserved
     * @throws IllegalStateException if there is no free slot with the given allocationId
     */
    // 预定空闲槽位。
    // 将池中指定的空闲槽位标记为已预定状态。预定成功的槽位将被用于创建 LogicalSlot。如果槽位不存在或不是空闲状态，则抛出 IllegalStateException。
    AllocatedSlot reserveFreeSlot(AllocationID allocationId);

    /**
     * Frees the reserved slot, adding it back into the set of free slots.
     *
     * @param allocationId identifying the reserved slot to freed
     * @param currentTime currentTime when the slot has been freed
     * @return the freed {@link AllocatedSlot} if there was an allocated with the given
     *     allocationId; otherwise {@link Optional#empty()}.
     */
    // 释放已预定的槽位。
    // 将指定的已预定槽位释放，并将其重新放回空闲槽位集合中。currentTime 记录其变为空闲的时间。
    Optional<AllocatedSlot> freeReservedSlot(AllocationID allocationId, long currentTime);

    /**
     * Returns slot information specified by the given allocationId.
     *
     * @return the slot information if there was a slot with the given allocationId; otherwise
     *     {@link Optional#empty()}
     */
    Optional<SlotInfo> getSlotInformation(AllocationID allocationID);

    /**
     * Returns information about all currently free slots.
     *
     * @return free slot information
     */
    FreeSlotTracker getFreeSlotTracker();

    /**
     * Returns information about all slots in this pool.
     *
     * @return collection of all slot information
     */
    Collection<? extends SlotInfo> getAllSlotsInformation();

    /** Information about a free slot. */
    interface FreeSlotInfo {
        SlotInfo asSlotInfo();

        /**
         * Returns since when this slot is free.
         *
         * @return the time since when the slot is free
         */
        long getFreeSince();

        default AllocationID getAllocationId() {
            return asSlotInfo().getAllocationId();
        }
    }

    /** A collection of {@link AllocatedSlot AllocatedSlots} and their reservation status. */
    interface AllocatedSlotsAndReservationStatus {
        boolean wasFree(AllocationID allocatedSlot);

        Collection<AllocatedSlot> getAllocatedSlots();
    }
}
