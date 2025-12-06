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
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@code AllocatedSlot} represents a slot that the JobMaster allocated from a TaskExecutor. It
 * represents a slice of allocated resources from the TaskExecutor.
 * <p>To allocate an {@code AllocatedSlot}, the requests a slot from the ResourceManager. The
 * ResourceManager picks (or starts) a TaskExecutor that will then allocate the slot to the
 * JobMaster and notify the JobMaster.
 *
 * <p>Note: Prior to the resource management changes introduced in (Flink Improvement Proposal 6),
 * an AllocatedSlot was allocated to the JobManager as soon as the TaskManager registered at the
 * JobManager. All slots had a default unknown resource profile.
 */
// 代表一个已经从 TaskManager 分配并交付给 JobMaster 的物理资源槽位。
// 物理槽位 (Physical Slot): 它是 TaskManager 上的一个具体资源容器，具有固定的资源配置（CPU、内存）和网络位置。
// 封装物理信息： 存储所有与底层 TaskManager 和资源相关的元数据（ID、位置、网关）。
// 管理负载 (Payload)： 作为逻辑槽 (LogicalSlot，它实现了 Payload 接口) 的容器。一个 AllocatedSlot 一次只能容纳一个 Payload（即一个逻辑槽/一个任务）。
// 生命周期控制： 当 JobMaster 决定释放或 TaskManager 失联时，它负责触发对所容纳负载的清理和释放。
// JobMaster 槽位池 (SlotPool) 中存储的实际物理资源单位。

class AllocatedSlot implements PhysicalSlot {
   //槽位的唯一标识符，用于标识该槽位的分配
    // 唯一标识这个槽位的分配过程。
    // 这是在 ResourceManager/TaskManager 协作下分配给 JobMaster 的凭证。
    /** The ID under which the slot is allocated. Uniquely identifies the slot. */
    private final AllocationID allocationId;
    // 用于标识槽位所在的 TaskManager，提供访问 TaskManager 的信息
    // 包含槽位所在 TaskManager 的网络地址和 ID (ResourceID)，用于网络通信和数据本地性决策。
    /** The location information of the TaskManager to which this slot belongs. */
    private final TaskManagerLocation taskManagerLocation;
    // 槽位所提供的资源配置（例如内存、CPU 核心数等）
    // 描述该槽位提供的具体资源量（例如，1 CPU 核，2GB 内存）。
    /** The resource profile of the slot provides. */
    private final ResourceProfile resourceProfile;
    // 通过 TaskManagerGateway，JobMaster 可以与 TaskManager 进行通信，获取任务执行的状态，分配任务等
    /** RPC gateway to call the TaskManager that holds this slot. */
    private final TaskManagerGateway taskManagerGateway;
    // 该槽位在 TaskManager 内部的索引编号，纯粹是信息性的，
    // 与 TaskManagerID 一起构成 SlotID。
    /** The number of the slot on the TaskManager to which slot belongs. Purely informational. */
    private final int physicalSlotNumber;
    // 用于以线程安全的方式存储和管理当前占用此物理槽位的逻辑任务载荷（即 LogicalSlot 实例）。
    // 如果为 null，则表示该物理槽位空闲。
    private final AtomicReference<Payload> payloadReference;

    // ------------------------------------------------------------------------

    public AllocatedSlot(
            AllocationID allocationId,
            TaskManagerLocation location,
            int physicalSlotNumber,
            ResourceProfile resourceProfile,
            TaskManagerGateway taskManagerGateway) {
        this.allocationId = checkNotNull(allocationId);
        this.taskManagerLocation = checkNotNull(location);
        this.physicalSlotNumber = physicalSlotNumber;
        this.resourceProfile = checkNotNull(resourceProfile);
        this.taskManagerGateway = checkNotNull(taskManagerGateway);

        payloadReference = new AtomicReference<>(null);
    }

    // ------------------------------------------------------------------------

    /** Gets the Slot's unique ID defined by its TaskManager. */
    public SlotID getSlotId() {
        return new SlotID(getTaskManagerId(), physicalSlotNumber);
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    /**
     * Gets the ID of the TaskManager on which this slot was allocated.
     *
     * <p>This is equivalent to {@link #getTaskManagerLocation()}.{@link #getTaskManagerId()}.
     *
     * @return This slot's TaskManager's ID.
     */
    public ResourceID getTaskManagerId() {
        return getTaskManagerLocation().getResourceID();
    }
    // 返回该槽位提供的资源概要。
    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    @Override  //检查该槽位是否将被长期占用（例如，当前任务是否需要长时间使用该槽位）
    public boolean willBeOccupiedIndefinitely() {
        return isUsed() && payloadReference.get().willOccupySlotIndefinitely();
    }

    @Override
    public TaskManagerLocation getTaskManagerLocation() {
        return taskManagerLocation;
    }

    @Override
    public TaskManagerGateway getTaskManagerGateway() {
        return taskManagerGateway;
    }

    @Override //返回该槽位在 TaskManager 上的编号
    public int getPhysicalSlotNumber() {
        return physicalSlotNumber;
    }

    /**
     * Returns true if this slot is being used (e.g. a logical slot is allocated from this slot).
     *
     * @return true if a logical slot is allocated from this slot, otherwise false
     */
    // 返回 payloadReference.get() != null 的结果，
    // 即检查当前是否有 Payload（逻辑槽）被分配给此物理槽位。
    public boolean isUsed() {
        return payloadReference.get() != null;
    }

    @Override //如果槽位未被占用（即 payloadReference 为 null），则分配该负载并返回 true；否则返回 false
    public boolean tryAssignPayload(Payload payload) {
        return payloadReference.compareAndSet(null, payload);
    }

    /**
     * Triggers the release of the assigned payload. If the payload could be released, then it is
     * removed from the slot.
     *
     * @param cause of the release operation
     */
    public void releasePayload(Throwable cause) {
        final Payload payload = payloadReference.get();

        if (payload != null) {
            payload.release(cause);
            payloadReference.set(null);
        }
    }

    // ------------------------------------------------------------------------

    /** This always returns a reference hash code. */
    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    /** This always checks based on reference equality. */
    @Override
    public final boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public String toString() {
        return "AllocatedSlot "
                + allocationId
                + " @ "
                + taskManagerLocation
                + " - "
                + physicalSlotNumber;
    }
}
