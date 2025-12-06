/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot profile describes the profile of a slot into which a task wants to be scheduled. The
 * profile contains attributes such as resource or locality constraints, some of which may be hard
 * or soft. It also contains the information of resources for the physical slot to host this task
 * slot, which can be used to allocate a physical slot when no physical slot is available for this
 * task slot. A matcher can be generated to filter out candidate slots by matching their {@link
 * SlotContext} against the slot profile and, potentially, further requirements.
 */
// SlotProfile 描述了运行一个特定任务所需的全部槽位特征和约束条件。
// 它将任务对执行环境的要求（包括资源、位置、恢复状态等）打包成一个统一的规范。
// 需求规范：它定义了任务对**资源（CPU/内存）的硬性要求以及对位置（Locality）**的软性偏好。
// 槽位匹配基础：调度器使用 SlotProfile 作为查找依据。它会根据这个 Profile 来筛选出候选槽位 (SlotContext)，判断其是否满足任务的需求。
public class SlotProfile {
    /** This specifies the desired resource profile for the task slot. */
    // 任务槽位（Task Slot）所需的资源概况。
    // 这是硬性要求。它指定了该任务本身所需的 CPU、内存等资源，这些资源将从物理槽位中被占用。
    private final ResourceProfile taskResourceProfile;

    /** This specifies the desired resource profile for the physical slot to host this task slot. */
    // 物理槽位（Physical Slot）所需的资源概况。 这是硬性要求。
    // 它指定了任务运行所在的整个 TaskManager 槽位（物理槽位）必须提供的总资源。
    // 通常用于当需要向 ResourceManager 申请新的 TaskManager 资源时，作为申请依据。
    private final ResourceProfile physicalSlotResourceProfile;

    /** This specifies the preferred locations for the slot. */
    // 偏好的位置列表。 软性约束。任务希望被调度到这些 TaskManager 实例上
    private final Collection<TaskManagerLocation> preferredLocations;

    /** This contains desired allocation ids of the slot. */
    // 偏好的分配 ID 列表。 软性约束。如果任务以前成功运行过，并且其数据或状态仍然驻留在某个槽位上，
    // 那么这个列表会包含该槽位的 AllocationID
    private final Collection<AllocationID> preferredAllocations;

    /** This contains all reserved allocation ids from the whole execution graph. */
    // 整个执行图中的所有保留分配 ID。
    // 用于局部恢复的硬性/软性约束。它包含了整个作业中所有处于保留状态（例如失败后保留以待恢复）的槽位 ID
    private final Set<AllocationID> reservedAllocations;

    private SlotProfile(
            final ResourceProfile taskResourceProfile,
            final ResourceProfile physicalSlotResourceProfile,
            final Collection<TaskManagerLocation> preferredLocations,
            final Collection<AllocationID> preferredAllocations,
            final Set<AllocationID> reservedAllocations) {

        this.taskResourceProfile = checkNotNull(taskResourceProfile);
        this.physicalSlotResourceProfile = checkNotNull(physicalSlotResourceProfile);
        this.preferredLocations = checkNotNull(preferredLocations);
        this.preferredAllocations = checkNotNull(preferredAllocations);
        this.reservedAllocations = checkNotNull(reservedAllocations);
    }

    /** Returns the desired resource profile for the task slot. */
    public ResourceProfile getTaskResourceProfile() {
        return taskResourceProfile;
    }

    /** Returns the desired resource profile for the physical slot to host this task slot. */
    public ResourceProfile getPhysicalSlotResourceProfile() {
        return physicalSlotResourceProfile;
    }

    /** Returns the preferred locations for the slot. */
    public Collection<TaskManagerLocation> getPreferredLocations() {
        return preferredLocations;
    }

    /** Returns the desired allocation ids for the slot. */
    public Collection<AllocationID> getPreferredAllocations() {
        return preferredAllocations;
    }

    /**
     * Returns a set of all reserved allocation ids from the execution graph. It will used by {@link
     * PreviousAllocationSlotSelectionStrategy} to support local recovery. In this case, a vertex
     * cannot take an reserved allocation unless it exactly prefers that allocation.
     *
     * <p>This is optional and can be empty if unused.
     */
    public Set<AllocationID> getReservedAllocations() {
        return reservedAllocations;
    }

    /**
     * Returns a slot profile for the given resource profile, prior allocations and all prior
     * allocation ids from the whole execution graph.
     *
     * @param taskResourceProfile specifying the required resources for the task slot
     * @param physicalSlotResourceProfile specifying the required resources for the physical slot to
     *     host this task slot
     * @param preferredLocations specifying the preferred locations
     * @param priorAllocations specifying the prior allocations
     * @param reservedAllocations specifying all reserved allocations
     * @return Slot profile with all the given information
     */
    public static SlotProfile priorAllocation(
            final ResourceProfile taskResourceProfile,
            final ResourceProfile physicalSlotResourceProfile,
            final Collection<TaskManagerLocation> preferredLocations,
            final Collection<AllocationID> priorAllocations,
            final Set<AllocationID> reservedAllocations) {

        return new SlotProfile(
                taskResourceProfile,
                physicalSlotResourceProfile,
                preferredLocations,
                priorAllocations,
                reservedAllocations);
    }

    @Override
    public String toString() {
        return "SlotProfile{"
                + "taskResourceProfile="
                + taskResourceProfile
                + ", physicalSlotResourceProfile="
                + physicalSlotResourceProfile
                + ", preferredLocations="
                + preferredLocations
                + ", preferredAllocations="
                + preferredAllocations
                + ", reservedAllocations="
                + reservedAllocations
                + '}';
    }
}
