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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.util.AutoCloseableAsync;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * Container for multiple {@link TaskSlot} instances. Additionally, it maintains multiple indices
 * for faster access to tasks and sets of allocated slots.
 *
 * <p>The task slot table automatically registers timeouts for allocated slots which cannot be
 * assigned to a job manager.
 *
 * <p>Before the task slot table can be used, it must be started via the {@link #start} method.
 */
// TaskSlotTable 是 Flink TaskManager（TaskExecutor）中管理任务槽（Task Slot）核心状态的接口。
// 它充当了 TaskManager 内部的“账本”，记录了当前节点上所有 Slot 的分配、占用、活跃状态以及与具体任务（Task）的对应关系。
// 状态维护：维护 Slot 的生命周期（Free -> Allocated -> Active -> Releasing）
// 索引查询：提供多种索引方式，例如：通过 JobID 查找 Slot，通过 AllocationID 查找 Task，或者通过 ExecutionAttemptID 查找具体的执行实例。
// 超时管理：自动处理 Slot 分配后的超时逻辑。如果一个 Slot 被分配给了某个 JobManager 但对方迟迟没有部署任务，它负责通知系统回收资源。
// 资源关联：为每个 Slot 绑定特定的资源（如 MemoryManager），确保任务之间的资源隔离。

public interface TaskSlotTable<T extends TaskSlotPayload>
        extends TimeoutListener<AllocationID>, AutoCloseableAsync {
    /**
     * Start the task slot table with the given slot actions.
     *
     * @param initialSlotActions to use for slot actions
     * @param mainThreadExecutor {@link ComponentMainThreadExecutor} to schedule internal calls to
     *     the main thread
     */
    // 启动 Table。
    // 注入 SlotActions（用于触发释放/超时回调）和主线程执行器（确保线程安全）。
    void start(SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor);

    /**
     * Returns the all {@link AllocationID} for the given job.
     *
     * @param jobId for which to return the set of {@link AllocationID}.
     * @return Set of {@link AllocationID} for the given job
     */
    // 获取某个作业占用的所有分配 ID。
    Set<AllocationID> getAllocationIdsPerJob(JobID jobId);

    /**
     * Returns the {@link AllocationID} of any active task listed in this {@code TaskSlotTable}.
     *
     * @return The {@code AllocationID} of any active task.
     */
    // 获取当前所有正在运行任务的 Slot 分配 ID。
    Set<AllocationID> getActiveTaskSlotAllocationIds();

    /**
     * Returns the {@link AllocationID} of active {@link TaskSlot}s attached to the job with the
     * given {@link JobID}.
     *
     * @param jobId The {@code JobID} of the job for which the {@code AllocationID}s of the attached
     *     active {@link TaskSlot}s shall be returned.
     * @return A set of {@code AllocationID}s that belong to active {@code TaskSlot}s having the
     *     passed {@code JobID}.
     */
    Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId);
    // 生成 Slot 汇报单，TaskManager 会定期将此报告发送给 ResourceManager，以便同步集群资源视图。
    SlotReport createSlotReport(ResourceID resourceId);

    /**
     * Allocate the slot with the given index for the given job and allocation id. If negative index
     * is given, a new auto increasing index will be generated. Returns true if the slot could be
     * allocated. Otherwise it returns false.
     *
     * @param index of the task slot to allocate, use negative value for dynamic slot allocation
     * @param jobId to allocate the task slot for
     * @param allocationId identifying the allocation
     * @param slotTimeout until the slot times out
     * @return True if the task slot could be allocated; otherwise false
     */
    // 为特定作业分配一个 Slot
    @VisibleForTesting
    boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, Duration slotTimeout);

    /**
     * Allocate the slot with the given index for the given job and allocation id. If negative index
     * is given, a new auto increasing index will be generated. Returns true if the slot could be
     * allocated. Otherwise it returns false.
     *
     * @param index of the task slot to allocate, use negative value for dynamic slot allocation
     * @param jobId to allocate the task slot for
     * @param allocationId identifying the allocation
     * @param resourceProfile of the requested slot, used only for dynamic slot allocation and will
     *     be ignored otherwise
     * @param slotTimeout until the slot times out
     * @return True if the task slot could be allocated; otherwise false
     */
    // 为特定作业分配一个 Slot
    boolean allocateSlot(
            int index,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            Duration slotTimeout);

    /**
     * Marks the slot under the given allocation id as active. If the slot could not be found, then
     * a {@link SlotNotFoundException} is thrown.
     *
     * @param allocationId to identify the task slot to mark as active
     * @throws SlotNotFoundException if the slot could not be found for the given allocation id
     * @return True if the slot could be marked active; otherwise false
     */
    // 将 Slot 状态由“已分配”转为“活跃”
    // 当 JobManager 真正开始在该 Slot 上部署任务时触发。
    boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException;

    /**
     * Marks the slot under the given allocation id as inactive. If the slot could not be found,
     * then a {@link SlotNotFoundException} is thrown.
     *
     * @param allocationId to identify the task slot to mark as inactive
     * @param slotTimeout until the slot times out
     * @throws SlotNotFoundException if the slot could not be found for the given allocation id
     * @return True if the slot could be marked inactive
     */
    // 将活跃 Slot 转为不活跃，并设置新的超时时间。
    boolean markSlotInactive(AllocationID allocationId, Duration slotTimeout)
            throws SlotNotFoundException;

    /**
     * Try to free the slot. If the slot is empty it will set the state of the task slot to free and
     * return its index. If the slot is not empty, then it will set the state of the task slot to
     * releasing, fail all tasks and return -1.
     *
     * @param allocationId identifying the task slot to be freed
     * @throws SlotNotFoundException if there is not task slot for the given allocation id
     * @return Index of the freed slot if the slot could be freed; otherwise -1
     */
    // 释放 Slot。
    // 如果 Slot 内仍有任务在运行，会将状态设为 Releasing 并让任务失败；如果为空，则直接设为 Free。
    default int freeSlot(AllocationID allocationId) throws SlotNotFoundException {
        return freeSlot(allocationId, new Exception("The task slot of this task is being freed."));
    }

    /**
     * Tries to free the slot. If the slot is empty it will set the state of the task slot to free
     * and return its index. If the slot is not empty, then it will set the state of the task slot
     * to releasing, fail all tasks and return -1.
     *
     * @param allocationId identifying the task slot to be freed
     * @param cause to fail the tasks with if slot is not empty
     * @throws SlotNotFoundException if there is not task slot for the given allocation id
     * @return Index of the freed slot if the slot could be freed; otherwise -1
     */
    // 释放 Slot。
    int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException;

    /**
     * Check whether the timeout with ticket is valid for the given allocation id.
     *
     * @param allocationId to check against
     * @param ticket of the timeout
     * @return True if the timeout is valid; otherwise false
     */
    // 校验超时票据是否有效（防止过期的超时信号干扰当前分配）。
    boolean isValidTimeout(AllocationID allocationId, UUID ticket);

    /**
     * Check whether the slot for the given index is allocated for the given job and allocation id.
     *
     * @param index of the task slot
     * @param jobId for which the task slot should be allocated
     * @param allocationId which should match the task slot's allocation id
     * @return True if the given task slot is allocated for the given job and allocation id
     */
    boolean isAllocated(int index, JobID jobId, AllocationID allocationId);

    /**
     * Try to mark the specified slot as active if it has been allocated by the given job.
     *
     * @param jobId of the allocated slot
     * @param allocationId identifying the allocation
     * @return True if the task slot could be marked active.
     */
    boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId);

    /**
     * Check whether the task slot with the given index is free.
     *
     * @param index of the task slot
     * @return True if the task slot is free; otherwise false
     */
    // 检查指定索引的物理槽位是否空闲。
    boolean isSlotFree(int index);

    /**
     * Check whether the job has allocated (not active) slots.
     *
     * @param jobId for which to check for allocated slots
     * @return True if there are allocated slots for the given job id.
     */
    boolean hasAllocatedSlots(JobID jobId);

    /**
     * Return an iterator of allocated slots for the given job id.
     *
     * @param jobId for which to return the allocated slots
     * @return Iterator of allocated slots.
     */
    Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId);

    /**
     * Returns the owning job of the {@link TaskSlot} identified by the given {@link AllocationID}.
     *
     * @param allocationId identifying the slot for which to retrieve the owning job
     * @return Owning job of the specified {@link TaskSlot} or null if there is no slot for the
     *     given allocation id or if the slot has no owning job assigned
     */
    // 根据分配 ID 查找所属的 JobID。
    @Nullable
    JobID getOwningJob(AllocationID allocationId);

    /**
     * Add the given task to the slot identified by the task's allocation id.
     *
     * @param task to add to the task slot with the respective allocation id
     * @throws SlotNotFoundException if there was no slot for the given allocation id
     * @throws SlotNotActiveException if there was no slot active for task's job and allocation id
     * @return True if the task could be added to the task slot; otherwise false
     */
    // 将一个具体的 Task 放入已激活的 Slot 中。
    // 对应的 Slot 必须已经是 Active 状态。
    boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException;

    /**
     * Remove the task with the given execution attempt id from its task slot. If the owning task
     * slot is in state releasing and empty after removing the task, the slot is freed via the slot
     * actions.
     *
     * @param executionAttemptID identifying the task to remove
     * @return The removed task if there is any for the given execution attempt id; otherwise null
     */
    // 根据执行尝试 ID 移除任务。
    // 移除后如果 Slot 处于 Releasing 状态且已空，则彻底释放 Slot。
    T removeTask(ExecutionAttemptID executionAttemptID);

    /**
     * Get the task for the given execution attempt id. If none could be found, then return null.
     *
     * @param executionAttemptID identifying the requested task
     * @return The task for the given execution attempt id if it exist; otherwise null
     */
    // 查询任务实例。
    // 支持单体查询或按作业批量查询。
    T getTask(ExecutionAttemptID executionAttemptID);

    /**
     * Return an iterator over all tasks for a given job.
     *
     * @param jobId identifying the job of the requested tasks
     * @return Iterator over all task for a given job
     */
    // 查询任务实例。支持单体查询或按作业批量查询。
    Iterator<T> getTasks(JobID jobId);

    /**
     * Get the current allocation for the task slot with the given index.
     *
     * @param index identifying the slot for which the allocation id shall be retrieved
     * @return Allocation id of the specified slot if allocated; otherwise null
     */
    AllocationID getCurrentAllocation(int index);

    /**
     * Get the memory manager of the slot allocated for the task.
     *
     * @param allocationID allocation id of the slot allocated for the task
     * @return the memory manager of the slot allocated for the task
     */
    // 获取该 Slot 专有的内存管理器。
    // Flink 的托管内存是按 Slot 隔离的，通过此方法，Task 可以获取到属于自己的那部分内存份额。
    MemoryManager getTaskMemoryManager(AllocationID allocationID) throws SlotNotFoundException;
}
