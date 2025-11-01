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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Container for multiple {@link TaskSlotPayload tasks} belonging to the same slot. A {@link
 * TaskSlot} can be in one of the following states:
 *
 * <ul>
 *   <li>Free - The slot is empty and not allocated to a job
 *   <li>Releasing - The slot is about to be freed after it has become empty.
 *   <li>Allocated - The slot has been allocated for a job.
 *   <li>Active - The slot is in active use by a job manager which is the leader of the allocating
 *       job.
 * </ul>
 *
 * <p>A task slot can only be allocated if it is in state free. An allocated task slot can transit
 * to state active.
 *
 * <p>An active slot allows to add tasks from the respective job and with the correct allocation id.
 * An active slot can be marked as inactive which sets the state back to allocated.
 *
 * <p>An allocated or active slot can only be freed if it is empty. If it is not empty, then it's
 * state can be set to releasing indicating that it can be freed once it becomes empty.
 *
 * @param <T> type of the {@link TaskSlotPayload} stored in this slot
 */
// TaskSlot 是 Flink 任务执行器 (TaskExecutor) 上的一个逻辑资源容器。
// 它代表了 TaskExecutor 可以提供给 JobMaster/ ResourceManager 使用的、带有特定计算和内存资源的单元
// 资源封装： 封装了一组可用的资源（由 ResourceProfile 定义），特别是它拥有自己的 MemoryManager，用于管理分配给该 Slot 的内存。
// 任务容器： 存储和管理当前在这个 Slot 上运行的 TaskSlotPayload（通常是实际的 Flink 任务 Task）。一个 Slot 可以包含多个任务（例如，当 Slot 共享被启用，或者为了支持 Streaming HA 等机制）
// TaskSlot 是 Flink 资源调度和任务执行的基本单位
public class TaskSlot<T extends TaskSlotPayload> implements AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(TaskSlot.class);

    /** Index of the task slot.*/
    // 槽位索引（ID）。
    // 在 TaskExecutor 内部，用于唯一标识这个 TaskSlot 的序号
    private final int index;

    /** Resource characteristics for this slot.*/
    // 槽位资源特征。
    // 描述了分配给这个 TaskSlot 的 CPU、管理内存、网络内存等资源总量
    private final ResourceProfile resourceProfile;

    /** Tasks running in this slot.*/
    // 任务集合。
    // 存储当前在这个槽位中运行的所有任务（TaskSlotPayload）。
    // 键是任务的执行尝试 ID，值是任务的实际负载。
    private final Map<ExecutionAttemptID, T> tasks;
    // 内存管理器。
    // 专门用于管理分配给这个 TaskSlot 的托管内存 (Managed Memory)。
    // 任务需要使用托管内存时，会向这个管理器申请。
    private final MemoryManager memoryManager;

    /** State of this slot. */
    // 槽位状态。
    // 标识 TaskSlot 当前所处的生命周期状态（FREE, ALLOCATED, ACTIVE, RELEASING）
    private TaskSlotState state;

    /** Job id to which the slot has been allocated.*/
    // 分配的 Job ID。
    // 标识这个 Slot 当前被分配给了哪个 Job。在 Slot 被分配时确定。
    private final JobID jobId;

    /** Allocation id of this slot. */
    // 分配 ID。
    // 标识这个 Slot 的当前分配实例。每次新的分配请求成功，都会生成一个新的 AllocationID。
    private final AllocationID allocationId;

    /** The closing future is completed when the slot is freed and closed. */
    // 关闭完成 Future。
    // 一个异步操作的完成标识。它会在 TaskSlot 被完全清理并释放资源后完成，用于通知等待者 Slot 已释放。
    private final CompletableFuture<Void> closingFuture;

    /** {@link Executor} for background actions, e.g. verify all managed memory released. */
    // 异步执行器。
    // 用于执行 TaskSlot 内部的后台异步操作，例如在关闭后验证托管内存是否完全释放。
    private final Executor asyncExecutor;

    public TaskSlot(
            final int index,
            final ResourceProfile resourceProfile,
            final int memoryPageSize,
            final JobID jobId,
            final AllocationID allocationId,
            final Executor asyncExecutor) {

        this.index = index;
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
        this.asyncExecutor = Preconditions.checkNotNull(asyncExecutor);

        this.tasks = CollectionUtil.newHashMapWithExpectedSize(4);
        this.state = TaskSlotState.ALLOCATED;

        this.jobId = jobId;
        this.allocationId = allocationId;

        this.memoryManager = createMemoryManager(resourceProfile, memoryPageSize);

        this.closingFuture = new CompletableFuture<>();
    }

    // ----------------------------------------------------------------------------------
    // State accessors
    // ----------------------------------------------------------------------------------

    public int getIndex() {
        return index;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public JobID getJobId() {
        return jobId;
    }

    public AllocationID getAllocationId() {
        return allocationId;
    }

    TaskSlotState getState() {
        return state;
    }

    public boolean isEmpty() {
        return tasks.isEmpty();
    }

    public boolean isActive(JobID activeJobId, AllocationID activeAllocationId) {
        Preconditions.checkNotNull(activeJobId);
        Preconditions.checkNotNull(activeAllocationId);

        return TaskSlotState.ACTIVE == state
                && activeJobId.equals(jobId)
                && activeAllocationId.equals(allocationId);
    }

    public boolean isAllocated(JobID jobIdToCheck, AllocationID allocationIDToCheck) {
        Preconditions.checkNotNull(jobIdToCheck);
        Preconditions.checkNotNull(allocationIDToCheck);

        return jobIdToCheck.equals(jobId)
                && allocationIDToCheck.equals(allocationId)
                && (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state);
    }

    public boolean isReleasing() {
        return TaskSlotState.RELEASING == state;
    }

    /**
     * Get all tasks running in this task slot.
     *
     * @return Iterator to all currently contained tasks in this task slot.
     */
    public Iterator<T> getTasks() {
        return tasks.values().iterator();
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    // ----------------------------------------------------------------------------------
    // State changing methods
    // ----------------------------------------------------------------------------------

    /**
     * Add the given task to the task slot. This is only possible if there is not already another
     * task with the same execution attempt id added to the task slot. In this case, the method
     * returns true. Otherwise the task slot is left unchanged and false is returned.
     *
     * <p>In case that the task slot state is not active an {@link IllegalStateException} is thrown.
     * In case that the task's job id and allocation id don't match with the job id and allocation
     * id for which the task slot has been allocated, an {@link IllegalArgumentException} is thrown.
     * @param task to be added to the task slot
     * @throws IllegalStateException if the task slot is not in state active
     * @return true if the task was added to the task slot; otherwise false
     */
    // 添加任务到槽位，
    // 验证任务的作业 ID 和分配 ID 与槽位的 jobId 和 allocationId 一致，确保槽位处于 ACTIVE 状态
    public boolean add(T task) {
        // Check that this slot has been assigned to the job sending this task
        Preconditions.checkArgument(
                task.getJobID().equals(jobId),
                "The task's job id does not match the "
                        + "job id for which the slot has been allocated.");
        Preconditions.checkArgument(
                task.getAllocationId().equals(allocationId),
                "The task's allocation "
                        + "id does not match the allocation id for which the slot has been allocated.");
        Preconditions.checkState(
                TaskSlotState.ACTIVE == state, "The task slot is not in state active.");

        T oldTask = tasks.put(task.getExecutionId(), task);

        if (oldTask != null) {
            tasks.put(task.getExecutionId(), oldTask);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Remove the task identified by the given execution attempt id.
     *
     * @param executionAttemptId identifying the task to be removed
     * @return The removed task if there was any; otherwise null.
     */
    // 删除任务
    public T remove(ExecutionAttemptID executionAttemptId) {
        return tasks.remove(executionAttemptId);
    }

    /** Removes all tasks from this task slot. */
    // 清理全部任务
    public void clear() {
        tasks.clear();
    }

    /**
     * Mark this slot as active. A slot can only be marked active if it's in state allocated.
     *
     * <p>The method returns true if the slot was set to active. Otherwise it returns false.
     *
     * @return True if the new state of the slot is active; otherwise false
     */
    // 激活TaskSlot
    public boolean markActive() {
        if (TaskSlotState.ALLOCATED == state || TaskSlotState.ACTIVE == state) {
            state = TaskSlotState.ACTIVE;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Mark the slot as inactive/allocated. A slot can only be marked as inactive/allocated if it's
     * in state allocated or active.
     *
     * @return True if the new state of the slot is allocated; otherwise false
     */
    // 失活TaskSlot
    public boolean markInactive() {
        if (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state) {
            state = TaskSlotState.ALLOCATED;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Generate the slot offer from this TaskSlot.
     *
     * @return The sot offer which this task slot can provide
     */
    // 生成 Slot Offer。
    // 将当前 TaskSlot 的关键信息（AllocationID、index、ResourceProfile）封装成一个 SlotOffer 对象，用于向 JobMaster 报告该 Slot 的可用性。
    public SlotOffer generateSlotOffer() {
        Preconditions.checkState(
                TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state,
                "The task slot is not in state active or allocated.");
        Preconditions.checkState(allocationId != null, "The task slot are not allocated");

        return new SlotOffer(allocationId, index, resourceProfile);
    }

    @Override
    public String toString() {
        return "TaskSlot(index:"
                + index
                + ", state:"
                + state
                + ", resource profile: "
                + resourceProfile
                + ", allocationId: "
                + (allocationId != null ? allocationId.toString() : "none")
                + ", jobId: "
                + (jobId != null ? jobId.toString() : "none")
                + ')';
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeAsync(new FlinkException("Closing the slot"));
    }

    /**
     * Close the task slot asynchronously.
     *
     * <p>Slot is moved to {@link TaskSlotState#RELEASING} state and only once. If there are active
     * tasks running in the slot then they are failed. The future of all tasks terminated and slot
     * cleaned up is initiated only once and always returned in case of multiple attempts to close
     * the slot.
     *
     * @param cause cause of closing
     * @return future of all running task if any being done and slot cleaned up.
     */
    CompletableFuture<Void> closeAsync(Throwable cause) {
        if (!isReleasing()) {
            state = TaskSlotState.RELEASING;
            if (!isEmpty()) {
                // we couldn't free the task slot because it still contains task, fail the tasks
                // and set the slot state to releasing so that it gets eventually freed
                tasks.values().forEach(task -> task.failExternally(cause));
            }

            final CompletableFuture<Void> shutdownFuture =
                    FutureUtils.waitForAll(
                                    tasks.values().stream()
                                            .map(TaskSlotPayload::getTerminationFuture)
                                            .collect(Collectors.toList()))
                            .thenRun(memoryManager::shutdown);
            verifyAllManagedMemoryIsReleasedAfter(shutdownFuture);
            FutureUtils.forward(shutdownFuture, closingFuture);
        }
        return closingFuture;
    }

    private void verifyAllManagedMemoryIsReleasedAfter(CompletableFuture<Void> after) {
        after.thenRunAsync(
                () -> {
                    if (!memoryManager.verifyEmpty()) {
                        LOG.warn(
                                "Not all slot managed memory is freed at {}. This usually indicates memory leak. "
                                        + "However, when running an old JVM version it can also be caused by slow garbage collection. "
                                        + "Try to upgrade to Java 8u72 or higher if running on an old Java version.",
                                this);
                    }
                },
                asyncExecutor);
    }

    private static MemoryManager createMemoryManager(
            ResourceProfile resourceProfile, int pageSize) {
        return MemoryManager.create(resourceProfile.getManagedMemory().getBytes(), pageSize);
    }
}
