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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.DualKeyLinkedMap;
import org.apache.flink.util.FlinkException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple implementation of {@link ExecutionSlotAllocator}. No support for slot sharing,
 * co-location, nor local recovery.
 */
// SimpleExecutionSlotAllocator 是 Flink 调度层用来分配执行槽位的组件，它实现了 ExecutionSlotAllocator 接口。
// 为任务分配逻辑槽位： 接收来自 Flink 调度器的执行尝试 ID (ExecutionAttemptID) 列表，并为每个 ID 分配一个逻辑槽位 (LogicalSlot)。
// 桥接物理槽位提供者： 它将逻辑槽位请求转换为对 PhysicalSlotProvider 的调用，从而触发实际的物理资源分配。
// 它为每个任务分配一个独立的物理槽位，是 Flink 基本调度策略中处理资源请求的核心。
public class SimpleExecutionSlotAllocator implements ExecutionSlotAllocator {
    // 物理槽位提供者。
    // 负责实际向 SlotPool 请求物理槽位。本类通过它将槽位请求转发给 JobMaster 的资源管理层。
    private final PhysicalSlotProvider slotProvider;
    // 槽位是否永久占用。
    // 标志分配的槽位是用于流式作业（永久占用，true）还是批处理作业（非永久占用，false）。
    // 这会影响底层 PhysicalSlotProvider 请求槽位的方式。
    private final boolean slotWillBeOccupiedIndefinitely;
    // 资源配置获取函数。
    // 一个函数式接口，用于根据给定的 ExecutionAttemptID 获取该任务所需的资源配置文件 (ResourceProfile)。
    private final Function<ExecutionAttemptID, ResourceProfile> resourceProfileRetriever;
    // 首选位置获取器。
    // 用于同步获取任务执行的首选 TaskManager 位置（即数据本地性偏好）。
    private final SyncPreferredLocationsRetriever preferredLocationsRetriever;
    // 已请求槽位的映射表。
    // 它使用任务 ID 和槽位请求 ID 作为双键，存储正在进行的逻辑槽位请求的 Future。用于跟踪和取消正在等待的槽位。
    private final DualKeyLinkedMap<
                    ExecutionAttemptID, SlotRequestId, CompletableFuture<LogicalSlot>>
            requestedPhysicalSlots;

    SimpleExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            Function<ExecutionAttemptID, ResourceProfile> resourceProfileRetriever,
            SyncPreferredLocationsRetriever preferredLocationsRetriever,
            boolean slotWillBeOccupiedIndefinitely) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.preferredLocationsRetriever = checkNotNull(preferredLocationsRetriever);
        this.requestedPhysicalSlots = new DualKeyLinkedMap<>();
    }
    // 用于接收一批需要分配槽位的任务，并为它们启动槽位请求流程。
    // 接收一个需要分配槽位的执行尝试 ID 列表 (executionAttemptIds)，
    // 并返回一个映射，将每个执行尝试 ID 映射到其对应的槽位分配结果 (ExecutionSlotAssignment)。
    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {
        // 存储最终的槽位分配结果
        Map<ExecutionAttemptID, ExecutionSlotAssignment> result = new HashMap<>();
        // 存储本次需要发起新请求的任务。
        // 它将新生成的槽位请求 ID 映射回对应的执行尝试 ID。
        Map<SlotRequestId, ExecutionAttemptID> remainingExecutionsToSlotRequest =
                new HashMap<>(executionAttemptIds.size());
        // 存储本次循环中生成的所有新的物理槽位请求
        List<PhysicalSlotRequest> physicalSlotRequests =
                new ArrayList<>(executionAttemptIds.size());

        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
            // 检查当前任务是否已经存在一个正在进行中（已请求但尚未完成）的槽位请求
            if (requestedPhysicalSlots.containsKeyA(executionAttemptId)) {
                // 如果请求已存在，则将已有的槽位 Future 加入结果集
                result.put(
                        executionAttemptId,
                        new ExecutionSlotAssignment(
                                executionAttemptId,
                                requestedPhysicalSlots.getValueByKeyA(executionAttemptId)));
            } else {
                // 为本次新的槽位分配创建一个唯一标识符 (SlotRequestId)
                final SlotRequestId slotRequestId = new SlotRequestId();
                final ResourceProfile resourceProfile =
                        resourceProfileRetriever.apply(executionAttemptId);
                Collection<TaskManagerLocation> preferredLocations =
                        preferredLocationsRetriever.getPreferredLocations(
                                executionAttemptId.getExecutionVertexId(), Collections.emptySet());
                final SlotProfile slotProfile =
                        SlotProfile.priorAllocation(
                                resourceProfile,
                                resourceProfile,
                                preferredLocations,
                                Collections.emptyList(),
                                Collections.emptySet());
                // 创建实际的物理槽位请求对象
                final PhysicalSlotRequest request =
                        new PhysicalSlotRequest(
                                slotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);
                physicalSlotRequests.add(request);
                remainingExecutionsToSlotRequest.put(slotRequestId, executionAttemptId);
            }
        }

        result.putAll(
                // 将 所有新生成的 PhysicalSlotRequest 批量转发给底层 PhysicalSlotProvider (slotProvider) 进行实际的分配
                allocatePhysicalSlotsFor(remainingExecutionsToSlotRequest, physicalSlotRequests));
        return result;
    }

    private Map<ExecutionAttemptID, ExecutionSlotAssignment> allocatePhysicalSlotsFor(
            Map<SlotRequestId, ExecutionAttemptID> executionAttemptIds,
            List<PhysicalSlotRequest> slotRequests) {
        Map<ExecutionAttemptID, ExecutionSlotAssignment> allocatedSlots = new HashMap<>();
        Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> slotFutures =
                slotProvider.allocatePhysicalSlots(slotRequests);

        slotFutures.forEach(
                (slotRequestId, slotRequestResultFuture) -> {
                    ExecutionAttemptID executionAttemptId = executionAttemptIds.get(slotRequestId);

                    final CompletableFuture<LogicalSlot> slotFuture =
                            slotRequestResultFuture.thenApply(
                                    physicalSlotRequest ->
                                            allocateLogicalSlotFromPhysicalSlot(
                                                    slotRequestId,
                                                    physicalSlotRequest.getPhysicalSlot(),
                                                    slotWillBeOccupiedIndefinitely));
                    slotFuture.exceptionally(
                            throwable -> {
                                this.requestedPhysicalSlots.removeKeyA(executionAttemptId);
                                this.slotProvider.cancelSlotRequest(slotRequestId, throwable);
                                return null;
                            });
                    requestedPhysicalSlots.put(executionAttemptId, slotRequestId, slotFuture);
                    allocatedSlots.put(
                            executionAttemptId,
                            new ExecutionSlotAssignment(executionAttemptId, slotFuture));
                });
        return allocatedSlots;
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        final CompletableFuture<LogicalSlot> slotFuture =
                this.requestedPhysicalSlots.getValueByKeyA(executionAttemptId);
        if (slotFuture != null) {
            slotFuture.cancel(false);
        }
    }

    private void returnLogicalSlot(LogicalSlot slot) {
        releaseSlot(
                slot,
                new FlinkException("Slot is being returned from SimpleExecutionSlotAllocator."));
    }

    private void releaseSlot(LogicalSlot slot, Throwable cause) {
        requestedPhysicalSlots.removeKeyB(slot.getSlotRequestId());
        slotProvider.cancelSlotRequest(slot.getSlotRequestId(), cause);
    }

    private LogicalSlot allocateLogicalSlotFromPhysicalSlot(
            final SlotRequestId slotRequestId,
            final PhysicalSlot physicalSlot,
            final boolean slotWillBeOccupiedIndefinitely) {

        final SingleLogicalSlot singleLogicalSlot =
                new SingleLogicalSlot(
                        slotRequestId,
                        physicalSlot,
                        Locality.UNKNOWN,
                        this::returnLogicalSlot,
                        slotWillBeOccupiedIndefinitely);

        final LogicalSlotHolder logicalSlotHolder = new LogicalSlotHolder(singleLogicalSlot);
        if (physicalSlot.tryAssignPayload(logicalSlotHolder)) {
            return singleLogicalSlot;
        } else {
            throw new IllegalStateException(
                    "BUG: Unexpected physical slot payload assignment failure!");
        }
    }

    private class LogicalSlotHolder implements PhysicalSlot.Payload {
        private final SingleLogicalSlot logicalSlot;

        private LogicalSlotHolder(SingleLogicalSlot logicalSlot) {
            this.logicalSlot = checkNotNull(logicalSlot);
        }

        @Override
        public void release(Throwable cause) {
            logicalSlot.release(cause);
            releaseSlot(logicalSlot, new FlinkException("Physical slot releases its payload."));
        }

        @Override
        public boolean willOccupySlotIndefinitely() {
            return logicalSlot.willOccupySlotIndefinitely();
        }
    }

    /** Factory to instantiate a {@link SimpleExecutionSlotAllocator}. */
    public static class Factory implements ExecutionSlotAllocatorFactory {
        private final PhysicalSlotProvider slotProvider;

        private final boolean slotWillBeOccupiedIndefinitely;

        public Factory(PhysicalSlotProvider slotProvider, boolean slotWillBeOccupiedIndefinitely) {
            this.slotProvider = slotProvider;
            this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        }

        @Override
        public ExecutionSlotAllocator createInstance(ExecutionSlotAllocationContext context) {
            SyncPreferredLocationsRetriever preferredLocationsRetriever =
                    new DefaultSyncPreferredLocationsRetriever(
                            executionVertexId -> Optional.empty(), context);
            return new SimpleExecutionSlotAllocator(
                    slotProvider,
                    id -> context.getResourceProfile(id.getExecutionVertexId()),
                    preferredLocationsRetriever,
                    slotWillBeOccupiedIndefinitely);
        }
    }
}
