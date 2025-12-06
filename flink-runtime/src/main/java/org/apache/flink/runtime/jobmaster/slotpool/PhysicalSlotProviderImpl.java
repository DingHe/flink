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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The provider serves physical slot requests. */
// PhysicalSlotProviderImpl 是 Flink 中 PhysicalSlotProvider 接口的实现，它是 JobMaster 调度器层（如 SchedulerNG）用于获取实际物理槽位的主要入口。
// 策略驱动的槽位选择： 它结合了槽位选择策略（SlotSelectionStrategy），从当前 JobMaster 槽位池（SlotPool）中的可用空闲槽位中，选择出最合适的槽位来满足任务的资源需求和位置偏好。
// 统一请求接口： 它向上层提供了一个统一的 allocatePhysicalSlots 接口，可以批量处理多个槽位请求。
// 桥接分配逻辑： 如果没有合适的空闲槽位，它会通过 SlotPool 向底层（DeclarativeSlotPoolBridge 和 DeclarativeSlotPool）发起新的资源分配请求，从而触发与 ResourceManager 的通信。
// 是 JobMaster 中将调度请求（PhysicalSlotRequest）转化为实际的槽位分配动作的关键组件。
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);
    // 槽位选择策略。
    // 核心组件之一。定义了如何从一组可用的空闲槽位中，根据资源配置文件（SlotProfile）和位置偏好，选择出最优槽位的逻辑。
    private final SlotSelectionStrategy slotSelectionStrategy;
    // 核心组件之二。引用 JobMaster 中负责维护槽位状态和管理资源需求的组件（例如 DeclarativeSlotPoolBridge）。它用于执行实际的槽位分配和释放操作。
    private final SlotPool slotPool;

    public PhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.slotPool = checkNotNull(slotPool);
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }
    // 分配物理槽位。
    @Override
    public Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests) {

        for (PhysicalSlotRequest physicalSlotRequest : physicalSlotRequests) {
            LOG.debug(
                    "Received slot request [{}] with resource requirements: {}",
                    physicalSlotRequest.getSlotRequestId(),
                    physicalSlotRequest.getSlotProfile().getPhysicalSlotResourceProfile());
        }

        Map<SlotRequestId, PhysicalSlotRequest> physicalSlotRequestsById =
                physicalSlotRequests.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlotRequest::getSlotRequestId,
                                        Function.identity()));
        Map<SlotRequestId, Optional<PhysicalSlot>> availablePhysicalSlots =
                tryAllocateFromAvailable(physicalSlotRequestsById.values());

        return availablePhysicalSlots.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    Optional<PhysicalSlot> availablePhysicalSlot = entry.getValue();
                                    SlotRequestId slotRequestId = entry.getKey();
                                    PhysicalSlotRequest physicalSlotRequest =
                                            physicalSlotRequestsById.get(slotRequestId);
                                    SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
                                    ResourceProfile resourceProfile =
                                            slotProfile.getPhysicalSlotResourceProfile();

                                    CompletableFuture<PhysicalSlot> slotFuture =
                                            availablePhysicalSlot
                                                    .map(CompletableFuture::completedFuture)
                                                    .orElseGet(
                                                            () ->
                                                                    requestNewSlot(
                                                                            slotRequestId,
                                                                            resourceProfile,
                                                                            slotProfile
                                                                                    .getPreferredAllocations(),
                                                                            physicalSlotRequest
                                                                                    .willSlotBeOccupiedIndefinitely()));

                                    return slotFuture.thenApply(
                                            physicalSlot ->
                                                    new PhysicalSlotRequest.Result(
                                                            slotRequestId, physicalSlot));
                                }));
    }
    // 尝试从现有空闲槽位分配。
    private Map<SlotRequestId, Optional<PhysicalSlot>> tryAllocateFromAvailable(
            Collection<PhysicalSlotRequest> slotRequests) {
        FreeSlotTracker freeSlotTracker = slotPool.getFreeSlotTracker();

        Map<SlotRequestId, Optional<PhysicalSlot>> allocateResult = new HashMap<>();
        for (PhysicalSlotRequest request : slotRequests) {
            Optional<SlotSelectionStrategy.SlotInfoAndLocality> slot =
                    slotSelectionStrategy.selectBestSlotForProfile(
                            freeSlotTracker, request.getSlotProfile());
            allocateResult.put(
                    request.getSlotRequestId(),
                    slot.flatMap(
                            slotInfoAndLocality -> {
                                freeSlotTracker.reserveSlot(
                                        slotInfoAndLocality.getSlotInfo().getAllocationId());
                                return slotPool.allocateAvailableSlot(
                                        request.getSlotRequestId(),
                                        slotInfoAndLocality.getSlotInfo().getAllocationId(),
                                        request.getSlotProfile().getPhysicalSlotResourceProfile());
                            }));
        }
        return allocateResult;
    }

    private CompletableFuture<PhysicalSlot> requestNewSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean willSlotBeOccupiedIndefinitely) {
        if (willSlotBeOccupiedIndefinitely) {
            return slotPool.requestNewAllocatedSlot(
                    slotRequestId, resourceProfile, preferredAllocations, null);
        } else {
            return slotPool.requestNewAllocatedBatchSlot(
                    slotRequestId, resourceProfile, preferredAllocations);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
