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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link SlotPool} implementation which uses the {@link DeclarativeSlotPool} to allocate slots. */
// DeclarativeSlotPoolBridge 类是 Flink 中 SlotPool 接口的一个实现，它继承自 DeclarativeSlotPoolService。
// 它的主要作用是作为 JobMaster 的旧调度器（通常是基于请求-响应模型的调度器，如 SchedulerNG）与新声明式资源管理核心（即 DeclarativeSlotPool）之间的桥梁 (Bridge)。
// 适配请求-响应模型： 它接收来自上层调度器（例如，JobGraph 阶段）的具体的槽位请求（requestNewAllocatedSlot），并将这些请求转换为对底层 DeclarativeSlotPool 的抽象资源需求。
// 管理待定请求： 它内部维护了所有待处理的槽位请求（PendingRequest），并在底层 DeclarativeSlotPool 报告有新的可用槽位时，负责将这些槽位与待处理请求进行匹配和分配。
// 超时处理： 它实现了对空闲槽位和批处理槽位请求的超时检查逻辑。
// 作业重启逻辑： 在作业重启期间，它能够区别对待槽位提供，确保快速恢复。
public class DeclarativeSlotPoolBridge extends DeclarativeSlotPoolService implements SlotPool {
    // 待处理的槽位请求集合。
    // 存储所有尚未被满足的槽位请求。使用 LinkedHashMap 可能是为了维护请求的插入顺序，便于匹配和超时检查。
    private final Map<SlotRequestId, PendingRequest> pendingRequests;
    // 已满足请求与槽位 ID 的映射。
    // 存储已经被分配槽位但尚未释放的请求。用于通过 SlotRequestId 快速查找对应的 AllocationID。
    private final Map<SlotRequestId, AllocationID> fulfilledRequests;
    // 空闲槽位超时时间。
    private final Duration idleSlotTimeout;
    // 请求槽位匹配策略。
    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;
    // 批处理槽位请求超时时间
    private final Duration batchSlotTimeout;
    // 批处理超时检查开关。
    private boolean isBatchSlotRequestTimeoutCheckDisabled;
    // 作业重启标志。
    // 标志当前作业是否正在重启过程中。重启期间，槽位提供可能被特殊处理 (registerSlots)。
    private boolean isJobRestarting = false;
    // 槽位批量分配标志。
    // 如果为 true，槽位匹配将等待收集到足够的槽位后再进行批量分配（针对批处理场景）。
    private final boolean slotBatchAllocatable;

    public DeclarativeSlotPoolBridge(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Duration rpcTimeout,
            Duration idleSlotTimeout,
            Duration batchSlotTimeout,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy,
            Duration slotRequestMaxInterval,
            boolean slotBatchAllocatable,
            @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) {
        super(
                jobId,
                declarativeSlotPoolFactory,
                clock,
                idleSlotTimeout,
                rpcTimeout,
                slotRequestMaxInterval,
                componentMainThreadExecutor);

        this.idleSlotTimeout = idleSlotTimeout;
        this.batchSlotTimeout = Preconditions.checkNotNull(batchSlotTimeout);

        log.debug(
                "Using the request slot matching strategy: {}",
                requestSlotMatchingStrategy.getClass().getSimpleName());
        this.requestSlotMatchingStrategy = requestSlotMatchingStrategy;
        this.slotBatchAllocatable = slotBatchAllocatable;

        this.isBatchSlotRequestTimeoutCheckDisabled = false;

        this.pendingRequests = new LinkedHashMap<>();
        this.fulfilledRequests = new HashMap<>();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return Optional.of(clazz.cast(this));
        }

        return Optional.empty();
    }

    @Override
    protected void onStart() {

        getDeclarativeSlotPool().registerNewSlotsListener(this::newSlotsAreAvailable);

        componentMainThreadExecutor.schedule(
                this::checkIdleSlotTimeout, idleSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
        componentMainThreadExecutor.schedule(
                this::checkBatchSlotTimeout, batchSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void onClose() {
        final FlinkException cause = new FlinkException("Closing slot pool");
        cancelPendingRequests(request -> true, cause);
    }

    /**
     * To set whether the underlying is currently restarting or not. In the former case the slot
     * pool bridge will accept all incoming slot offers.
     *
     * @param isJobRestarting whether this is restarting or not
     */
    @Override
    public void setIsJobRestarting(boolean isJobRestarting) {
        this.isJobRestarting = isJobRestarting;
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();

        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        if (isJobRestarting) {
            return getDeclarativeSlotPool()
                    .registerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());

        } else {
            return getDeclarativeSlotPool()
                    .offerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());
        }
    }

    private void cancelPendingRequests(
            Predicate<PendingRequest> requestPredicate, FlinkException cancelCause) {

        ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

        // need a copy since failing a request could trigger another request to be issued
        final Iterable<PendingRequest> pendingRequestsToFail =
                new ArrayList<>(pendingRequests.values());
        pendingRequests.clear();

        for (PendingRequest pendingRequest : pendingRequestsToFail) {
            if (requestPredicate.test(pendingRequest)) {
                pendingRequest.failRequest(cancelCause);
                decreasedResourceRequirements =
                        decreasedResourceRequirements.add(pendingRequest.getResourceProfile(), 1);
            } else {
                pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
            }
        }

        getDeclarativeSlotPool().decreaseResourceRequirementsBy(decreasedResourceRequirements);
    }

    @Override
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
    }

    @VisibleForTesting
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        if (pendingRequests.isEmpty()) {
            return;
        }

        if (slotBatchAllocatable) {
            newSlotsAvailableForSlotBatchAllocatable(newSlots);
        } else {
            newSlotsAvailableForDirectlyAllocatable(newSlots);
        }
    }

    private void newSlotsAvailableForSlotBatchAllocatable(
            Collection<? extends PhysicalSlot> newSlots) {
        log.debug("Received new available slots: {}", newSlots);

        final FreeSlotTracker freeSlotInfoTracker = getDeclarativeSlotPool().getFreeSlotTracker();

        final int slotsNum = freeSlotInfoTracker.getAvailableSlots().size();
        if (slotsNum < pendingRequests.size()) {
            // Do nothing and waiting slots.
            log.debug(
                    "The number of available slots: {}, the required number of slots: {}, waiting for more available slots.",
                    slotsNum,
                    pendingRequests.size());
            return;
        }

        final Collection<PhysicalSlot> availableSlots =
                freeSlotInfoTracker.getFreeSlotsInformation();
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        availableSlots, pendingRequests.values());
        if (requestSlotMatches.size() == pendingRequests.size()) {
            reserveAndFulfillMatchedFreeSlots(requestSlotMatches);
        } else if (requestSlotMatches.size() < pendingRequests.size()) {
            // Do nothing and waiting slots.
            log.debug(
                    "Ignored the matched results: {}, pendingRequests: {}, waiting for more available slots.",
                    requestSlotMatches,
                    pendingRequests);
        } else {
            // For requestSlotMatches.size() > pendingRequests.size()
            throw new IllegalStateException(
                    "The number of matched slots is not equals to the pendingRequests.");
        }
    }

    private void newSlotsAvailableForDirectlyAllocatable(
            Collection<? extends PhysicalSlot> newSlots) {
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        newSlots, pendingRequests.values());
        reserveAndFulfillMatchedFreeSlots(requestSlotMatches);
    }

    private void reserveAndFulfillMatchedFreeSlots(
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches) {
        for (RequestSlotMatchingStrategy.RequestSlotMatch match : requestSlotMatches) {
            final PendingRequest pendingRequest = match.getPendingRequest();
            final PhysicalSlot slot = match.getSlot();

            log.debug("Matched pending request {} with slot {}.", pendingRequest, slot);

            Preconditions.checkNotNull(
                    pendingRequests.remove(pendingRequest.getSlotRequestId()),
                    "Cannot fulfill a non existing pending slot request.");

            reserveFreeSlot(
                    pendingRequest.getSlotRequestId(),
                    slot.getAllocationId(),
                    pendingRequest.getResourceProfile());
        }

        // we have to first reserve all matching slots before fulfilling the requests
        // otherwise it can happen that the scheduler reserves one of the new slots
        // for a request which has been triggered by fulfilling a pending request
        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            final PendingRequest pendingRequest = requestSlotMatch.getPendingRequest();
            final PhysicalSlot slot = requestSlotMatch.getSlot();

            Preconditions.checkState(
                    pendingRequest.fulfill(slot), "Pending requests must be fulfillable.");
        }
    }

    @VisibleForTesting
    Collection<PhysicalSlot> getFreeSlotsInformation() {
        return getDeclarativeSlotPool().getFreeSlotTracker().getFreeSlotsInformation();
    }

    private void reserveFreeSlot(
            SlotRequestId slotRequestId,
            AllocationID allocationId,
            ResourceProfile resourceProfile) {
        log.debug("Reserve slot {} for slot request id {}", allocationId, slotRequestId);
        getDeclarativeSlotPool().reserveFreeSlot(allocationId, resourceProfile);
        fulfilledRequests.put(slotRequestId, allocationId);
    }

    @Override
    public Optional<PhysicalSlot> allocateAvailableSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull AllocationID allocationID,
            @Nonnull ResourceProfile requirementProfile) {
        assertRunningInMainThread();
        Preconditions.checkNotNull(requirementProfile, "The requiredSlotProfile must not be null.");

        log.debug(
                "Reserving free slot {} for slot request id {} and profile {}.",
                allocationID,
                slotRequestId,
                requirementProfile);

        return Optional.of(
                reserveFreeSlotForResource(slotRequestId, allocationID, requirementProfile));
    }

    private PhysicalSlot reserveFreeSlotForResource(
            SlotRequestId slotRequestId,
            AllocationID allocationId,
            ResourceProfile requiredSlotProfile) {
        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(requiredSlotProfile, 1));
        final PhysicalSlot physicalSlot =
                getDeclarativeSlotPool().reserveFreeSlot(allocationId, requiredSlotProfile);
        fulfilledRequests.put(slotRequestId, allocationId);

        return physicalSlot;
    }

    @Override
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull ResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations,
            @Nullable Duration timeout) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated slot with slot request id {} and resource profile {}",
                slotRequestId,
                resourceProfile);

        final PendingRequest pendingRequest =
                PendingRequest.createNormalRequest(
                        slotRequestId, resourceProfile, preferredAllocations);

        return internalRequestNewSlot(pendingRequest, timeout);
    }

    @Override
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull ResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated batch slot with slot request id {} and resource profile {}",
                slotRequestId,
                resourceProfile);

        final PendingRequest pendingRequest =
                PendingRequest.createBatchRequest(
                        slotRequestId, resourceProfile, preferredAllocations);

        return internalRequestNewSlot(pendingRequest, null);
    }

    private CompletableFuture<PhysicalSlot> internalRequestNewSlot(
            PendingRequest pendingRequest, @Nullable Duration timeout) {
        internalRequestNewAllocatedSlot(pendingRequest);

        if (timeout == null) {
            return pendingRequest.getSlotFuture();
        } else {
            return FutureUtils.orTimeout(
                            pendingRequest.getSlotFuture(),
                            timeout.toMillis(),
                            TimeUnit.MILLISECONDS,
                            componentMainThreadExecutor,
                            String.format(
                                    "Pending slot request %s timed out after %d ms.",
                                    pendingRequest.getSlotRequestId(), timeout.toMillis()))
                    .whenComplete(
                            (physicalSlot, throwable) -> {
                                if (throwable instanceof TimeoutException) {
                                    timeoutPendingSlotRequest(pendingRequest.getSlotRequestId());
                                }
                            });
        }
    }

    private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
        releaseSlot(
                slotRequestId,
                new TimeoutException("Pending slot request timed out in slot pool."));
    }

    private void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
        pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);

        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
    }

    @Override
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirements);
    }

    @Override
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        log.debug("Release slot with slot request id {}", slotRequestId);
        assertRunningInMainThread();

        final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

        if (pendingRequest != null) {
            getDeclarativeSlotPool()
                    .decreaseResourceRequirementsBy(
                            ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
            pendingRequest.failRequest(
                    new FlinkException(
                            String.format(
                                    "Pending slot request with %s has been released.",
                                    pendingRequest.getSlotRequestId()),
                            cause));
        } else {
            final AllocationID allocationId = fulfilledRequests.remove(slotRequestId);

            if (allocationId != null) {
                ResourceCounter previouslyFulfilledRequirement =
                        getDeclarativeSlotPool()
                                .freeReservedSlot(allocationId, cause, getRelativeTimeMillis());
                getDeclarativeSlotPool()
                        .decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
            } else {
                log.debug(
                        "Could not find slot which has fulfilled slot request {}. Ignoring the release operation.",
                        slotRequestId);
            }
        }
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        assertRunningInMainThread();

        failPendingRequests(acquiredResources);
    }

    private void failPendingRequests(Collection<ResourceRequirement> acquiredResources) {
        // only fails streaming requests because batch jobs do not require all resources
        // requirements to be fullfilled at the same time
        Predicate<PendingRequest> predicate = request -> !request.isBatchRequest();
        if (pendingRequests.values().stream().anyMatch(predicate)) {
            log.warn(
                    "Could not acquire the minimum required resources, failing slot requests. Acquired: {}. Current slot pool status: {}",
                    acquiredResources,
                    getSlotServiceStatus());
            cancelPendingRequests(
                    predicate,
                    NoResourceAvailableException.withoutStackTrace(
                            "Could not acquire the minimum required resources."));
        }
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsInformation() {
        assertRunningInMainThread();

        final Collection<? extends SlotInfo> allSlotsInformation =
                getDeclarativeSlotPool().getAllSlotsInformation();
        final Set<AllocationID> freeSlots =
                getDeclarativeSlotPool().getFreeSlotTracker().getAvailableSlots();

        return allSlotsInformation.stream()
                .filter(slotInfo -> !freeSlots.contains(slotInfo.getAllocationId()))
                .collect(Collectors.toList());
    }

    @Override
    public FreeSlotTracker getFreeSlotTracker() {
        assertRunningInMainThread();

        return getDeclarativeSlotPool().getFreeSlotTracker();
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        isBatchSlotRequestTimeoutCheckDisabled = true;
    }

    private void assertRunningInMainThread() {
        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.assertRunningInMainThread();
        } else {
            throw new IllegalStateException("The FutureSlotPool has not been started yet.");
        }
    }

    private void checkIdleSlotTimeout() {
        getDeclarativeSlotPool().releaseIdleSlots(getRelativeTimeMillis());

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkIdleSlotTimeout, idleSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    void checkBatchSlotTimeout() {
        assertRunningInMainThread();

        if (isBatchSlotRequestTimeoutCheckDisabled) {
            return;
        }

        final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

        if (!pendingBatchRequests.isEmpty()) {
            final Set<ResourceProfile> allResourceProfiles = getResourceProfilesFromAllSlots();

            final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests =
                    pendingBatchRequests.stream()
                            .collect(
                                    Collectors.partitioningBy(
                                            canBeFulfilledWithAnySlot(allResourceProfiles)));

            final List<PendingRequest> fulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(true);
            final List<PendingRequest> unfulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(false);

            final long currentTimestamp = getRelativeTimeMillis();

            for (PendingRequest fulfillableRequest : fulfillableRequests) {
                fulfillableRequest.markFulfillable();
            }

            for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
                unfulfillableRequest.markUnfulfillable(currentTimestamp);

                if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMillis()
                        <= currentTimestamp) {
                    timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
                }
            }
        }

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkBatchSlotTimeout,
                    batchSlotTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private Set<ResourceProfile> getResourceProfilesFromAllSlots() {
        return Stream.concat(
                        getFreeSlotTracker().getFreeSlotsInformation().stream(),
                        getAllocatedSlotsInformation().stream())
                .map(SlotInfo::getResourceProfile)
                .collect(Collectors.toSet());
    }

    private Collection<PendingRequest> getPendingBatchRequests() {
        return pendingRequests.values().stream()
                .filter(PendingRequest::isBatchRequest)
                .collect(Collectors.toList());
    }

    private static Predicate<PendingRequest> canBeFulfilledWithAnySlot(
            Set<ResourceProfile> allocatedResourceProfiles) {
        return pendingRequest -> {
            for (ResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
                if (allocatedResourceProfile.isMatching(pendingRequest.getResourceProfile())) {
                    return true;
                }
            }

            return false;
        };
    }

    @VisibleForTesting
    public int getNumPendingRequests() {
        return pendingRequests.size();
    }

    @VisibleForTesting
    void increaseResourceRequirementsBy(ResourceCounter increment) {
        getDeclarativeSlotPool().increaseResourceRequirementsBy(increment);
    }

    @VisibleForTesting
    boolean isBatchSlotRequestTimeoutCheckEnabled() {
        return !isBatchSlotRequestTimeoutCheckDisabled;
    }
}
