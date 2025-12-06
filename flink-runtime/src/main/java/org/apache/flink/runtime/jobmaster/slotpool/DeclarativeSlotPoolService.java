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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SlotPoolService} implementation for the {@link DeclarativeSlotPool}. */
// 专为声明式槽位池 (DeclarativeSlotPool) 设计。
// 它扮演着 JobMaster 与外部实体（如 ResourceManager 和 TaskManager）进行交互的适配层和状态机。
// 生命周期管理： 负责 DeclarativeSlotPool 的启动和关闭流程，并维护服务自身的运行状态（CREATED, STARTED, CLOSED）。
// 对接 ResourceManager： 通过 DeclareResourceRequirementServiceConnectionManager 将 JobMaster 产生的资源需求（由 DeclarativeSlotPool 产生）发送给 ResourceManager。
// 对接 TaskManager： 接收 TaskManager 的槽位提供 (offerSlots)，并管理 TaskManager 的注册和释放。
// 状态同步和报告： 处理槽位分配失败，并在 TaskManager 宕机时释放所有相关槽位。同时，它能生成当前 TaskManager 上槽位的报告。
// 是 JobMaster 中实现声明式资源分配逻辑的主要服务门面。
public class DeclarativeSlotPoolService implements SlotPoolService {
    // 当前作业 ID。
    // 标识该服务所属的 Flink 作业。
    private final JobID jobId;
    // RPC 超时时间。
    // 用于与 TaskManager 或 ResourceManager 进行远程通信时的超时设置。
    private final Duration rpcTimeout;
    // 核心槽位池逻辑。
    // 实际负责维护资源需求和槽位状态的核心组件。该服务类围绕它进行调度和外部通信。
    private final DeclarativeSlotPool declarativeSlotPool;

    private final Clock clock;
    // 已注册的 TaskManager 集合。
    private final Set<ResourceID> registeredTaskManagers;

    protected final Logger log = LoggerFactory.getLogger(getClass());
    // 负责管理与声明资源需求服务（即 ResourceManager）的连接，并在连接建立后将资源需求转发给它。
    private DeclareResourceRequirementServiceConnectionManager
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;
    // 当前 JobMaster 的唯一标识符。
    @Nullable private JobMasterId jobMasterId;
    // 当前 JobMaster 的网络通信地址，
    // 用于发送给 ResourceManager。
    @Nullable private String jobManagerAddress;
    // 维护服务的生命周期状态（CREATED, STARTED, CLOSED）。
    private State state = State.CREATED;
    protected final ComponentMainThreadExecutor componentMainThreadExecutor;

    public DeclarativeSlotPoolService(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Duration idleSlotTimeout,
            Duration rpcTimeout,
            Duration slotRequestMaxInterval,
            @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) {
        this.jobId = jobId;
        this.clock = clock;
        this.rpcTimeout = rpcTimeout;
        this.registeredTaskManagers = new HashSet<>();
        this.componentMainThreadExecutor = componentMainThreadExecutor;

        this.declarativeSlotPool =
                declarativeSlotPoolFactory.create(
                        jobId,
                        this::declareResourceRequirements,
                        idleSlotTimeout,
                        rpcTimeout,
                        slotRequestMaxInterval,
                        componentMainThreadExecutor);
    }

    protected DeclarativeSlotPool getDeclarativeSlotPool() {
        return declarativeSlotPool;
    }

    protected long getRelativeTimeMillis() {
        return clock.relativeTimeMillis();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(declarativeSlotPool.getClass())) {
            return Optional.of(clazz.cast(declarativeSlotPool));
        }

        return Optional.empty();
    }

    @Override
    public final void start(JobMasterId jobMasterId, String address) throws Exception {
        Preconditions.checkState(
                state == State.CREATED, "The DeclarativeSlotPoolService can only be started once.");

        this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
        this.jobManagerAddress = Preconditions.checkNotNull(address);

        this.resourceRequirementServiceConnectionManager =
                DefaultDeclareResourceRequirementServiceConnectionManager.create(
                        componentMainThreadExecutor);

        onStart();

        state = State.STARTED;
    }

    /**
     * This method is called when the slot pool service is started. It can be overridden by
     * subclasses.
     */
    protected void onStart() {}

    protected void assertHasBeenStarted() {
        Preconditions.checkState(
                state == State.STARTED, "The DeclarativeSlotPoolService has to be started.");
    }

    @Override
    public final void close() {
        if (state != State.CLOSED) {

            onClose();

            resourceRequirementServiceConnectionManager.close();
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;

            releaseAllTaskManagers(
                    new FlinkException("The DeclarativeSlotPoolService is being closed."));

            state = State.CLOSED;
        }
    }

    /**
     * This method is called when the slot pool service is closed. It can be overridden by
     * subclasses.
     */
    protected void onClose() {}

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

        return declarativeSlotPool.offerSlots(
                offers, taskManagerLocation, taskManagerGateway, clock.relativeTimeMillis());
    }

    boolean isTaskManagerRegistered(ResourceID taskManagerId) {
        return registeredTaskManagers.contains(taskManagerId);
    }

    @Override
    public Optional<ResourceID> failAllocation(
            @Nullable ResourceID taskManagerId, AllocationID allocationId, Exception cause) {
        assertHasBeenStarted();
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(
                taskManagerId,
                "This slot pool only supports failAllocation calls coming from the TaskExecutor.");

        final ResourceCounter previouslyFulfilledRequirements =
                declarativeSlotPool.releaseSlot(allocationId, cause);

        onFailAllocation(previouslyFulfilledRequirements);

        if (declarativeSlotPool.containsSlots(taskManagerId)) {
            return Optional.empty();
        } else {
            return Optional.of(taskManagerId);
        }
    }

    /**
     * This method is called when an allocation fails. It can be overridden by subclasses.
     *
     * @param previouslyFulfilledRequirements previouslyFulfilledRequirements by the failed
     *     allocation
     */
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {}

    @Override
    public boolean registerTaskManager(ResourceID taskManagerId) {
        assertHasBeenStarted();

        log.debug("Register new TaskExecutor {}.", taskManagerId);
        return registeredTaskManagers.add(taskManagerId);
    }

    @Override
    public boolean releaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        if (registeredTaskManagers.remove(taskManagerId)) {
            internalReleaseTaskManager(taskManagerId, cause);
            return true;
        }

        return false;
    }

    @Override
    public void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();
        if (isTaskManagerRegistered(taskManagerId)) {

            Collection<AllocationID> freeSlots =
                    declarativeSlotPool.getFreeSlotTracker().getFreeSlotsInformation().stream()
                            .filter(
                                    slotInfo ->
                                            slotInfo.getTaskManagerLocation()
                                                    .getResourceID()
                                                    .equals(taskManagerId))
                            .map(SlotInfo::getAllocationId)
                            .collect(Collectors.toSet());

            for (AllocationID allocationId : freeSlots) {
                final ResourceCounter previouslyFulfilledRequirement =
                        declarativeSlotPool.releaseSlot(allocationId, cause);
                // release free slots, previously fulfilled requirement should be empty.
                Preconditions.checkState(
                        previouslyFulfilledRequirement.equals(ResourceCounter.empty()));
            }
        }
    }

    private void releaseAllTaskManagers(Exception cause) {
        for (ResourceID registeredTaskManager : registeredTaskManagers) {
            internalReleaseTaskManager(registeredTaskManager, cause);
        }

        registeredTaskManagers.clear();
    }

    private void internalReleaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        final ResourceCounter previouslyFulfilledRequirement =
                declarativeSlotPool.releaseSlots(taskManagerId, cause);

        onReleaseTaskManager(previouslyFulfilledRequirement);
    }

    /**
     * This method is called when a TaskManager is released. It can be overridden by subclasses.
     *
     * @param previouslyFulfilledRequirement previouslyFulfilledRequirement by the released
     *     TaskManager
     */
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {}

    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.connect(
                resourceRequirements ->
                        resourceManagerGateway.declareRequiredResources(
                                jobMasterId, resourceRequirements, rpcTimeout));

        declareResourceRequirements(declarativeSlotPool.getResourceRequirements());
    }

    private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.declareResourceRequirements(
                ResourceRequirements.create(jobId, jobManagerAddress, resourceRequirements));
    }

    @Override
    public void disconnectResourceManager() {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.disconnect();
    }

    @Override
    public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
        assertHasBeenStarted();

        final Collection<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>();

        for (SlotInfo slotInfo : declarativeSlotPool.getAllSlotsInformation()) {
            if (slotInfo.getTaskManagerLocation().getResourceID().equals(taskManagerId)) {
                allocatedSlotInfos.add(
                        new AllocatedSlotInfo(
                                slotInfo.getPhysicalSlotNumber(), slotInfo.getAllocationId()));
            }
        }
        return new AllocatedSlotReport(jobId, allocatedSlotInfos);
    }

    private enum State {
        CREATED,
        STARTED,
        CLOSED,
    }

    protected String getSlotServiceStatus() {
        return String.format(
                "Registered TMs: %d, registered slots: %d free slots: %d",
                registeredTaskManagers.size(),
                declarativeSlotPool.getAllSlotsInformation().size(),
                declarativeSlotPool.getFreeSlotTracker().getAvailableSlots().size());
    }
}
