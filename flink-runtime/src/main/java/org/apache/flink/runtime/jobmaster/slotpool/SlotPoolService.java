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
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;
//主要由 JobMaster 使用。任务槽（slot）是任务执行的基础资源，每个任务槽对应一个 TaskManager 上的执行资源
/** Service used by the {@link JobMaster} to manage a slot pool. */
public interface SlotPoolService extends AutoCloseable {

    /**
     * Tries to cast this slot pool service into the given clazz.
     * 尝试将 SlotPoolService 实例转换为指定类型
     * @param clazz to cast the slot pool service into
     * @param <T> type of clazz
     * @return {@link Optional#of} the target type if it can be cast; otherwise {@link
     *     Optional#empty()}
     */
    default <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Start the encapsulated slot pool implementation.
     * 启动封装的槽池实现。JobMaster 会调用此方法来初始化槽池服务，并将其与指定的 JobMasterId 和地址关联
     * @param jobMasterId jobMasterId to start the service with
     * @param address address of the owner
     * @throws Exception if the service cannot be started
     */
    void start(JobMasterId jobMasterId, String address) throws Exception;

    /** Close the slot pool service. */
    void close();

    /**
     * Offers multiple slots to the {@link SlotPoolService}. The slot offerings can be individually
     * accepted or rejected by returning the collection of accepted slot offers.
     *
     * @param taskManagerLocation from which the slot offers originate
     * @param taskManagerGateway to talk to the slot offerer
     * @param offers slot offers which are offered to the {@link SlotPoolService}
     * @return A collection of accepted slot offers. The remaining slot offers are implicitly
     *     rejected.
     */
    Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers);

    /**
     * Fails the allocation with the given allocationId.
     *
     * @param taskManagerId taskManagerId is non-null if the signal comes from a TaskManager; if the
     *     signal comes from the ResourceManager, then it is null
     * @param allocationId allocationId identifies which allocation to fail
     * @param cause cause why the allocation failed
     * @return Optional task executor if it has no more slots registered
     */
    Optional<ResourceID> failAllocation(
            @Nullable ResourceID taskManagerId, AllocationID allocationId, Exception cause);

    /**
     * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPoolService}.
     * 在 SlotPoolService 中注册一个 TaskManager，使其槽可以被管理
     * @param taskManagerId identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    boolean registerTaskManager(ResourceID taskManagerId);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPoolService}.
     * 释放一个已注册的 TaskManager，即从槽池中移除该 TaskManager
     * @param taskManagerId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    boolean releaseTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Releases all free slots belonging to the owning TaskExecutor if it has been registered.
     * 释放所有属于指定 TaskManager 的空闲槽。当 TaskManager 被注销或不再需要时，可以调用该方法
     * @param taskManagerId identifying the TaskExecutor
     * @param cause cause for failing the slots
     */
    void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     * 将槽池与 ResourceManager 连接。连接后，槽池可以向 ResourceManager 请求更多的资源（槽）
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    void disconnectResourceManager();

    /**
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements.
     *
     * @param acquiredResources the resources that have been acquired
     */
    default void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {}
}
