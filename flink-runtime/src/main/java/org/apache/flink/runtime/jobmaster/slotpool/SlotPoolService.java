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
// SlotPoolService 接口是 JobMaster 用来管理和操作底层资源槽位池的高级服务接口。
// 作用是将底层的具体 SlotPool 实现（如 DeclarativeSlotPoolBridge）抽象出来，
// 为 JobMaster 提供一个统一、简洁的资源管理视图，同时处理与 TaskManager 和 ResourceManager 之间的资源交互逻辑。
// 生命周期管理： 启动和关闭槽位池。
// 外部通信： 管理与 TaskManager（接收槽位提供）和 ResourceManager（连接/断开）的资源连接。
// 资源状态报告： 报告 TaskManager 上的槽位分配情况。

/** Service used by the {@link JobMaster} to manage a slot pool. */
public interface SlotPoolService extends AutoCloseable {

    /**
     * Tries to cast this slot pool service into the given clazz.
     * @param clazz to cast the slot pool service into
     * @param <T> type of clazz
     * @return {@link Optional#of} the target type if it can be cast; otherwise {@link
     *     Optional#empty()}
     */
    // 允许用户尝试将当前的 SlotPoolService 实例转换为特定的子类型 (T)。
    // 这通常用于访问底层 SlotPool 实现中未在接口中公开的特定方法（例如，用于测试或特定的调度策略）
    default <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Start the encapsulated slot pool implementation.
     * @param jobMasterId jobMasterId to start the service with
     * @param address address of the owner
     * @throws Exception if the service cannot be started
     */
    // 启动槽位池服务。
    // JobMaster 在启动时调用此方法，初始化并启动底层的 Slot Pool 实现。
    // 需要提供 JobMaster 的 ID 和地址，以便 TaskManager 和 ResourceManager 知道与谁通信。
    void start(JobMasterId jobMasterId, String address) throws Exception;

    /** Close the slot pool service. */
    // 关闭 Slot Pool Service，释放所有持有的资源，例如取消所有未完成的槽位请求。
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
    // 处理槽位提供。
    // TaskManager 通过此方法向 SlotPoolService 提供其可用的物理槽位。
    // SlotPoolService 根据当前作业需求决定接受（返回接受的 SlotOffer 列表）或拒绝这些槽位。
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
     * @param taskManagerId identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    // 注册 TaskManager。
    // 将一个 TaskManager 注册到槽位池服务中，使其资源可供 JobMaster 使用和管理。
    boolean registerTaskManager(ResourceID taskManagerId);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPoolService}.
     * @param taskManagerId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    // 释放 TaskManager。
    // 从槽位池服务中注销一个 TaskManager，通常是因为它已断开连接或故障。该 TaskManager 上的所有槽位都会被释放。
    boolean releaseTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Releases all free slots belonging to the owning TaskExecutor if it has been registered.
     * 释放所有属于指定 TaskManager 的空闲槽。当 TaskManager 被注销或不再需要时，可以调用该方法
     * @param taskManagerId identifying the TaskExecutor
     * @param cause cause for failing the slots
     */
    // 释放 TaskManager 上的空闲槽位
    // 释放特定 TaskManager 上当前处于空闲状态的所有槽位。
    void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    // 连接到 ResourceManager。
    // 建立 JobMaster 与 ResourceManager 之间的通信。
    // 这是 SlotPoolService 能够发起资源请求的前提。
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    // 断开与 ResourceManager 的连接。
    // 终止与当前 ResourceManager 的连接。
    // SlotPoolService 将停止向其请求新的槽位，并取消所有待处理的请求。
    void disconnectResourceManager();

    /**
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    // 创建已分配槽位报告。
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements.
     *
     * @param acquiredResources the resources that have been acquired
     */
    // 通知资源不足。
    // 通知 JobMaster 当前可用的资源不足以满足所有资源需求。调度器或 JobMaster 可以利用此信息来调整其调度策略或向用户报告资源短缺。
    default void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {}
}
