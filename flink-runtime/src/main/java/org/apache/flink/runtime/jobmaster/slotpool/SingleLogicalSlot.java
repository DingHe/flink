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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** Implementation of the {@link LogicalSlot}. */
// SingleLogicalSlot 是对 LogicalSlot 接口的实现，代表一个逻辑执行资源槽。
// Logical Slot (逻辑槽): 它是 JobMaster 视角下，分配给单个任务执行实例（例如一个 Execution）的抽象资源单元。它与物理资源槽 (PhysicalSlot) 挂钩。
// SingleLogicalSlot 负责将任务与实际的物理资源关联起来，并管理其生命周期，尤其是释放逻辑。
public class SingleLogicalSlot implements LogicalSlot, PhysicalSlot.Payload {
    // Payload 原子更新器。
    // 用于安全地（线程安全）更新 payload 字段。
    private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, Payload> PAYLOAD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    SingleLogicalSlot.class, Payload.class, "payload");
    // 状态原子更新器。
    // 用于安全地（线程安全）更新 state 字段，确保状态转换的正确性。
    private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SingleLogicalSlot.class, State.class, "state");
    // 标识最初请求此资源槽的唯一请求。
    private final SlotRequestId slotRequestId;
    // 槽位上下文。
    // 包含关于底层物理槽位的基本信息，例如 AllocationID、TaskManagerLocation 和 TaskManagerGateway。
    private final SlotContext slotContext;

    // locality of this slot wrt the requested preferred locations
    // 数据本地性信息。
    // 描述该槽位与任务数据或先前执行位置的本地性级别（例如，LOCAL, HOST_LOCAL, ANY）。
    private final Locality locality;

    // owner of this slot to which it is returned upon release
    // 槽位拥有者。
    private final SlotOwner slotOwner;
    // 当逻辑槽位的释放过程（包括清理载荷和返回给拥有者）完成后，这个 Future 将被完成 (complete(null))。
    private final CompletableFuture<Void> releaseFuture;
    // 当前状态。
    // 槽位的当前生命周期状态，可以是 ALIVE（存活）、RELEASING（正在释放）或 RELEASED（已释放）。
    private volatile State state;

    // LogicalSlot.Payload of this slot
    // 实际部署到此槽位上的任务（或更准确地说，表示任务生命周期的对象）。
    private volatile Payload payload;

    /** Whether this logical slot will be occupied indefinitely. */
    // 表示此槽位是否将被任务永久占用（通常用于流式作业）。
    // 如果为 false，则槽位在任务完成时会被自动释放。
    private boolean willBeOccupiedIndefinitely;

    @VisibleForTesting
    public SingleLogicalSlot(
            SlotRequestId slotRequestId,
            SlotContext slotContext,
            Locality locality,
            SlotOwner slotOwner) {

        this(slotRequestId, slotContext, locality, slotOwner, true);
    }

    public SingleLogicalSlot(
            SlotRequestId slotRequestId,
            SlotContext slotContext,
            Locality locality,
            SlotOwner slotOwner,
            boolean willBeOccupiedIndefinitely) {
        this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
        this.slotContext = Preconditions.checkNotNull(slotContext);
        this.locality = Preconditions.checkNotNull(locality);
        this.slotOwner = Preconditions.checkNotNull(slotOwner);
        this.willBeOccupiedIndefinitely = willBeOccupiedIndefinitely;
        this.releaseFuture = new CompletableFuture<>();

        this.state = State.ALIVE;
        this.payload = null;
    }

    @Override
    public TaskManagerLocation getTaskManagerLocation() {
        return slotContext.getTaskManagerLocation();
    }

    @Override
    public TaskManagerGateway getTaskManagerGateway() {
        return slotContext.getTaskManagerGateway();
    }

    @Override
    public Locality getLocality() {
        return locality;
    }

    @Override
    public boolean isAlive() {
        return state == State.ALIVE;
    }

    @Override
    public boolean tryAssignPayload(Payload payload) {
        return PAYLOAD_UPDATER.compareAndSet(this, null, payload);
    }

    @Nullable
    @Override
    public Payload getPayload() {
        return payload;
    }

    @Override
    public CompletableFuture<?> releaseSlot(@Nullable Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
            signalPayloadRelease(cause);
            returnSlotToOwner(payload.getTerminalStateFuture());
        }

        return releaseFuture;
    }

    @Override
    public AllocationID getAllocationId() {
        return slotContext.getAllocationId();
    }

    @Override
    public SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    public static SingleLogicalSlot allocateFromPhysicalSlot(
            final SlotRequestId slotRequestId,
            final PhysicalSlot physicalSlot,
            final Locality locality,
            final SlotOwner slotOwner,
            final boolean slotWillBeOccupiedIndefinitely) {

        final SingleLogicalSlot singleTaskSlot =
                new SingleLogicalSlot(
                        slotRequestId,
                        physicalSlot,
                        locality,
                        slotOwner,
                        slotWillBeOccupiedIndefinitely);

        if (physicalSlot.tryAssignPayload(singleTaskSlot)) {
            return singleTaskSlot;
        } else {
            throw new IllegalStateException(
                    "BUG: Unexpected physical slot payload assignment failure!");
        }
    }

    // -------------------------------------------------------------------------
    // AllocatedSlot.Payload implementation
    // -------------------------------------------------------------------------

    /**
     * A release of the payload by the {@link AllocatedSlot} triggers a release of the payload of
     * the logical slot.
     *
     * @param cause of the payload release
     */
    @Override
    public void release(Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
            signalPayloadRelease(cause);
        }
        markReleased();
        releaseFuture.complete(null);
    }

    @Override
    public boolean willOccupySlotIndefinitely() {
        return willBeOccupiedIndefinitely;
    }

    private void signalPayloadRelease(Throwable cause) {
        tryAssignPayload(TERMINATED_PAYLOAD);
        payload.fail(cause);
    }

    private void returnSlotToOwner(CompletableFuture<?> terminalStateFuture) {
        FutureUtils.assertNoException(
                terminalStateFuture.thenRun(
                        () -> {
                            if (state == State.RELEASING) {
                                slotOwner.returnLogicalSlot(this);
                            }

                            markReleased();

                            releaseFuture.complete(null);
                        }));
    }

    private void markReleased() {
        state = State.RELEASED;
    }

    // -------------------------------------------------------------------------
    // Internal classes
    // -------------------------------------------------------------------------

    enum State {
        ALIVE,
        RELEASING,
        RELEASED
    }
}
