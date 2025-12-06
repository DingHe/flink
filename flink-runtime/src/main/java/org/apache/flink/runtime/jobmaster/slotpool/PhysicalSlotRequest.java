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

import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

/** Represents a request for a physical slot. */
// PhysicalSlotRequest 类是一个数据容器，它封装了 JobMaster 向 SlotPool 或 ResourceManager 申请一个物理执行槽位所需的全部信息。
// 明确请求目的：它将请求的唯一标识符 (SlotRequestId)、槽位的具体需求 (SlotProfile) 以及槽位的使用期限 (slotWillBeOccupiedIndefinitely) 捆绑在一起。
public class PhysicalSlotRequest {
    // 槽位请求的唯一标识符。
    // 用于在异步请求过程中跟踪和识别特定的槽位请求，也是后续取消和结果匹配的依据。
    private final SlotRequestId slotRequestId;
    // 槽位需求概要。 这是一个重要的复杂对象，包含了任务对槽位的全部要求，包括资源需求、位置偏好、以及先前分配 ID 等。
    private final SlotProfile slotProfile;
    // 槽位是否将被无限期占用。 如果为 true，通常表示该请求是针对长时间运行的任务（如流处理任务），
    // SlotPool 在分配该槽位时会知道该槽位不会在短期内释放，可能影响其资源管理策略或超时逻辑。
    private final boolean slotWillBeOccupiedIndefinitely;

    public PhysicalSlotRequest(
            final SlotRequestId slotRequestId,
            final SlotProfile slotProfile,
            final boolean slotWillBeOccupiedIndefinitely) {

        this.slotRequestId = slotRequestId;
        this.slotProfile = slotProfile;
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
    }

    public SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    public SlotProfile getSlotProfile() {
        return slotProfile;
    }

    public boolean willSlotBeOccupiedIndefinitely() {
        return slotWillBeOccupiedIndefinitely;
    }

    /** Result of a {@link PhysicalSlotRequest}. */
    // 用于表示成功分配物理槽位的结果。
    public static class Result {
        // 对应的槽位请求 ID。
        // 用于将异步返回的结果与最初的请求进行匹配。
        private final SlotRequestId slotRequestId;
        // 实际分配到的物理槽位对象
        private final PhysicalSlot physicalSlot;

        public Result(final SlotRequestId slotRequestId, final PhysicalSlot physicalSlot) {
            this.slotRequestId = slotRequestId;
            this.physicalSlot = physicalSlot;
        }

        public SlotRequestId getSlotRequestId() {
            return slotRequestId;
        }

        public PhysicalSlot getPhysicalSlot() {
            return physicalSlot;
        }
    }
}
