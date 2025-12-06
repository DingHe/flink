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

import org.apache.flink.runtime.jobmaster.SlotRequestId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Interface for components which have to perform actions on allocated slots. */
// AllocatedSlotActions（已分配槽位操作）接口定义了对已经分配给 JobMaster 并处于活跃状态的资源槽位可以执行的行为。
// 释放（或归还）特定的已分配资源槽位。
public interface AllocatedSlotActions {

    /**
     * Releases the slot with the given {@link SlotRequestId}. Additionally, one can provide a cause
     * for the slot release.
     *
     * @param slotRequestId identifying the slot to release
     * @param cause of the slot release, null if none
     */
    // 释放由特定请求 ID 标识的已分配资源槽位。
    // slotRequestId (@Nonnull SlotRequestId): 槽位请求 ID。
    // 这个 ID 唯一标识了当初 JobMaster 请求并最终分配到的那个资源槽位。释放操作必须准确地通过这个 ID 来定位目标槽位。
    void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause);
}
