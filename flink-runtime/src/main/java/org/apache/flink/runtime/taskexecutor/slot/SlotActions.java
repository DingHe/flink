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

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import java.util.UUID;

/** Interface to trigger slot actions from within the {@link TaskSlotTable}. */
// 定义了任务槽（Task Slot）在特定生命周期事件发生时需要触发的行为
// SlotActions 的核心作用是 “解耦” 和 “反向通知”。
// 在 TaskManager 内部，TaskSlotTable 负责管理所有 Slot 的状态（比如这个 Slot 是分配给谁了，现在是忙碌还是空闲）。
// 然而，TaskSlotTable 只是一个状态管理者，它不具备与外部组件（如 JobManager 或 ResourceManager）通信的能力，也不负责处理高级的业务逻辑。
// 当 TaskSlotTable 发现某个 Slot 需要被释放（比如任务结束了）或者某个 Slot 申请超时了，它会通过这个 SlotActions 接口发出通知。
// 具体的实现类（通常是 TaskExecutor）会接收到这些通知，并执行真正的清理逻辑、RPC 通信或资源汇报。
public interface SlotActions {

    /**
     * Free the task slot with the given allocation id.
     *
     * @param allocationId to identify the slot to be freed
     */
    // 释放指定的任务槽。
    // allocationId: 这是一个全局唯一的标识符，用于定位哪一个 Slot 需要被释放。
    // 触发时机：
    // 当 JobManager 主动通知 TaskManager 任务已经结束，不需要这个 Slot 时。
    // 当 TaskManager 发现某个作业已经失败，需要回收其占用的所有资源时。
    void freeSlot(AllocationID allocationId);

    /**
     * Timeout the task slot for the given allocation id. The timeout is identified by the given
     * ticket to filter invalid timeouts out.
     *
     * @param allocationId identifying the task slot to be timed out
     * @param ticket allowing to filter invalid timeouts out
     */
    // 处理任务槽分配请求的超时。
    // allocationId: 指向申请超时的那个 Slot 分配请求。
    // ticket: 一个 UUID 类型的票据（令牌）。
    // 触发时机：
    // 当 ResourceManager 告诉 TaskManager 预留一个 Slot 给某个 Job，但该 Job 在规定时间内（配置的超时时间）没有真正发送任务部署指令来占用它。
    void timeoutSlot(AllocationID allocationId, UUID ticket);
}
