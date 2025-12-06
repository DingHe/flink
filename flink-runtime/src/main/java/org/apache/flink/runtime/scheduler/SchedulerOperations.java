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

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import java.util.List;

/** Component which is used by {@link SchedulingStrategy} to commit scheduling decisions. */
// SchedulerOperations 接口充当了 Flink 调度策略（SchedulingStrategy）与底层的 JobMaster 状态和资源管理之间的桥梁或执行者。
// 执行调度决策：它允许 SchedulingStrategy 组件（负责制定调度计划和顺序的"大脑"）将它所做的决策（即：“哪些任务应该被部署”）转化为实际的操作
// 抽象执行细节：它隐藏了实际分配资源、与 SlotPool 交互、以及最终将任务部署到 TaskManager 的复杂实现细节。SchedulingStrategy 只需调用 allocateSlotsAndDeploy()，而无需关心这些过程如何发生。
public interface SchedulerOperations {

    /**
     * Allocate slots and deploy the vertex when slots are returned. Vertices will be deployed only
     * after all of them have been assigned slots. The given order will be respected, i.e. tasks
     * with smaller indices will be deployed earlier. Only vertices in CREATED state will be
     * accepted. Errors will happen if scheduling Non-CREATED vertices.
     *
     * @param verticesToDeploy The execution vertices to deploy
     */
    // 分配槽位并部署执行顶点
    // verticesToDeploy ： 一个 ExecutionVertexID 列表，代表了调度策略决定要部署的**任务（执行顶点）**的唯一标识符。
    void allocateSlotsAndDeploy(List<ExecutionVertexID> verticesToDeploy);
}
