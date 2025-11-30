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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FlinkException;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** Handler for the {@link OperatorCoordinator OperatorCoordinators}. */
// OperatorCoordinatorHandler（操作符协调器处理器）负责管理 Flink 作业中所有 Operator Coordinator 的生命周期和通信。
// Operator Coordinator 是一种运行在 JobManager 上的组件，用于解决 Source Operator 和其他复杂 Operator 在处理状态、Checkpoints 和动态负载变化时遇到的挑战。
// 例如，它管理 Source Split 的分配，以确保高效且一致地处理数据。
public interface OperatorCoordinatorHandler {

    /**
     * Initialize operator coordinators.
     *
     * @param mainThreadExecutor Executor for submitting work to the main thread.
     */
    // 初始化所有协调器。
    // 在作业启动初期调用，用于设置所有协调器实例，并向它们提供 Flink 主线程执行器 (mainThreadExecutor)，
    // 确保协调器可以在主线程中执行其逻辑，避免并发问题。
    void initializeOperatorCoordinators(ComponentMainThreadExecutor mainThreadExecutor);

    /** Start all operator coordinators. */
    // 启动所有协调器。
    // 在初始化完成后调用，用于触发所有协调器的启动逻辑（例如，Source Coordinator 可能在此阶段开始读取元数据）。
    void startAllOperatorCoordinators();

    /** Dispose all operator coordinators. */
    // 销毁所有协调器。
    // 在作业结束（成功、失败或取消）时调用，用于优雅地关闭和清理所有协调器持有的资源（如线程、连接或临时文件）。
    void disposeAllOperatorCoordinators();

    /**
     * Delivers an OperatorEvent to a {@link OperatorCoordinator}.
     *
     * @param taskExecutionId Execution attempt id of the originating task.
     * @param operatorId OperatorId of the target OperatorCoordinator.
     * @param event Event to deliver to the OperatorCoordinator.
     * @throws FlinkException If no coordinator is registered for operator.
     */
    // 将一个 Operator Event 从 Task 传递给其对应的 Coordinator。
    // taskExecutionId： 发送事件的任务实例 ID。
    // operatorId： 目标 Operator Coordinator 的 ID。
    // event： 任务发给 Coordinator 的消息（例如，Source Task 报告它需要更多的 Split）。
    void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecutionId, OperatorID operatorId, OperatorEvent event)
            throws FlinkException;

    /**
     * Deliver coordination request from the client to the coordinator.
     *
     * @param operator Id of target operator.
     * @param request request for the operator.
     * @return Future with the response.
     * @throws FlinkException If the coordinator doesn't exist or if it can not handle the request.
     */
    // 将一个 Coordination Request 从客户端（通常是 JobMaster 或外部 API）传递给 Coordinator。
    // operator： 目标 Operator Coordinator 的 ID。
    // request： 客户端发给 Coordinator 的请求。
    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException;

    /**
     * Register and start new operator coordinators.
     *
     * @param coordinators the operator coordinator to be registered.
     * @param mainThreadExecutor Executor for submitting work to the main thread.
     */
    // 注册并启动新的协调器。
    // 用于在运行时（例如，当 Job Vertex 被动态添加或重新配置时）添加新的协调器实例。
    // 它需要提供新的协调器集合、主线程执行器和操作符的并行度。
    void registerAndStartNewCoordinators(
            Collection<OperatorCoordinatorHolder> coordinators,
            ComponentMainThreadExecutor mainThreadExecutor,
            final int parallelism);
}
