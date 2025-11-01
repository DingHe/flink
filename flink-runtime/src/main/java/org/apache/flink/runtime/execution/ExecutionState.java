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

package org.apache.flink.runtime.execution;

/**
 * An enumeration of all states that a task can be in during its execution. Tasks usually start in
 * the state {@code CREATED} and switch states according to this diagram:
 *
 * <pre>{@code
 *  CREATED  -> SCHEDULED -> DEPLOYING -> INITIALIZING -> RUNNING -> FINISHED
 *     |            |            |          |              |
 *     |            |            |    +-----+--------------+
 *     |            |            V    V
 *     |            |         CANCELLING -----+----> CANCELED
 *     |            |                         |
 *     |            +-------------------------+
 *     |
 *     |                                   ... -> FAILED
 *     V
 * RECONCILING  -> INITIALIZING | RUNNING | FINISHED | CANCELED | FAILED
 *
 * }</pre>
 *
 * <p>It is possible to enter the {@code RECONCILING} state from {@code CREATED} state if job
 * manager fail over, and the {@code RECONCILING} state can switch into any existing task state.
 *
 * <p>It is possible to enter the {@code FAILED} state from any other state.
 *
 * <p>The states {@code FINISHED}, {@code CANCELED}, and {@code FAILED} are considered terminal
 * states.
 */
// 定义了 Flink 作业中**单个任务（Task）**在其生命周期中所能经历的所有状态。这些状态对于 Flink 的调度、容错和监控机制至关重要。
public enum ExecutionState {
    CREATED, // 已创建。 任务已被逻辑上定义，但尚未提交给任何 TaskManager 进行调度。

    SCHEDULED, // 已调度。 任务已被 JobManager 选中并分配给一个 TaskExecutor，等待 TaskExecutor 准备执行环境。

    DEPLOYING, // 部署中。 TaskExecutor 正在下载任务所需的代码和配置，并准备执行环境。

    RUNNING, // 运行中。 任务正在执行其主要逻辑，处理数据。

    /**
     * This state marks "successfully completed". It can only be reached when a program reaches the
     * "end of its input". The "end of input" can be reached when consuming a bounded input (fix set
     * of files, bounded query, etc) or when stopping a program (not cancelling!) which make the
     * input look like it reached its end at a specific point.
     */
    FINISHED, // 已完成。 任务成功执行完毕，达到其输入的“终点”。对于有界流或批处理是正常结束。

    CANCELING, // 取消中。 任务收到了取消请求，正在优雅地停止其执行并清理资源。

    CANCELED, // 已取消。 任务因外部取消请求而停止并清理完毕。

    FAILED, // 失败。 任务因内部错误（如异常）而停止执行。

    RECONCILING, // 协调中/恢复中。 JobManager 经历了故障转移（Failover）并正在重建任务状态，它将从这个状态转换到已知的最新状态（如 INITIALIZING、RUNNING、FINISHED 等）。

    /** Restoring last possible valid state of the task if it has it. */
    INITIALIZING; // 初始化中。 任务代码开始执行，通常涉及恢复状态（如果适用，如从 Checkpoint 恢复上次的有效状态）。

    public boolean isTerminal() {
        return this == FINISHED || this == CANCELED || this == FAILED;
    }
}
