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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.Collection;
import java.util.List;

/**
 * The plan of one checkpoint, indicating which tasks to trigger, waiting for acknowledge or commit
 * for one specific checkpoint.
 */
// 定义了 Flink 单个检查点（Checkpoint）的执行计划和范围。
// 在 Flink 运行时，特别是在支持部分完成 (Partial Finishing) 功能的场景下，一个 Job 的部分任务可能已经完成并退出。
// CheckpointPlan 的作用就是确定在特定检查点触发时：
// 哪些任务需要接收触发消息（发送障碍物，Barrier）。
// 哪些任务必须返回确认（Acknowledge）才能使检查点成功。
// 哪些任务已经完成，不需要参与检查点过程。
// 提供了检查点执行所需的任务集合的精确视图，确保检查点只针对当前正在运行或需要处理的任务。
public interface CheckpointPlan extends FinishedTaskStateProvider {

    /** Returns the tasks who need to be sent a message when a checkpoint is started. */
    // 需要触发的任务。
    // 返回需要接收**检查点触发消息（Barrier）**的 Execution 实例列表。
    List<Execution> getTasksToTrigger();

    /** Returns tasks who need to acknowledge a checkpoint before it succeeds. */
    // 需要等待确认的任务。
    // 返回必须向 JobManager 发送检查点确认 (Acknowledge) 消息的 Execution 实例列表。
    // 只有当这些任务全部确认后，检查点才可能被视为成功。
    List<Execution> getTasksToWaitFor();

    /**
     * Returns tasks that are still running when taking the checkpoint, these need to be sent a
     * message when the checkpoint is confirmed.
     */
    // 需要提交的任务。
    // 返回在检查点确认成功后，需要接收提交/完成消息 (Commit Message) 的 ExecutionVertex 实例列表。
    // 通常与 getTasksToWaitFor() 中的任务相似，但返回类型是 ExecutionVertex。
    List<ExecutionVertex> getTasksToCommitTo();

    /** Returns tasks that have already been finished when taking the checkpoint. */
    // 已完成的任务。
    // 返回在触发检查点时，Job 中已经完成并退出的 Execution 实例列表。
    // 这些任务不再参与检查点 I/O，但其状态需要在检查点中被正确地标记为 Finished。
    List<Execution> getFinishedTasks();

    /** Returns the job vertices whose tasks are all finished when taking the checkpoint. */
    // 完全完成的 Job 顶点。
    // 返回 Job 中所有子任务都已完成的 ExecutionJobVertex（即算子组）的集合。主要用于测试和内部验证。
    @VisibleForTesting
    Collection<ExecutionJobVertex> getFullyFinishedJobVertex();

    /** Returns whether we support checkpoints after some tasks finished. */
    // 存在已完成任务的可能性检查。
    boolean mayHaveFinishedTasks();
}
