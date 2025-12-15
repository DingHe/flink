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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for {@link CheckpointPlanCalculator}. If all tasks are running, it
 * directly marks all the sources as tasks to trigger, otherwise it would try to find the running
 * tasks without running processors as tasks to trigger.
 */
// 它负责在 Flink JobMaster 端，根据作业当前的执行图（ExecutionGraph）状态，异步地计算出下一次检查点 (Checkpoint) 的完整执行计划 (CheckpointPlan)。
// 这个计划详细列出了哪些任务需要接收检查点障碍 (Barrier)、哪些任务需要返回确认 (ACK)，以及哪些任务已完成。
// 该计算器能够区分两种主要的运行状态：
// 所有任务都在运行中： 此时，只需将所有 Source 任务标记为触发点。
// 部分任务已完成： 此时，计算器会遍历拓扑结构，精确识别出那些仍在运行中但上游已无正在运行任务的 Job Vertex，将它们标记为新的检查点触发点，以确保检查点流程能够从这些“新的起点”开始。
public class DefaultCheckpointPlanCalculator implements CheckpointPlanCalculator {
    // 作业 ID。
    // 当前 Job 的唯一标识符，用于日志和异常报告。
    private final JobID jobId;
    // 计算器上下文。
    // 提供了计算计划所需的外部依赖和执行器，例如获取 JobMaster 的主执行器 (MainExecutor) 和判断是否有已完成任务 (hasFinishedTasks())。
    private final CheckpointPlanCalculatorContext context;
    // 拓扑顺序的 Job 顶点列表。
    // 存储 ExecutionGraph 中所有 Job Vertex，并按照拓扑顺序排列（从 Source 到 Sink），方便遍历图结构进行检查点计划计算。
    private final List<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();
    // 所有任务列表。
    // 存储 ExecutionGraph 中所有子任务 (ExecutionVertex) 的集合。
    private final List<ExecutionVertex> allTasks = new ArrayList<>();
    // 所有 Source 任务列表。
    // 存储 Job 中所有作为 Source 的子任务集合，它们是全运行状态下检查点的默认触发起点。
    private final List<ExecutionVertex> sourceTasks = new ArrayList<>();
    // 允许部分完成检查点标志
    private final boolean allowCheckpointsAfterTasksFinished;

    public DefaultCheckpointPlanCalculator(
            JobID jobId,
            CheckpointPlanCalculatorContext context,
            Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable,
            boolean allowCheckpointsAfterTasksFinished) {

        this.jobId = checkNotNull(jobId);
        this.context = checkNotNull(context);
        this.allowCheckpointsAfterTasksFinished = allowCheckpointsAfterTasksFinished;

        checkNotNull(jobVerticesInTopologyOrderIterable);
        jobVerticesInTopologyOrderIterable.forEach(
                jobVertex -> {
                    jobVerticesInTopologyOrder.add(jobVertex);
                    allTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));

                    if (jobVertex.getJobVertex().isInputVertex()) {
                        sourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                    }
                });
    }
    // 负责异步地计算并返回下一个检查点的完整执行计划 (CheckpointPlan)。
    // 它在专用的执行器上运行，避免阻塞 JobMaster 的主线程，并包含检查点启动前的状态验证和核心计划计算逻辑。
    @Override
    public CompletableFuture<CheckpointPlan> calculateCheckpointPlan() {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        if (context.hasFinishedTasks() && !allowCheckpointsAfterTasksFinished) {
                            throw new CheckpointException(
                                    "Some tasks of the job have already finished and checkpointing with finished tasks is not enabled.",
                                    CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                        }
                        // 检查 JobGraph 中所有任务是否都已经附加了当前的执行尝试 (Execution)。如果任何任务尚未准备好，将抛出 CheckpointException。
                        checkAllTasksInitiated();

                        CheckpointPlan result =
                                context.hasFinishedTasks()
                                        // 模式 A (部分完成)： 如果存在已完成任务，调用 calculateAfterTasksFinished()，该方法会复杂地计算新的触发点。
                                        ? calculateAfterTasksFinished()
                                        // 模式 B (全运行)： 如果所有任务都在运行，调用 calculateWithAllTasksRunning()，将所有 Source 任务作为触发点。
                                        : calculateWithAllTasksRunning();
                        // 检查计算出的所有需要等待确认的任务是否都处于 RUNNING 状态
                        checkTasksStarted(result.getTasksToWaitFor());

                        return result;
                    } catch (Throwable throwable) {
                        throw new CompletionException(throwable);
                    }
                },
                context.getMainExecutor());
    }

    /**
     * Checks if all tasks are attached with the current Execution already. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks do not have attached Execution.
     */
    // 确保 JobGraph 中所有的子任务 (ExecutionVertex) 都有一个当前活跃的执行尝试 (Execution) 实例。
    // 如果一个 ExecutionVertex 还没有对应的 Execution 实例，意味着该任务尚未被调度或仍在等待初始化，JobMaster 无法对其进行操作（例如发送检查点障碍物）。
    // 检查强制要求检查点只能在整个执行图的所有任务都已启动或至少被初始化时才能进行。
    private void checkAllTasksInitiated() throws CheckpointException {
        for (ExecutionVertex task : allTasks) {
            if (task.getCurrentExecutionAttempt() == null) {
                throw new CheckpointException(
                        String.format(
                                "task %s of job %s is not being executed at the moment. Aborting checkpoint.",
                                task.getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Checks if all tasks to trigger have already been in RUNNING state. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks to trigger have not turned into RUNNING yet.
     */
    private void checkTasksStarted(List<Execution> toTrigger) throws CheckpointException {
        for (Execution execution : toTrigger) {
            if (execution.getState() != ExecutionState.RUNNING) {
                throw new CheckpointException(
                        String.format(
                                "Checkpoint triggering task %s of job %s is not being executed at the moment. "
                                        + "Aborting checkpoint.",
                                execution.getVertex().getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Computes the checkpoint plan when all tasks are running. It would simply marks all the source
     * tasks as need to trigger and all the tasks as need to wait and commit.
     *
     * @return The plan of this checkpoint.
     */
    // 在作业处于完全运行状态时，创建一个基础的、覆盖所有任务的检查点计划 (CheckpointPlan)。
    // 在这种理想状态下，检查点流程遵循最简单的模式：
    // 触发点： 仅有 Source 任务接收检查点障碍 (Barrier)。
    // 等待点/提交点： 所有任务都必须参与并确认检查点。
    private CheckpointPlan calculateWithAllTasksRunning() {
        // 获取 Source 任务流
        // 获取当前执行尝试： 将每个 Source 任务 (ExecutionVertex) 映射到其当前的执行尝试 (Execution) 实例。实际的检查点障碍是发送给这个 Execution 实例的。
        List<Execution> executionsToTrigger =
                sourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());
        // 将 Job 中的所有任务 (allTasks) 转换为对应的 Execution 实例列表。这些是必须发送确认 (ACK) 才能使检查点成功的任务。
        List<Execution> tasksToWaitFor = createTaskToWaitFor(allTasks);

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(executionsToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(allTasks),
                Collections.emptyList(),
                Collections.emptyList(),
                allowCheckpointsAfterTasksFinished);
    }

    /**
     * Calculates the checkpoint plan after some tasks have finished. We iterate the job graph to
     * find the task that is still running, but do not has precedent running tasks.
     *
     * @return The plan of this checkpoint.
     */
    private CheckpointPlan calculateAfterTasksFinished() {
        // First collect the task running status into BitSet so that we could
        // do JobVertex level judgement for some vertices and avoid time-consuming
        // access to volatile isFinished flag of Execution.
        Map<JobVertexID, BitSet> taskRunningStatusByVertex = collectTaskRunningStatus();

        List<Execution> tasksToTrigger = new ArrayList<>();
        List<Execution> tasksToWaitFor = new ArrayList<>();
        List<ExecutionVertex> tasksToCommitTo = new ArrayList<>();
        List<Execution> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            BitSet taskRunningStatus = taskRunningStatusByVertex.get(jobVertex.getJobVertexId());

            if (taskRunningStatus.cardinality() == 0) {
                fullyFinishedJobVertex.add(jobVertex);

                for (ExecutionVertex task : jobVertex.getTaskVertices()) {
                    finishedTasks.add(task.getCurrentExecutionAttempt());
                }

                continue;
            }

            List<JobEdge> prevJobEdges = jobVertex.getJobVertex().getInputs();

            // this is an optimization: we determine at the JobVertex level if some tasks can even
            // be eligible for being in the "triggerTo" set.
            boolean someTasksMustBeTriggered =
                    someTasksMustBeTriggered(taskRunningStatusByVertex, prevJobEdges);

            for (int i = 0; i < jobVertex.getTaskVertices().length; ++i) {
                ExecutionVertex task = jobVertex.getTaskVertices()[i];
                if (taskRunningStatus.get(task.getParallelSubtaskIndex())) {
                    tasksToWaitFor.add(task.getCurrentExecutionAttempt());
                    tasksToCommitTo.add(task);

                    if (someTasksMustBeTriggered) {
                        boolean hasRunningPrecedentTasks =
                                hasRunningPrecedentTasks(
                                        task, prevJobEdges, taskRunningStatusByVertex);

                        if (!hasRunningPrecedentTasks) {
                            tasksToTrigger.add(task.getCurrentExecutionAttempt());
                        }
                    }
                } else {
                    finishedTasks.add(task.getCurrentExecutionAttempt());
                }
            }
        }

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(tasksToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(tasksToCommitTo),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex),
                allowCheckpointsAfterTasksFinished);
    }

    private boolean someTasksMustBeTriggered(
            Map<JobVertexID, BitSet> runningTasksByVertex, List<JobEdge> prevJobEdges) {

        for (JobEdge jobEdge : prevJobEdges) {
            DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
            BitSet upstreamRunningStatus =
                    runningTasksByVertex.get(jobEdge.getSource().getProducer().getID());

            if (hasActiveUpstreamVertex(distributionPattern, upstreamRunningStatus)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Every task must have active upstream tasks if
     *
     * <ol>
     *   <li>ALL_TO_ALL connection and some predecessors are still running.
     *   <li>POINTWISE connection and all predecessors are still running.
     * </ol>
     *
     * @param distribution The distribution pattern between the upstream vertex and the current
     *     vertex.
     * @param upstreamRunningTasks The running tasks of the upstream vertex.
     * @return Whether every task of the current vertex is connected to some active predecessors.
     */
    private boolean hasActiveUpstreamVertex(
            DistributionPattern distribution, BitSet upstreamRunningTasks) {
        return (distribution == DistributionPattern.ALL_TO_ALL
                        && upstreamRunningTasks.cardinality() > 0)
                || (distribution == DistributionPattern.POINTWISE
                        && upstreamRunningTasks.cardinality() == upstreamRunningTasks.size());
    }

    private boolean hasRunningPrecedentTasks(
            ExecutionVertex vertex,
            List<JobEdge> prevJobEdges,
            Map<JobVertexID, BitSet> taskRunningStatusByVertex) {

        InternalExecutionGraphAccessor executionGraphAccessor = vertex.getExecutionGraphAccessor();

        for (int i = 0; i < prevJobEdges.size(); ++i) {
            if (prevJobEdges.get(i).getDistributionPattern() == DistributionPattern.POINTWISE) {
                for (IntermediateResultPartitionID consumedPartitionId :
                        vertex.getConsumedPartitionGroup(i)) {
                    ExecutionVertex precedentTask =
                            executionGraphAccessor
                                    .getResultPartitionOrThrow(consumedPartitionId)
                                    .getProducer();
                    BitSet precedentVertexRunningStatus =
                            taskRunningStatusByVertex.get(precedentTask.getJobvertexId());

                    if (precedentVertexRunningStatus.get(precedentTask.getParallelSubtaskIndex())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Collects the task running status for each job vertex.
     *
     * @return The task running status for each job vertex.
     */
    @VisibleForTesting
    Map<JobVertexID, BitSet> collectTaskRunningStatus() {
        Map<JobVertexID, BitSet> runningStatusByVertex = new HashMap<>();

        for (ExecutionJobVertex vertex : jobVerticesInTopologyOrder) {
            BitSet runningTasks = new BitSet(vertex.getTaskVertices().length);

            for (int i = 0; i < vertex.getTaskVertices().length; ++i) {
                if (!vertex.getTaskVertices()[i].getCurrentExecutionAttempt().isFinished()) {
                    runningTasks.set(i);
                }
            }

            runningStatusByVertex.put(vertex.getJobVertexId(), runningTasks);
        }

        return runningStatusByVertex;
    }

    private List<Execution> createTaskToWaitFor(List<ExecutionVertex> tasks) {
        List<Execution> tasksToAck = new ArrayList<>(tasks.size());
        for (ExecutionVertex task : tasks) {
            tasksToAck.add(task.getCurrentExecutionAttempt());
        }

        return tasksToAck;
    }
}
