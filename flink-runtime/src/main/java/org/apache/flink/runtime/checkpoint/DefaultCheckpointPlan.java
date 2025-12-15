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

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;

import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of he {@link CheckpointPlan}. */
// 封装了 JobManager 在触发特定检查点时，根据执行图当前状态（特别是考虑到部分完成/退出的任务）所计算出的所有必要的任务集合。
// 它不仅提供这些任务列表，还负责在检查点协调过程中，管理和验证已完成任务的状态，确保检查点元数据正确反映 Job 的真实完成状态。
// 单个检查点执行计划的静态快照，包含要触发、等待、提交的任务列表，以及已完成任务的状态填充和验证逻辑，尤其针对 UnionListState 在部分完成场景下的限制进行了严格检查。
public class DefaultCheckpointPlan implements CheckpointPlan {
    // 需要触发的任务列表。
    // 接收检查点 Barrier 的起始任务集合。
    private final List<Execution> tasksToTrigger;
    // 需要等待确认的任务列表。
    // 必须发送 ACK 才能使检查点成功的任务集合。
    private final List<Execution> tasksToWaitFor;
    // 需要提交的任务列表。
    // 检查点成功后需要接收 Commit 消息的任务集合。
    private final List<ExecutionVertex> tasksToCommitTo;
    // 已完成任务列表。
    // 在触发检查点时已经完成其工作的 Execution 实例列表。
    private final List<Execution> finishedTasks;
    // 存在已完成任务的可能性标志。
    private final boolean mayHaveFinishedTasks;
    // 完全完成的 Job 顶点集合。
    private final Map<JobVertexID, ExecutionJobVertex> fullyFinishedOrFinishedOnRestoreVertices;
    // 算子完成任务计数。 存储每个 ExecutionJobVertex 中已报告其内部算子已完成的子任务数量
    private final IdentityHashMap<ExecutionJobVertex, Integer> vertexOperatorsFinishedTasksCount;

    DefaultCheckpointPlan(
            List<Execution> tasksToTrigger,
            List<Execution> tasksToWaitFor,
            List<ExecutionVertex> tasksToCommitTo,
            List<Execution> finishedTasks,
            List<ExecutionJobVertex> fullyFinishedJobVertex,
            boolean mayHaveFinishedTasks) {

        this.tasksToTrigger = checkNotNull(tasksToTrigger);
        this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
        this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
        this.finishedTasks = checkNotNull(finishedTasks);
        this.mayHaveFinishedTasks = mayHaveFinishedTasks;

        this.fullyFinishedOrFinishedOnRestoreVertices = new HashMap<>();
        fullyFinishedJobVertex.forEach(
                jobVertex ->
                        fullyFinishedOrFinishedOnRestoreVertices.put(
                                jobVertex.getJobVertexId(), jobVertex));

        this.vertexOperatorsFinishedTasksCount = new IdentityHashMap<>();
    }

    @Override
    public List<Execution> getTasksToTrigger() {
        return tasksToTrigger;
    }

    @Override
    public List<Execution> getTasksToWaitFor() {
        return tasksToWaitFor;
    }

    @Override
    public List<ExecutionVertex> getTasksToCommitTo() {
        return tasksToCommitTo;
    }

    @Override
    public List<Execution> getFinishedTasks() {
        return finishedTasks;
    }

    @Override
    public Collection<ExecutionJobVertex> getFullyFinishedJobVertex() {
        return fullyFinishedOrFinishedOnRestoreVertices.values();
    }

    @Override
    public boolean mayHaveFinishedTasks() {
        return mayHaveFinishedTasks;
    }

    @Override
    public void reportTaskFinishedOnRestore(ExecutionVertex task) {
        fullyFinishedOrFinishedOnRestoreVertices.putIfAbsent(
                task.getJobvertexId(), task.getJobVertex());
    }

    @Override
    public void reportTaskHasFinishedOperators(ExecutionVertex task) {
        vertexOperatorsFinishedTasksCount.compute(
                task.getJobVertex(), (k, v) -> v == null ? 1 : v + 1);
    }

    @Override
    public void fulfillFinishedTaskStatus(Map<OperatorID, OperatorState> operatorStates) {
        if (!mayHaveFinishedTasks) {
            return;
        }

        Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex = new HashMap<>();
        for (Execution task : finishedTasks) {
            JobVertexID jobVertexId = task.getVertex().getJobvertexId();
            if (!fullyFinishedOrFinishedOnRestoreVertices.containsKey(jobVertexId)) {
                partlyFinishedVertex.put(jobVertexId, task.getVertex().getJobVertex());
            }
        }

        checkNoPartlyFinishedVertexUsedUnionListState(partlyFinishedVertex, operatorStates);
        checkNoPartlyOperatorsFinishedVertexUsedUnionListState(
                partlyFinishedVertex, operatorStates);

        fulfillFullyFinishedOrFinishedOnRestoreOperatorStates(operatorStates);
        fulfillSubtaskStateForPartiallyFinishedOperators(operatorStates);
    }

    /**
     * If a job vertex using {@code UnionListState} has part of tasks FINISHED where others are
     * still in RUNNING state, the checkpoint would be aborted since it might cause incomplete
     * {@code UnionListState}.
     */
    private void checkNoPartlyFinishedVertexUsedUnionListState(
            Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex,
            Map<OperatorID, OperatorState> operatorStates) {
        for (ExecutionJobVertex vertex : partlyFinishedVertex.values()) {
            if (hasUsedUnionListState(vertex, operatorStates)) {
                throw new PartialFinishingNotSupportedByStateException(
                        String.format(
                                "The vertex %s (id = %s) has used"
                                        + " UnionListState, but part of its tasks are FINISHED.",
                                vertex.getName(), vertex.getJobVertexId()));
            }
        }
    }

    /**
     * If a job vertex using {@code UnionListState} has all the tasks in RUNNING state, but part of
     * the tasks have reported that the operators are finished, the checkpoint would be aborted.
     * This is to force the fast tasks wait for the slow tasks so that their final checkpoints would
     * be the same one, otherwise if the fast tasks finished, the slow tasks would be blocked
     * forever since all the following checkpoints would be aborted.
     */
    private void checkNoPartlyOperatorsFinishedVertexUsedUnionListState(
            Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex,
            Map<OperatorID, OperatorState> operatorStates) {
        for (Map.Entry<ExecutionJobVertex, Integer> entry :
                vertexOperatorsFinishedTasksCount.entrySet()) {
            ExecutionJobVertex vertex = entry.getKey();

            // If the vertex is partly finished, then it must not used UnionListState
            // due to it passed the previous check.
            if (partlyFinishedVertex.containsKey(vertex.getJobVertexId())) {
                continue;
            }

            if (entry.getValue() != vertex.getParallelism()
                    && hasUsedUnionListState(vertex, operatorStates)) {
                throw new PartialFinishingNotSupportedByStateException(
                        String.format(
                                "The vertex %s (id = %s) has used"
                                        + " UnionListState, but part of its tasks has called operators' finish method.",
                                vertex.getName(), vertex.getJobVertexId()));
            }
        }
    }

    private boolean hasUsedUnionListState(
            ExecutionJobVertex vertex, Map<OperatorID, OperatorState> operatorStates) {
        for (OperatorIDPair operatorIDPair : vertex.getOperatorIDs()) {
            OperatorState operatorState =
                    operatorStates.get(operatorIDPair.getGeneratedOperatorID());
            if (operatorState == null) {
                continue;
            }

            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                boolean hasUnionListState =
                        Stream.concat(
                                        operatorSubtaskState.getManagedOperatorState().stream(),
                                        operatorSubtaskState.getRawOperatorState().stream())
                                .filter(Objects::nonNull)
                                .flatMap(
                                        operatorStateHandle ->
                                                operatorStateHandle.getStateNameToPartitionOffsets()
                                                        .values().stream())
                                .anyMatch(
                                        stateMetaInfo ->
                                                stateMetaInfo.getDistributionMode()
                                                        == OperatorStateHandle.Mode.UNION);

                if (hasUnionListState) {
                    return true;
                }
            }
        }

        return false;
    }

    private void fulfillFullyFinishedOrFinishedOnRestoreOperatorStates(
            Map<OperatorID, OperatorState> operatorStates) {
        // Completes the operator state for the fully finished operators
        for (ExecutionJobVertex jobVertex : fullyFinishedOrFinishedOnRestoreVertices.values()) {
            for (OperatorIDPair operatorID : jobVertex.getOperatorIDs()) {
                OperatorState operatorState =
                        operatorStates.get(operatorID.getGeneratedOperatorID());
                checkState(
                        operatorState == null || !operatorState.hasSubtaskStates(),
                        "There should be no states or only coordinator state reported for fully finished operators");

                operatorState =
                        new FullyFinishedOperatorState(
                                operatorID.getGeneratedOperatorID(),
                                jobVertex.getParallelism(),
                                jobVertex.getMaxParallelism());
                operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
            }
        }
    }

    private void fulfillSubtaskStateForPartiallyFinishedOperators(
            Map<OperatorID, OperatorState> operatorStates) {
        for (Execution finishedTask : finishedTasks) {
            ExecutionJobVertex jobVertex = finishedTask.getVertex().getJobVertex();
            for (OperatorIDPair operatorIDPair : jobVertex.getOperatorIDs()) {
                OperatorState operatorState =
                        operatorStates.get(operatorIDPair.getGeneratedOperatorID());

                if (operatorState != null && operatorState.isFullyFinished()) {
                    continue;
                }

                if (operatorState == null) {
                    operatorState =
                            new OperatorState(
                                    operatorIDPair.getGeneratedOperatorID(),
                                    jobVertex.getParallelism(),
                                    jobVertex.getMaxParallelism());
                    operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
                }

                operatorState.putState(
                        finishedTask.getParallelSubtaskIndex(),
                        FinishedOperatorSubtaskState.INSTANCE);
            }
        }
    }
}
