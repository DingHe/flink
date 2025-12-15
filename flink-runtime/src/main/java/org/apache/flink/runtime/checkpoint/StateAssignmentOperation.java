/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a
 * checkpoint.
 */
// StateAssignmentOperation 类封装了 Flink 从检查点（Checkpoint）或保存点（Savepoint）恢复状态到新启动的 Job 拓扑时的整个过程。
// 当 Flink 作业由于故障重启或进行扩缩容（Rescaling）时，任务的并行度（Parallelism）可能发生变化。该类的主要职责就是：
// 收集和验证：获取来自检查点的旧状态元数据 (OperatorState) 和当前 Job 的执行图 (ExecutionJobVertex)。
// 状态重分配（Repartitioning）：根据新的并行度（Parallelism）和最大并行度（Max Parallelism），将旧状态（包括 Keyed State、Operator State 和 Channel State）按正确的逻辑重新分配给新 Job 的每个并行子任务。
// 状态分配：将重分配后的状态句柄封装成 TaskStateSnapshot，并设置给新的任务执行尝试 (Execution 实例)，以便 TaskManager 上的任务可以正确地加载和恢复状态。
// 它是实现 Flink 无缝故障恢复和并行度伸缩的关键桥梁。
@Internal
public class StateAssignmentOperation {

    private static final Logger LOG = LoggerFactory.getLogger(StateAssignmentOperation.class);
    // 当前 Job 拓扑中所有任务顶点（Job Vertex）的集合。
    // 这些是需要被分配状态的目标任务。
    private final Set<ExecutionJobVertex> tasks;
    // 从检查点加载的原始 Operator 状态。
    // 键是 OperatorID，值是该 Operator 的完整状态元数据。
    private final Map<OperatorID, OperatorState> operatorStates;
    // 用于恢复的检查点/保存点 ID。
    private final long restoreCheckpointId;
    // 指示是否允许检查点中存在无法映射到当前 Job 拓扑中的状态（即未恢复的状态）。
    private final boolean allowNonRestoredState;

    /** The state assignments for each ExecutionJobVertex that will be filled in multiple passes. */
    // 主要的内部状态映射。
    // 存储了每个任务顶点 (ExecutionJobVertex) 最终计算出的状态分配结果 (TaskStateAssignment)
    private final Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments;
    /**
     * Stores the assignment of a consumer. {@link IntermediateResult} only allows to traverse
     * producer.
     */
    // 消费者分配映射。
    // 用于通过一个中间数据集 ID (IntermediateDataSetID) 快速查找其消费者任务 (TaskStateAssignment) 的状态分配信息。
    // 这有助于处理 Input/Output Channel State 的重分配。
    private final Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment =
            new HashMap<>();

    public StateAssignmentOperation(
            long restoreCheckpointId,
            Set<ExecutionJobVertex> tasks,
            Map<OperatorID, OperatorState> operatorStates,
            boolean allowNonRestoredState) {

        this.restoreCheckpointId = restoreCheckpointId;
        this.tasks = Preconditions.checkNotNull(tasks);
        this.operatorStates = Preconditions.checkNotNull(operatorStates);
        this.allowNonRestoredState = allowNonRestoredState;
        this.vertexAssignments = CollectionUtil.newHashMapWithExpectedSize(tasks.size());
    }
    // Flink 状态分配和恢复操作的核心驱动逻辑。它将从检查点加载的 Operator 状态（旧状态）映射、重分区并分配给当前新启动的 Job 拓扑中的任务（新任务）
    // 整个方法分为三个主要阶段：检查与初始化、状态重分区和最终状态分配。
    public void assignStates() {
        // 检查从检查点加载的所有状态 (operatorStates) 是否都能被当前 Job 拓扑 (tasks) 中的 Operator 找到对应的位置。
        checkStateMappingCompleteness(allowNonRestoredState, operatorStates, tasks);

        Map<OperatorID, OperatorState> localOperators = new HashMap<>(operatorStates);

        // find the states of all operators belonging to this task and compute additional
        // information in first pass
        // 第一遍遍历：任务状态分配初始化
        // 此阶段遍历 JobGraph 中的每个任务顶点 (ExecutionJobVertex)，确定它所需的所有 Operator 状态，并初始化 TaskStateAssignment 对象。
        for (ExecutionJobVertex executionJobVertex : tasks) {
            // 获取当前任务顶点（Task Vertex）中包含的所有 Operator ID 对（包含用户定义的 ID 和 Flink 生成的 ID）。
            List<OperatorIDPair> operatorIDPairs = executionJobVertex.getOperatorIDs();
            Map<OperatorID, OperatorState> operatorStates =
                    CollectionUtil.newHashMapWithExpectedSize(operatorIDPairs.size());
            for (OperatorIDPair operatorIDPair : operatorIDPairs) {
                // 首先尝试使用用户定义的 Operator ID 去 localOperators 中查找状态。
                // 如果找不到或者用户没有定义 ID，则使用 Flink 生成的 Operator ID。
                OperatorID operatorID =
                        operatorIDPair
                                .getUserDefinedOperatorID()
                                .filter(localOperators::containsKey)
                                .orElse(operatorIDPair.getGeneratedOperatorID());
                // 从本地 Map 中取出并移除找到的 Operator 状态。
                // 一旦取出，它就不会被其他任务（如果存在 Operator State 共享的 bug，可以防止重复分配）再次使用。
                OperatorState operatorState = localOperators.remove(operatorID);
                // 处理无状态 Operator。如果检查点中没有为这个 Operator 存储状态（即 operatorState 为空），
                // 则为其创建一个新的、空的 OperatorState 对象，但仍包含正确的 ID、并行度和最大并行度元数据。
                if (operatorState == null) {
                    operatorState =
                            new OperatorState(
                                    operatorID,
                                    executionJobVertex.getParallelism(),
                                    executionJobVertex.getMaxParallelism());
                }
                // 将找到的（或新建的）Operator 状态，使用 Flink 生成的 Operator ID 作为键，存入当前任务的 Map 中。
                operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
            }
            // 创建任务状态分配对象。
            // 将任务顶点、它包含的 Operator 状态，以及全局的 consumerAssignment 和 vertexAssignments 映射传入，
            // 构建一个 TaskStateAssignment 实例。
            // 这个对象封装了处理一个任务所需的所有状态和元数据。
            final TaskStateAssignment stateAssignment =
                    new TaskStateAssignment(
                            executionJobVertex,
                            operatorStates,
                            consumerAssignment,
                            vertexAssignments);
            vertexAssignments.put(executionJobVertex, stateAssignment);
            // 遍历当前任务上游产生的所有数据结果集 (IntermediateResult)。
            // 将上游结果集 ID 映射到当前任务的 stateAssignment。
            // 这主要用于在重分配 Channel State 时，能够快速定位消费该数据的下游任务的分配对象。
            for (final IntermediateResult producedDataSet : executionJobVertex.getInputs()) {
                consumerAssignment.put(producedDataSet.getId(), stateAssignment);
            }
        }

        // repartition state
        // 3. 状态重分区阶段（Rescaling）
        // 遍历所有已初始化的任务状态分配对象。
        for (TaskStateAssignment stateAssignment : vertexAssignments.values()) {
            // 重分区条件判断
            // 当前任务包含非完全结束（Fully Finished）的 Operator 状态（即有 Keyed State 或 Operator State 需要恢复或重分区）
            // 上游任务有输出通道状态（Result Subpartition State），这可能影响当前任务的恢复。
            // 下游任务有输入通道状态（Input Channel State），这可能影响当前任务的恢复。
            if (stateAssignment.hasNonFinishedState
                    // FLINK-31963: We need to run repartitioning for stateless operators that have
                    // upstream output or downstream input states.
                    || stateAssignment.hasUpstreamOutputStates()
                    || stateAssignment.hasDownstreamInputStates()) {
                // 执行实际的状态重分配逻辑
                // 根据新旧并行度重新划分 Keyed State 的 Key Group 范围，并重新分配 Operator State 和 Channel State。
                assignAttemptState(stateAssignment);
            }
        }

        // actually assign the state
        // 4. 最终状态分配阶段
        // 将重分区后的状态封装成 TaskStateSnapshot，并分配给 Job Master 上的 Execution 对象。
        for (TaskStateAssignment stateAssignment : vertexAssignments.values()) {
            // If upstream has output states or downstream has input states, even the empty task
            // state should be assigned for the current task in order to notify this task that the
            // old states will send to it which likely should be filtered.
            // 最终分配条件判断
            // 任务有正常的非结束状态需要恢复。
            // 任务已完全结束。虽然没有状态需要恢复，但必须通知任务调度器该任务应被标记为已完成恢复状态。
            // 即使任务本身无状态，但如果它的上游或下游涉及 Channel State，它也必须接收一个（可能是空的）状态快照，以便在 Task 启动时执行相关的网络/通道初始化和状态过滤逻辑。
            if (stateAssignment.hasNonFinishedState
                    || stateAssignment.isFullyFinished
                    || stateAssignment.hasUpstreamOutputStates()
                    || stateAssignment.hasDownstreamInputStates()) {
                // 执行最终分配
                // 为当前任务顶点下的每个并行子任务（Execution Attempt）构建最终的 JobManagerTaskRestore 对象，
                // 并调用 Execution.setInitialState(...) 将状态元数据发送给 Task Manager，启动任务恢复过程。
                assignTaskStateToExecutionJobVertices(stateAssignment);
            }
        }
    }

    private void assignAttemptState(TaskStateAssignment taskStateAssignment) {

        // 1. first compute the new parallelism
        checkParallelismPreconditions(taskStateAssignment);

        List<KeyGroupRange> keyGroupPartitions =
                createKeyGroupPartitions(
                        taskStateAssignment.executionJobVertex.getMaxParallelism(),
                        taskStateAssignment.newParallelism);

        /*
         * Redistribute ManagedOperatorStates and RawOperatorStates from old parallelism to new parallelism.
         *
         * The old ManagedOperatorStates with old parallelism 3:
         *
         * 		parallelism0 parallelism1 parallelism2
         * op0   states0,0    state0,1	   state0,2
         * op1
         * op2   states2,0    state2,1	   state2,2
         * op3   states3,0    state3,1     state3,2
         *
         * The new ManagedOperatorStates with new parallelism 4:
         *
         * 		parallelism0 parallelism1 parallelism2 parallelism3
         * op0   state0,0	  state0,1 	   state0,2		state0,3
         * op1
         * op2   state2,0	  state2,1 	   state2,2		state2,3
         * op3   state3,0	  state3,1 	   state3,2		state3,3
         */
        reDistributePartitionableStates(
                taskStateAssignment.oldState,
                taskStateAssignment.newParallelism,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                taskStateAssignment.subManagedOperatorState);
        reDistributePartitionableStates(
                taskStateAssignment.oldState,
                taskStateAssignment.newParallelism,
                OperatorSubtaskState::getRawOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                taskStateAssignment.subRawOperatorState);

        reDistributeInputChannelStates(taskStateAssignment);
        reDistributeResultSubpartitionStates(taskStateAssignment);

        reDistributeKeyedStates(keyGroupPartitions, taskStateAssignment);
    }

    private void assignTaskStateToExecutionJobVertices(TaskStateAssignment assignment) {
        ExecutionJobVertex executionJobVertex = assignment.executionJobVertex;

        List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();
        final int newParallelism = executionJobVertex.getParallelism();

        /*
         *  An executionJobVertex's all state handles needed to restore are something like a matrix
         *
         * 		parallelism0 parallelism1 parallelism2 parallelism3
         * op0   sh(0,0)     sh(0,1)       sh(0,2)	    sh(0,3)
         * op1   sh(1,0)	 sh(1,1)	   sh(1,2)	    sh(1,3)
         * op2   sh(2,0)	 sh(2,1)	   sh(2,2)		sh(2,3)
         * op3   sh(3,0)	 sh(3,1)	   sh(3,2)		sh(3,3)
         *
         */
        for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {
            Execution currentExecutionAttempt =
                    executionJobVertex.getTaskVertices()[subTaskIndex].getCurrentExecutionAttempt();

            if (assignment.isFullyFinished) {
                assignFinishedStateToTask(currentExecutionAttempt);
            } else {
                assignNonFinishedStateToTask(
                        assignment, operatorIDs, subTaskIndex, currentExecutionAttempt);
            }
        }
    }

    private void assignFinishedStateToTask(Execution currentExecutionAttempt) {
        JobManagerTaskRestore taskRestore =
                new JobManagerTaskRestore(
                        restoreCheckpointId, TaskStateSnapshot.FINISHED_ON_RESTORE);
        currentExecutionAttempt.setInitialState(taskRestore);
    }

    private void assignNonFinishedStateToTask(
            TaskStateAssignment assignment,
            List<OperatorIDPair> operatorIDs,
            int subTaskIndex,
            Execution currentExecutionAttempt) {
        TaskStateSnapshot taskState = new TaskStateSnapshot(operatorIDs.size(), false);

        for (OperatorIDPair operatorID : operatorIDs) {
            OperatorInstanceID instanceID =
                    OperatorInstanceID.of(subTaskIndex, operatorID.getGeneratedOperatorID());

            OperatorSubtaskState operatorSubtaskState = assignment.getSubtaskState(instanceID);

            taskState.putSubtaskStateByOperatorID(
                    operatorID.getGeneratedOperatorID(), operatorSubtaskState);
        }

        JobManagerTaskRestore taskRestore =
                new JobManagerTaskRestore(restoreCheckpointId, taskState);
        currentExecutionAttempt.setInitialState(taskRestore);
    }

    public void checkParallelismPreconditions(TaskStateAssignment taskStateAssignment) {
        for (OperatorState operatorState : taskStateAssignment.oldState.values()) {
            checkParallelismPreconditions(operatorState, taskStateAssignment.executionJobVertex);
        }
    }

    private void reDistributeKeyedStates(
            List<KeyGroupRange> keyGroupPartitions, TaskStateAssignment stateAssignment) {
        stateAssignment.oldState.forEach(
                (operatorID, operatorState) -> {
                    for (int subTaskIndex = 0;
                            subTaskIndex < stateAssignment.newParallelism;
                            subTaskIndex++) {
                        OperatorInstanceID instanceID =
                                OperatorInstanceID.of(subTaskIndex, operatorID);
                        Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> subKeyedStates =
                                reAssignSubKeyedStates(
                                        operatorState,
                                        keyGroupPartitions,
                                        subTaskIndex,
                                        stateAssignment.newParallelism,
                                        operatorState.getParallelism());
                        stateAssignment.subManagedKeyedState.put(instanceID, subKeyedStates.f0);
                        stateAssignment.subRawKeyedState.put(instanceID, subKeyedStates.f1);
                    }
                });
    }

    // TODO rewrite based on operator id
    private Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> reAssignSubKeyedStates(
            OperatorState operatorState,
            List<KeyGroupRange> keyGroupPartitions,
            int subTaskIndex,
            int newParallelism,
            int oldParallelism) {

        List<KeyedStateHandle> subManagedKeyedState;
        List<KeyedStateHandle> subRawKeyedState;

        if (newParallelism == oldParallelism) {
            if (operatorState.getState(subTaskIndex) != null) {
                subManagedKeyedState =
                        operatorState.getState(subTaskIndex).getManagedKeyedState().asList();
                subRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState().asList();
            } else {
                subManagedKeyedState = emptyList();
                subRawKeyedState = emptyList();
            }
        } else {
            subManagedKeyedState =
                    getManagedKeyedStateHandles(
                            operatorState, keyGroupPartitions.get(subTaskIndex));
            subRawKeyedState =
                    getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
        }

        if (subManagedKeyedState.isEmpty() && subRawKeyedState.isEmpty()) {
            return new Tuple2<>(emptyList(), emptyList());
        } else {
            return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
        }
    }

    public static <T extends StateObject> void reDistributePartitionableStates(
            Map<OperatorID, OperatorState> oldOperatorStates,
            int newParallelism,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle,
            OperatorStateRepartitioner<T> stateRepartitioner,
            Map<OperatorInstanceID, List<T>> result) {

        // The nested list wraps as the level of operator -> subtask -> state object collection
        Map<OperatorID, List<List<T>>> oldStates =
                splitManagedAndRawOperatorStates(oldOperatorStates, extractHandle);

        oldOperatorStates.forEach(
                (operatorID, oldOperatorState) ->
                        result.putAll(
                                applyRepartitioner(
                                        operatorID,
                                        stateRepartitioner,
                                        oldStates.get(operatorID),
                                        oldOperatorState.getParallelism(),
                                        newParallelism)));
    }

    public void reDistributeResultSubpartitionStates(TaskStateAssignment assignment) {
        // FLINK-31963: We can skip this phase if there is no output state AND downstream has no
        // input states
        if (!assignment.hasOutputState && !assignment.hasDownstreamInputStates()) {
            return;
        }

        checkForUnsupportedToplogyChanges(
                assignment.oldState,
                OperatorSubtaskState::getResultSubpartitionState,
                assignment.outputOperatorID);

        final OperatorState outputState = assignment.oldState.get(assignment.outputOperatorID);
        final List<List<ResultSubpartitionStateHandle>> outputOperatorState =
                splitBySubtasks(outputState, OperatorSubtaskState::getResultSubpartitionState);

        final ExecutionJobVertex executionJobVertex = assignment.executionJobVertex;
        final List<IntermediateDataSet> outputs =
                executionJobVertex.getJobVertex().getProducedDataSets();

        if (outputState.getParallelism() == executionJobVertex.getParallelism()) {
            assignment.resultSubpartitionStates.putAll(
                    toInstanceMap(assignment.outputOperatorID, outputOperatorState));
            return;
        }
        // Parallelism of this vertex changed, distribute ResultSubpartitionStateHandle
        // according to output mapping.
        for (int partitionIndex = 0; partitionIndex < outputs.size(); partitionIndex++) {
            final List<List<ResultSubpartitionStateHandle>> partitionState =
                    outputs.size() == 1
                            ? outputOperatorState
                            : getPartitionState(
                                    outputOperatorState,
                                    ResultSubpartitionInfo::getPartitionIdx,
                                    partitionIndex);
            final MappingBasedRepartitioner<ResultSubpartitionStateHandle> repartitioner =
                    new MappingBasedRepartitioner<>(
                            assignment.getOutputMapping(partitionIndex).getRescaleMappings());
            final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> repartitioned =
                    applyRepartitioner(
                            assignment.outputOperatorID,
                            repartitioner,
                            partitionState,
                            outputOperatorState.size(),
                            executionJobVertex.getParallelism());
            addToSubtasks(assignment.resultSubpartitionStates, repartitioned);
        }
    }

    public void reDistributeInputChannelStates(TaskStateAssignment stateAssignment) {
        // FLINK-31963: We can skip this phase only if there is no input state AND upstream has no
        // output states
        if (!stateAssignment.hasInputState && !stateAssignment.hasUpstreamOutputStates()) {
            return;
        }

        checkForUnsupportedToplogyChanges(
                stateAssignment.oldState,
                OperatorSubtaskState::getInputChannelState,
                stateAssignment.inputOperatorID);

        final ExecutionJobVertex executionJobVertex = stateAssignment.executionJobVertex;
        final List<IntermediateResult> inputs = executionJobVertex.getInputs();

        // check for rescaling: no rescaling = simple reassignment
        final OperatorState inputState =
                stateAssignment.oldState.get(stateAssignment.inputOperatorID);
        final List<List<InputChannelStateHandle>> inputOperatorState =
                splitBySubtasks(inputState, OperatorSubtaskState::getInputChannelState);

        boolean hasAnyFullMapper =
                executionJobVertex.getJobVertex().getInputs().stream()
                        .map(JobEdge::getDownstreamSubtaskStateMapper)
                        .anyMatch(m -> m.equals(SubtaskStateMapper.FULL));
        boolean hasAnyPreviousOperatorChanged =
                executionJobVertex.getInputs().stream()
                        .map(IntermediateResult::getProducer)
                        .map(vertexAssignments::get)
                        .anyMatch(
                                taskStateAssignment -> {
                                    final int oldParallelism =
                                            stateAssignment
                                                    .oldState
                                                    .get(stateAssignment.inputOperatorID)
                                                    .getParallelism();
                                    return oldParallelism
                                            != taskStateAssignment.executionJobVertex
                                                    .getParallelism();
                                });

        // need rescale if any input operator parallelism was changed and have any input with FULL
        // subtask state mapper
        if (inputState.getParallelism() == executionJobVertex.getParallelism()
                && !(hasAnyFullMapper && hasAnyPreviousOperatorChanged)) {
            stateAssignment.inputChannelStates.putAll(
                    toInstanceMap(stateAssignment.inputOperatorID, inputOperatorState));
            return;
        }
        // if this subtask has a different degree of parallelism, use the partitioner to figure
        // out to which subtasks the state should be reassigned. In some cases, state is
        // replicated to multiple subtasks and filtered during recovery.
        // example: if this task is downscaled from 3 to 2 and it uses a range partitioner over
        // [0;128)
        // old assignment: 0 -> [0;43); 1 -> [43;87); 2 -> [87;128)
        // new assignment: 0 -> [0;64]; 1 -> [64;128)
        // subtask 0 recovers data from old subtask 0 + 1 and subtask 1 recovers data from old
        // subtask 1 + 2
        for (int gateIndex = 0; gateIndex < inputs.size(); gateIndex++) {
            final RescaleMappings mapping =
                    stateAssignment.getInputMapping(gateIndex).getRescaleMappings();

            final List<List<InputChannelStateHandle>> gateState =
                    inputs.size() == 1
                            ? inputOperatorState
                            : getPartitionState(
                                    inputOperatorState, InputChannelInfo::getGateIdx, gateIndex);
            final MappingBasedRepartitioner<InputChannelStateHandle> repartitioner =
                    new MappingBasedRepartitioner<>(mapping);
            final Map<OperatorInstanceID, List<InputChannelStateHandle>> repartitioned =
                    applyRepartitioner(
                            stateAssignment.inputOperatorID,
                            repartitioner,
                            gateState,
                            inputOperatorState.size(),
                            stateAssignment.newParallelism);
            addToSubtasks(stateAssignment.inputChannelStates, repartitioned);
        }
    }

    private static <K, V> void addToSubtasks(Map<K, List<V>> target, Map<K, List<V>> toAdd) {
        toAdd.forEach(
                (key, values) ->
                        target.computeIfAbsent(key, (unused) -> new ArrayList<>(values.size()))
                                .addAll(values));
    }

    private <T extends AbstractChannelStateHandle<?>> void checkForUnsupportedToplogyChanges(
            Map<OperatorID, OperatorState> oldOperatorStates,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle,
            OperatorID expectedOperatorID) {
        final List<OperatorID> unexpectedState =
                oldOperatorStates.entrySet().stream()
                        .filter(idAndState -> !idAndState.getKey().equals(expectedOperatorID))
                        .filter(idAndState -> hasChannelState(idAndState.getValue(), extractHandle))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        if (!unexpectedState.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot recover from unaligned checkpoint when topology changes, such that "
                            + "data exchanges with persisted data are now chained.\n"
                            + "The following operators contain channel state: "
                            + unexpectedState);
        }
    }

    private <T extends AbstractChannelStateHandle<?>> boolean hasChannelState(
            OperatorState operatorState,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle) {
        return operatorState.getSubtaskStates().values().stream()
                .anyMatch(subState -> !isEmpty(extractHandle.apply(subState)));
    }

    private <T extends AbstractChannelStateHandle<?>> boolean isEmpty(StateObjectCollection<T> s) {
        return s.stream().allMatch(state -> state.getOffsets().isEmpty());
    }

    private static <T extends AbstractChannelStateHandle<I>, I> List<List<T>> getPartitionState(
            List<List<T>> subtaskStates, Function<I, Integer> partitionExtractor, int partitionId) {
        return subtaskStates.stream()
                .map(
                        subtaskState ->
                                subtaskState.stream()
                                        .filter(
                                                state ->
                                                        partitionExtractor.apply(state.getInfo())
                                                                == partitionId)
                                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    private static <T extends StateObject>
            Map<OperatorID, List<List<T>>> splitManagedAndRawOperatorStates(
                    Map<OperatorID, OperatorState> operatorStates,
                    Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle) {
        return operatorStates.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                operatorIdAndState ->
                                        splitBySubtasks(
                                                operatorIdAndState.getValue(), extractHandle)));
    }

    private static <T extends StateObject> List<List<T>> splitBySubtasks(
            OperatorState operatorState,
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractHandle) {
        List<List<T>> statePerSubtask = new ArrayList<>(operatorState.getParallelism());

        for (int subTaskIndex = 0; subTaskIndex < operatorState.getParallelism(); subTaskIndex++) {
            OperatorSubtaskState subtaskState = operatorState.getState(subTaskIndex);
            statePerSubtask.add(
                    subtaskState == null
                            ? emptyList()
                            : extractHandle.apply(subtaskState).asList());
        }

        return statePerSubtask;
    }

    /**
     * Collect {@link KeyGroupsStateHandle managedKeyedStateHandles} which have intersection with
     * given {@link KeyGroupRange} from {@link TaskState operatorState}.
     *
     * @param operatorState all state handles of a operator
     * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
     * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
     */
    public static List<KeyedStateHandle> getManagedKeyedStateHandles(
            OperatorState operatorState, KeyGroupRange subtaskKeyGroupRange) {

        final int parallelism = operatorState.getParallelism();

        List<KeyedStateHandle> subtaskKeyedStateHandles = null;

        for (int i = 0; i < parallelism; i++) {
            if (operatorState.getState(i) != null) {

                Collection<KeyedStateHandle> keyedStateHandles =
                        operatorState.getState(i).getManagedKeyedState();

                if (subtaskKeyedStateHandles == null) {
                    subtaskKeyedStateHandles =
                            new ArrayList<>(parallelism * keyedStateHandles.size());
                }

                extractIntersectingState(
                        keyedStateHandles, subtaskKeyGroupRange, subtaskKeyedStateHandles);
            }
        }

        return subtaskKeyedStateHandles != null ? subtaskKeyedStateHandles : emptyList();
    }

    /**
     * Collect {@link KeyGroupsStateHandle rawKeyedStateHandles} which have intersection with given
     * {@link KeyGroupRange} from {@link TaskState operatorState}.
     *
     * @param operatorState all state handles of a operator
     * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
     * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
     */
    public static List<KeyedStateHandle> getRawKeyedStateHandles(
            OperatorState operatorState, KeyGroupRange subtaskKeyGroupRange) {

        final int parallelism = operatorState.getParallelism();

        List<KeyedStateHandle> extractedKeyedStateHandles = null;

        for (int i = 0; i < parallelism; i++) {
            if (operatorState.getState(i) != null) {

                Collection<KeyedStateHandle> rawKeyedState =
                        operatorState.getState(i).getRawKeyedState();

                if (extractedKeyedStateHandles == null) {
                    extractedKeyedStateHandles =
                            new ArrayList<>(parallelism * rawKeyedState.size());
                }

                extractIntersectingState(
                        rawKeyedState, subtaskKeyGroupRange, extractedKeyedStateHandles);
            }
        }

        return extractedKeyedStateHandles != null ? extractedKeyedStateHandles : emptyList();
    }

    /**
     * Extracts certain key group ranges from the given state handles and adds them to the
     * collector.
     */
    @VisibleForTesting
    public static void extractIntersectingState(
            Collection<? extends KeyedStateHandle> originalSubtaskStateHandles,
            KeyGroupRange rangeToExtract,
            List<KeyedStateHandle> extractedStateCollector) {

        for (KeyedStateHandle keyedStateHandle : originalSubtaskStateHandles) {

            if (keyedStateHandle != null) {

                KeyedStateHandle intersectedKeyedStateHandle =
                        keyedStateHandle.getIntersection(rangeToExtract);

                if (intersectedKeyedStateHandle != null) {
                    extractedStateCollector.add(intersectedKeyedStateHandle);
                }
            }
        }
    }

    /**
     * Groups the available set of key groups into key group partitions. A key group partition is
     * the set of key groups which is assigned to the same task. Each set of the returned list
     * constitutes a key group partition.
     *
     * <p><b>IMPORTANT</b>: The assignment of key groups to partitions has to be in sync with the
     * KeyGroupStreamPartitioner.
     *
     * @param numberKeyGroups Number of available key groups (indexed from 0 to numberKeyGroups - 1)
     * @param parallelism Parallelism to generate the key group partitioning for
     * @return List of key group partitions
     */
    public static List<KeyGroupRange> createKeyGroupPartitions(
            int numberKeyGroups, int parallelism) {
        Preconditions.checkArgument(numberKeyGroups >= parallelism);
        List<KeyGroupRange> result = new ArrayList<>(parallelism);

        for (int i = 0; i < parallelism; ++i) {
            result.add(
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            numberKeyGroups, parallelism, i));
        }
        return result;
    }

    /**
     * Verifies conditions in regards to parallelism and maxParallelism that must be met when
     * restoring state.
     *
     * @param operatorState state to restore
     * @param executionJobVertex task for which the state should be restored
     */
    private static void checkParallelismPreconditions(
            OperatorState operatorState, ExecutionJobVertex executionJobVertex) {
        // ----------------------------------------max parallelism
        // preconditions-------------------------------------

        if (operatorState.getMaxParallelism() < executionJobVertex.getParallelism()) {
            throw new IllegalStateException(
                    "The state for task "
                            + executionJobVertex.getJobVertexId()
                            + " can not be restored. The maximum parallelism ("
                            + operatorState.getMaxParallelism()
                            + ") of the restored state is lower than the configured parallelism ("
                            + executionJobVertex.getParallelism()
                            + "). Please reduce the parallelism of the task to be lower or equal to the maximum parallelism.");
        }

        // check that the number of key groups have not changed or if we need to override it to
        // satisfy the restored state
        if (operatorState.getMaxParallelism() != executionJobVertex.getMaxParallelism()) {

            if (executionJobVertex.canRescaleMaxParallelism(operatorState.getMaxParallelism())) {
                LOG.debug(
                        "Rescaling maximum parallelism for JobVertex {} from {} to {}",
                        executionJobVertex.getJobVertexId(),
                        executionJobVertex.getMaxParallelism(),
                        operatorState.getMaxParallelism());

                executionJobVertex.setMaxParallelism(operatorState.getMaxParallelism());
            } else {
                // if the max parallelism cannot be rescaled, we complain on mismatch
                throw new IllegalStateException(
                        "The maximum parallelism ("
                                + operatorState.getMaxParallelism()
                                + ") with which the latest "
                                + "checkpoint of the execution job vertex "
                                + executionJobVertex
                                + " has been taken and the current maximum parallelism ("
                                + executionJobVertex.getMaxParallelism()
                                + ") changed. This "
                                + "is currently not supported.");
            }
        }
    }

    /**
     * Verifies that all operator states can be mapped to an execution job vertex.
     *
     * @param allowNonRestoredState if false an exception will be thrown if a state could not be
     *     mapped
     * @param operatorStates operator states to map
     * @param tasks task to map to
     */
    private static void checkStateMappingCompleteness(
            boolean allowNonRestoredState,
            Map<OperatorID, OperatorState> operatorStates,
            Set<ExecutionJobVertex> tasks) {

        Set<OperatorID> allOperatorIDs = new HashSet<>();
        for (ExecutionJobVertex executionJobVertex : tasks) {
            for (OperatorIDPair operatorIDPair : executionJobVertex.getOperatorIDs()) {
                allOperatorIDs.add(operatorIDPair.getGeneratedOperatorID());
                operatorIDPair.getUserDefinedOperatorID().ifPresent(allOperatorIDs::add);
            }
        }
        for (Map.Entry<OperatorID, OperatorState> operatorGroupStateEntry :
                operatorStates.entrySet()) {
            // ----------------------------------------find operator for
            // state---------------------------------------------

            if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey())) {

                OperatorState operatorState = operatorGroupStateEntry.getValue();

                if (allowNonRestoredState) {
                    LOG.info(
                            "Skipped checkpoint state for operator {}.",
                            operatorState.getOperatorID());
                } else {
                    throw new IllegalStateException(
                            "There is no operator for the state " + operatorState.getOperatorID());
                }
            }
        }
    }

    public static <T> Map<OperatorInstanceID, List<T>> applyRepartitioner(
            OperatorID operatorID,
            OperatorStateRepartitioner<T> opStateRepartitioner,
            List<List<T>> chainOpParallelStates,
            int oldParallelism,
            int newParallelism) {

        List<List<T>> states =
                applyRepartitioner(
                        opStateRepartitioner,
                        chainOpParallelStates,
                        oldParallelism,
                        newParallelism);

        return toInstanceMap(operatorID, states);
    }

    private static <T> Map<OperatorInstanceID, List<T>> toInstanceMap(
            OperatorID operatorID, List<List<T>> states) {
        Map<OperatorInstanceID, List<T>> result =
                CollectionUtil.newHashMapWithExpectedSize(states.size());

        for (int subtaskIndex = 0; subtaskIndex < states.size(); subtaskIndex++) {
            checkNotNull(states.get(subtaskIndex) != null, "states.get(subtaskIndex) is null");
            result.put(OperatorInstanceID.of(subtaskIndex, operatorID), states.get(subtaskIndex));
        }

        return result;
    }

    /**
     * Repartitions the given operator state using the given {@link OperatorStateRepartitioner} with
     * respect to the new parallelism.
     *
     * @param opStateRepartitioner partitioner to use
     * @param chainOpParallelStates state to repartition
     * @param oldParallelism parallelism with which the state is currently partitioned
     * @param newParallelism parallelism with which the state should be partitioned
     * @return repartitioned state
     */
    // TODO rewrite based on operator id
    public static <T> List<List<T>> applyRepartitioner(
            OperatorStateRepartitioner<T> opStateRepartitioner,
            List<List<T>> chainOpParallelStates,
            int oldParallelism,
            int newParallelism) {

        if (chainOpParallelStates == null) {
            return emptyList();
        }

        return opStateRepartitioner.repartitionState(
                chainOpParallelStates, oldParallelism, newParallelism);
    }
}
