/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Used by {@link StateAssignmentOperation} to store temporal information while creating {@link
 * OperatorSubtaskState}.
 */
// TaskStateAssignment 的主要作用是封装和管理一个 JobVertex (任务组) 在从检查点恢复状态时的所有相关信息，特别是当 Job 的并行度发生变化时。
// 存储任务组信息： 记录当前任务组 (ExecutionJobVertex) 的新并行度。
// 存储旧状态： 记录从检查点加载的该任务组的所有旧状态 (OperatorState，包括托管状态、原始状态、通道状态等)。
// 计算状态分配映射： 在并行度发生变化时（弹性伸缩），根据新的并行度和旧状态并行度，计算出子任务状态的映射关系，即哪个旧子任务的状态应该分配给哪个新子任务。
// 生成子任务状态： 根据计算出的映射关系和旧状态数据，为每个新的子任务 (OperatorInstanceID) 生成最终的 OperatorSubtaskState 对象，该对象将发送给 TaskManager 进行恢复。
// 一个复杂的状态恢复蓝图，尤其关注在拓扑结构发生变化时，如何正确地处理算子状态和通道数据状态 (In-flight data state) 的重新分配。

class TaskStateAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateAssignment.class);
    // 当前正在处理的 JobVertex 的执行图表示。提供了任务组的结构信息，如新并行度、输入/输出数据集等。
    final ExecutionJobVertex executionJobVertex;
    // 从检查点加载的旧的算子状态。
    // Map 的 Key 是 OperatorID，Value 是该算子的整体状态 (OperatorState)。
    final Map<OperatorID, OperatorState> oldState;
    // 标志位，表示 oldState 中是否包含任何未完成的状态。
    final boolean hasNonFinishedState;
    // 标志位，表示 oldState 中所有的算子状态是否都处于 FullyFinished 状态（例如，用于批处理 Job）。
    final boolean isFullyFinished;
    // 标志位，表示该 JobVertex 的输入算子（链的最后一个）是否有输入通道状态（InputChannelStateHandle，用于非对齐检查点）。
    final boolean hasInputState;
    // 标志位，表示该 JobVertex 的输出算子（链的第一个）是否有结果子分区状态（ResultSubpartitionStateHandle，用于非对齐检查点）。
    final boolean hasOutputState;
    // 当前 JobVertex 在新 Job Graph 中的目标并行度。
    final int newParallelism;
    // 该 JobVertex 算子链中最后一个算子的 ID（处理输入的算子）。
    final OperatorID inputOperatorID;
    // 该 JobVertex 算子链中第一个算子的 ID（处理输出的算子）。
    final OperatorID outputOperatorID;
    // 存储已分配的托管算子状态（由 Flink 维护）。
    // Key 是新的子任务实例 ID。
    final Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState;
    // 存储已分配的原始算子状态（用户自己维护）。
    // Key 是新的子任务实例 ID。
    final Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState;
    // 存储已分配的托管 Keyed 状态。Key 是新的子任务实例 ID。
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState;
    // 存储已分配的原始 Keyed 状态。Key 是新的子任务实例 ID。
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState;
    // 存储已分配的输入通道状态（对应该任务的输入 Gate）。
    final Map<OperatorInstanceID, List<InputChannelStateHandle>> inputChannelStates;
    // 存储已分配的结果子分区状态（对应该任务的输出 Partition）。
    final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> resultSubpartitionStates;
    /** The subtask mapping when the output operator was rescaled. */
    // 缓存该任务的输出（发送到下游）在并行度变化时的映射关系，Key 是输出数据集的索引。
    private final Map<Integer, SubtasksRescaleMapping> outputSubtaskMappings = new HashMap<>();
    /** The subtask mapping when the input operator was rescaled. */
    // 缓存该任务的输入（接收自上游）在并行度变化时的映射关系，Key 是输入 Gate 的索引。
    private final Map<Integer, SubtasksRescaleMapping> inputSubtaskMappings = new HashMap<>();
    // 延迟加载（Lazy Loaded）的下游任务分配数组。
    @Nullable private TaskStateAssignment[] downstreamAssignments;
    // 延迟加载的上游任务分配数组。
    @Nullable private TaskStateAssignment[] upstreamAssignments;
    // 缓存该任务上游是否有输出通道状态的标志。
    @Nullable private Boolean hasUpstreamOutputStates;
    // 缓存该任务下游是否有输入通道状态的标志。
    @Nullable private Boolean hasDownstreamInputStates;
    // Map，Key 是该 JobVertex 产生的数据集 ID，Value 是该数据集的下游消费任务组的 TaskStateAssignment。
    // 用于查找下游信息。
    private final Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment;
    // Map，Key 是 ExecutionJobVertex，Value 是其对应的 TaskStateAssignment。
    // 用于查找 Job Graph 中所有任务组的分配信息。
    private final Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments;

    public TaskStateAssignment(
            // executionJobVertex: 当前的任务执行顶点（Job Vertex），包含任务的拓扑和并行度信息。
            ExecutionJobVertex executionJobVertex,
            // oldState: 从检查点加载的、属于该任务顶点中所有 Operator 的原始状态集合。
            Map<OperatorID, OperatorState> oldState,
            // consumerAssignment: 全局的 Map，用于将中间数据ID映射到其消费者任务的 TaskStateAssignment 对象。
            Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment,
            // vertexAssignments: 全局的 Map，用于将任务顶点映射到其TaskStateAssignment 对象。
            Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments) {

        this.executionJobVertex = executionJobVertex;
        this.oldState = oldState;
        // 只要有一个 Operator 的 getNumberCollectedStates() 大于 0（即收集到了 Keyed State、Operator State 或 Channel State），
        // 就将 hasNonFinishedState 设置为 true。
        // 它表示任务有实际的状态需要恢复。
        this.hasNonFinishedState =
                oldState.values().stream()
                        .anyMatch(operatorState -> operatorState.getNumberCollectedStates() > 0);
        // 检查当前任务包含的 Operator 中，是否有任何一个被标记为“完全结束”（例如，使用了 FullyFinishedOperatorState）
        this.isFullyFinished = oldState.values().stream().anyMatch(OperatorState::isFullyFinished);
        // 混合状态检查。
        // 如果发现任务中存在任何一个已结束的 Operator 状态 (isFullyFinished 为 true)，则强制断言所有 Operator 状态都必须是已结束的
        if (isFullyFinished) {
            checkState(
                    oldState.values().stream().allMatch(OperatorState::isFullyFinished),
                    "JobVertex could not have mixed finished and unfinished operators");
        }
        // 获取当前任务顶点在 Job 重新启动后的新并行度，这是状态重分配的目标并行度。
        newParallelism = executionJobVertex.getParallelism();
        this.consumerAssignment = checkNotNull(consumerAssignment);
        this.vertexAssignments = checkNotNull(vertexAssignments);
        // 计算预期的子任务总数
        // 一个任务顶点包含的 Operator 数量 (oldState.size()) 乘以新的并行度 (newParallelism)，得到所有 Operator 子任务（Instance）的总数。
        // 这用于初始化 Map 的预期容量，以提高性能。
        final int expectedNumberOfSubtasks = newParallelism * oldState.size();

        subManagedOperatorState =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subRawOperatorState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        inputChannelStates = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        resultSubpartitionStates =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subManagedKeyedState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subRawKeyedState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);

        final List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();
        // 确定输出 Operator ID。
        // 在 Flink 的任务（Task）内部，Operator 通常被链式（chained）执行。
        // 按照约定，链的首部（索引 0）通常负责输出数据（因为它是第一个处理并可能向下游发送数据的 Operator）。
        outputOperatorID = operatorIDs.get(0).getGeneratedOperatorID();
        // 确定输入 Operator ID。
        // 按照约定，链的尾部（最后一个元素）通常负责接收输入（例如，从网络缓冲区接收或从 Source 读取）
        inputOperatorID = operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID();
        // 检查输入 Operator 的所有子任务状态中，是否有任何一个包含输入通道状态（InputChannelState）
        hasInputState =
                oldState.get(inputOperatorID).getStates().stream()
                        .anyMatch(subState -> !subState.getInputChannelState().isEmpty());
        // 计算 hasOutputState 标志。
        // 检查输出 Operator 的所有子任务状态中，是否有任何一个包含结果子分区状态（ResultSubpartitionState）
        hasOutputState =
                oldState.get(outputOperatorID).getStates().stream()
                        .anyMatch(subState -> !subState.getResultSubpartitionState().isEmpty());
    }
    // 获取下游任务分配。
    // 延迟计算并返回一个 TaskStateAssignment 数组，包含所有直接消费该任务组输出的下游任务的分配信息。
    public TaskStateAssignment[] getDownstreamAssignments() {
        if (downstreamAssignments == null) {
            downstreamAssignments =
                    Arrays.stream(executionJobVertex.getProducedDataSets())
                            .map(result -> consumerAssignment.get(result.getId()))
                            .toArray(TaskStateAssignment[]::new);
        }
        return downstreamAssignments;
    }

    // 获取分配对象的索引。
    private static int getAssignmentIndex(
            TaskStateAssignment[] assignments, TaskStateAssignment assignment) {
        return Arrays.asList(assignments).indexOf(assignment);
    }

    // 获取上游任务分配。
    // 延迟计算并返回一个 TaskStateAssignment 数组，包含所有直接产生该任务组输入数据的上游任务的分配信息。
    public TaskStateAssignment[] getUpstreamAssignments() {
        if (upstreamAssignments == null) {
            upstreamAssignments =
                    executionJobVertex.getInputs().stream()
                            .map(result -> vertexAssignments.get(result.getProducer()))
                            .toArray(TaskStateAssignment[]::new);
        }
        return upstreamAssignments;
    }

    // 根据一个 Operator 实例的唯一 ID（OperatorInstanceID），从已经完成重分区和分配工作的工作区 (TaskStateAssignment) 中，
    // 提取并构建该实例最终需要恢复的完整状态快照（OperatorSubtaskState）。
    // 这个状态快照随后会被发送给 Task Manager，用于启动和初始化具体的 Subtask。
    public OperatorSubtaskState getSubtaskState(OperatorInstanceID instanceID) {
        checkState(
                subManagedKeyedState.containsKey(instanceID)
                        || !subRawKeyedState.containsKey(instanceID),
                "If an operator has no managed key state, it should also not have a raw keyed state.");

        // 根据当前 Operator 实例 ID (instanceID)，从已分配的输入通道状态 Map (inputChannelStates) 中获取该实例需要恢复的输入通道状态句柄集合。
        final StateObjectCollection<InputChannelStateHandle> inputState =
                getState(instanceID, inputChannelStates);
        // 获取该实例需要恢复的输出结果子分区状态句柄集合。
        final StateObjectCollection<ResultSubpartitionStateHandle> outputState =
                getState(instanceID, resultSubpartitionStates);
        return OperatorSubtaskState.builder()
                // 获取并设置重分配给该实例的托管/原始 Operator 状态句柄集合。
                .setManagedOperatorState(getState(instanceID, subManagedOperatorState))
                .setRawOperatorState(getState(instanceID, subRawOperatorState))
                // 获取并设置重分配给该实例的托管/原始 Keyed State 状态句柄集合。
                .setManagedKeyedState(getState(instanceID, subManagedKeyedState))
                .setRawKeyedState(getState(instanceID, subRawKeyedState))
                // 设置前面获取到的输入/输出通道状态句柄集合。
                .setInputChannelState(inputState)
                .setResultSubpartitionState(outputState)
                // 设置用于描述输入通道重伸缩（并行度调整）信息的对象。这对于处理非对齐检查点恢复至关重要。
                .setInputRescalingDescriptor(
                        createRescalingDescriptor(
                                instanceID,
                                inputOperatorID,
                                getUpstreamAssignments(),
                                (assignment, recompute) -> {
                                    int assignmentIndex =
                                            getAssignmentIndex(
                                                    assignment.getDownstreamAssignments(), this);
                                    return assignment.getOutputMapping(assignmentIndex, recompute);
                                },
                                inputSubtaskMappings,
                                this::getInputMapping))
                // 设置用于描述输出通道重伸缩（并行度调整）信息的对象。
                .setOutputRescalingDescriptor(
                        createRescalingDescriptor(
                                instanceID,
                                outputOperatorID,
                                getDownstreamAssignments(),
                                (assignment, recompute) -> {
                                    int assignmentIndex =
                                            getAssignmentIndex(
                                                    assignment.getUpstreamAssignments(), this);
                                    return assignment.getInputMapping(assignmentIndex, recompute);
                                },
                                outputSubtaskMappings,
                                this::getOutputMapping))
                .build();
    }

    public boolean hasUpstreamOutputStates() {
        if (hasUpstreamOutputStates == null) {
            hasUpstreamOutputStates =
                    Arrays.stream(getUpstreamAssignments())
                            .anyMatch(assignment -> assignment.hasOutputState);
        }
        return hasUpstreamOutputStates;
    }

    public boolean hasDownstreamInputStates() {
        if (hasDownstreamInputStates == null) {
            hasDownstreamInputStates =
                    Arrays.stream(getDownstreamAssignments())
                            .anyMatch(assignment -> assignment.hasInputState);
        }
        return hasDownstreamInputStates;
    }

    private InflightDataGateOrPartitionRescalingDescriptor log(
            InflightDataGateOrPartitionRescalingDescriptor descriptor, int subtask, int partition) {
        LOG.debug(
                "created {} for task={} subtask={} partition={}",
                descriptor,
                executionJobVertex.getName(),
                subtask,
                partition);
        return descriptor;
    }

    private InflightDataRescalingDescriptor log(
            InflightDataRescalingDescriptor descriptor, int subtask) {
        LOG.debug(
                "created {} for task={} subtask={}",
                descriptor,
                executionJobVertex.getName(),
                subtask);
        return descriptor;
    }

    // Flink 非对齐检查点 (Unaligned Checkpoints) 或 增量 Savepoint 恢复机制中，处理通道状态 (Channel State) 重伸缩（并行度变化）的核心逻辑。
    // 它负责为特定的 Operator 实例构建一个 InflightDataRescalingDescriptor，告诉 Task Manager 如何处理和恢复网络缓冲区中在途数据的状态。
    private InflightDataRescalingDescriptor createRescalingDescriptor(
            OperatorInstanceID instanceID, // 当前正在为其构建描述符的 Operator 实例 ID。
            OperatorID expectedOperatorID, // 期望该实例所属的 Operator ID（用于输入/输出 Operator 的快速检查）。
            TaskStateAssignment[] connectedAssignments, // 连接到当前任务的上游或下游的所有任务的 TaskStateAssignment 数组。
            BiFunction<TaskStateAssignment, Boolean, SubtasksRescaleMapping> mappingRetriever, // 用于从连接任务（上游或下游）中获取（或计算）与当前任务相关的通道重伸缩映射信息。
            Map<Integer, SubtasksRescaleMapping> subtaskGateOrPartitionMappings, // 存储已计算好的当前任务输入门/输出分区的重伸缩映射。
            Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator) { // 计算当前任务的输入/输出通道自身的重伸缩映射（即新旧并行度之间的映射）

        // 如果当前 instanceID 对应的 Operator ID 与期望的 expectedOperatorID 不匹配，
        // 说明这个 Operator 实例不是负责输入或输出的那个（例如，如果期望的是 inputOperatorID，但 instanceID 属于链中间的 Operator），
        // 那么它不可能有 Channel State 需要恢复。
        if (!expectedOperatorID.equals(instanceID.getOperatorId())) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        SubtasksRescaleMapping[] rescaledChannelsMappings =
                Arrays.stream(connectedAssignments)
                        .map(assignment -> mappingRetriever.apply(assignment, false))
                        .toArray(SubtasksRescaleMapping[]::new);

        // no state on input and output, especially for any aligned checkpoint
        // 检查是否完全没有通道状态需要恢复或重伸缩。
        // 当前任务的输入门/输出分区映射缓存为空（这意味着当前任务的并行度没有变化，并且没有显式的通道状态）
        if (subtaskGateOrPartitionMappings.isEmpty()
                // 所有连接任务的映射也全部为空（这意味着连接任务的并行度也没有变化，并且没有显式的通道状态）。
                && Arrays.stream(rescaledChannelsMappings).allMatch(Objects::isNull)) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }
        // 遍历所有连接的通道（Input Gates 或 Result Partitions），计算每个通道的精确重伸缩映射，并返回一个 InflightDataGateOrPartitionRescalingDescriptor 数组
        InflightDataGateOrPartitionRescalingDescriptor[] gateOrPartitionDescriptors =
                createGateOrPartitionRescalingDescriptors(
                        instanceID,
                        connectedAssignments,
                        assignment -> mappingRetriever.apply(assignment, true),
                        subtaskGateOrPartitionMappings,
                        subtaskMappingCalculator,
                        rescaledChannelsMappings);
        // 检查上一步计算出的所有输入门/输出分区的描述符是否全部是恒等映射
        if (Arrays.stream(gateOrPartitionDescriptors)
                .allMatch(InflightDataGateOrPartitionRescalingDescriptor::isIdentity)) {
            return log(InflightDataRescalingDescriptor.NO_RESCALE, instanceID.getSubtaskId());
        } else {
            return log(
                    new InflightDataRescalingDescriptor(gateOrPartitionDescriptors),
                    instanceID.getSubtaskId());
        }
    }

    private InflightDataGateOrPartitionRescalingDescriptor[]
            createGateOrPartitionRescalingDescriptors(
                    OperatorInstanceID instanceID,
                    TaskStateAssignment[] connectedAssignments,
                    Function<TaskStateAssignment, SubtasksRescaleMapping> mappingCalculator,
                    Map<Integer, SubtasksRescaleMapping> subtaskGateOrPartitionMappings,
                    Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator,
                    SubtasksRescaleMapping[] rescaledChannelsMappings) {
        return IntStream.range(0, rescaledChannelsMappings.length)
                .mapToObj(
                        partition -> {
                            TaskStateAssignment connectedAssignment =
                                    connectedAssignments[partition];
                            SubtasksRescaleMapping rescaleMapping =
                                    Optional.ofNullable(rescaledChannelsMappings[partition])
                                            .orElseGet(
                                                    () ->
                                                            mappingCalculator.apply(
                                                                    connectedAssignment));
                            SubtasksRescaleMapping subtaskMapping =
                                    Optional.ofNullable(
                                                    subtaskGateOrPartitionMappings.get(partition))
                                            .orElseGet(
                                                    () ->
                                                            subtaskMappingCalculator.apply(
                                                                    partition));
                            return getInflightDataGateOrPartitionRescalingDescriptor(
                                    instanceID, partition, rescaleMapping, subtaskMapping);
                        })
                .toArray(InflightDataGateOrPartitionRescalingDescriptor[]::new);
    }

    private InflightDataGateOrPartitionRescalingDescriptor
            getInflightDataGateOrPartitionRescalingDescriptor(
                    OperatorInstanceID instanceID,
                    int partition,
                    SubtasksRescaleMapping rescaleMapping,
                    SubtasksRescaleMapping subtaskMapping) {

        int[] oldSubtaskInstances =
                subtaskMapping.rescaleMappings.getMappedIndexes(instanceID.getSubtaskId());

        // no scaling or simple scale-up without the need of virtual
        // channels.
        boolean isIdentity =
                (subtaskMapping.rescaleMappings.isIdentity()
                                && rescaleMapping.getRescaleMappings().isIdentity())
                        || oldSubtaskInstances.length == 0;

        final Set<Integer> ambiguousSubtasks =
                subtaskMapping.mayHaveAmbiguousSubtasks
                        ? subtaskMapping.rescaleMappings.getAmbiguousTargets()
                        : emptySet();
        return log(
                new InflightDataGateOrPartitionRescalingDescriptor(
                        oldSubtaskInstances,
                        rescaleMapping.getRescaleMappings(),
                        ambiguousSubtasks,
                        isIdentity ? MappingType.IDENTITY : MappingType.RESCALING),
                instanceID.getSubtaskId(),
                partition);
    }
    // 从一个存储了重分区后状态句柄的 Map 中，
    // 根据特定的 Operator 实例 ID 查找并提取该实例对应的状态句柄集合，并将其封装成 Flink 内部使用的 StateObjectCollection 格式。
    private <T extends StateObject> StateObjectCollection<T> getState(
            OperatorInstanceID instanceID,
            Map<OperatorInstanceID, List<T>> subManagedOperatorState) {
        List<T> value = subManagedOperatorState.get(instanceID);
        return value != null ? new StateObjectCollection<>(value) : StateObjectCollection.empty();
    }

    private SubtasksRescaleMapping getOutputMapping(int assignmentIndex, boolean recompute) {
        SubtasksRescaleMapping mapping = outputSubtaskMappings.get(assignmentIndex);
        if (recompute && mapping == null) {
            return getOutputMapping(assignmentIndex);
        } else {
            return mapping;
        }
    }

    private SubtasksRescaleMapping getInputMapping(int assignmentIndex, boolean recompute) {
        SubtasksRescaleMapping mapping = inputSubtaskMappings.get(assignmentIndex);
        if (recompute && mapping == null) {
            return getInputMapping(assignmentIndex);
        } else {
            return mapping;
        }
    }

    public SubtasksRescaleMapping getOutputMapping(int partitionIndex) {
        final TaskStateAssignment downstreamAssignment = getDownstreamAssignments()[partitionIndex];
        final IntermediateResult output = executionJobVertex.getProducedDataSets()[partitionIndex];
        final int gateIndex = downstreamAssignment.executionJobVertex.getInputs().indexOf(output);

        final SubtaskStateMapper mapper =
                checkNotNull(
                        downstreamAssignment
                                .executionJobVertex
                                .getJobVertex()
                                .getInputs()
                                .get(gateIndex)
                                .getUpstreamSubtaskStateMapper(),
                        "No channel rescaler found during rescaling of channel state");
        final RescaleMappings mapping =
                mapper.getNewToOldSubtasksMapping(
                        oldState.get(outputOperatorID).getParallelism(), newParallelism);
        return outputSubtaskMappings.compute(
                partitionIndex,
                (idx, oldMapping) ->
                        checkSubtaskMapping(oldMapping, mapping, mapper.isAmbiguous()));
    }

    public SubtasksRescaleMapping getInputMapping(int gateIndex) {
        final SubtaskStateMapper mapper =
                checkNotNull(
                        executionJobVertex
                                .getJobVertex()
                                .getInputs()
                                .get(gateIndex)
                                .getDownstreamSubtaskStateMapper(),
                        "No channel rescaler found during rescaling of channel state");
        final RescaleMappings mapping =
                mapper.getNewToOldSubtasksMapping(
                        oldState.get(inputOperatorID).getParallelism(), newParallelism);

        return inputSubtaskMappings.compute(
                gateIndex,
                (idx, oldMapping) ->
                        checkSubtaskMapping(oldMapping, mapping, mapper.isAmbiguous()));
    }

    @Override
    public String toString() {
        return "TaskStateAssignment for " + executionJobVertex.getName();
    }

    private static @Nonnull SubtasksRescaleMapping checkSubtaskMapping(
            @Nullable SubtasksRescaleMapping oldMapping,
            RescaleMappings mapping,
            boolean mayHaveAmbiguousSubtasks) {
        if (oldMapping == null) {
            return new SubtasksRescaleMapping(mapping, mayHaveAmbiguousSubtasks);
        }
        if (!oldMapping.rescaleMappings.equals(mapping)) {
            throw new IllegalStateException(
                    "Incompatible subtask mappings: are multiple operators "
                            + "ingesting/producing intermediate results with varying degrees of parallelism?"
                            + "Found "
                            + oldMapping
                            + " and "
                            + mapping
                            + ".");
        }
        return new SubtasksRescaleMapping(
                mapping, oldMapping.mayHaveAmbiguousSubtasks || mayHaveAmbiguousSubtasks);
    }
    // 任务的并行度发生变化时，旧的子任务与新的子任务是如何对应和划分状态的。
    static class SubtasksRescaleMapping {
        // 核心映射对象。 存储了新旧子任务索引之间精确的一对一或多对多映射关系。它是实现状态和数据重伸缩的依据。
        private final RescaleMappings rescaleMappings;
        /**
         * If channel data cannot be safely divided into subtasks (several new subtask indexes are
         * associated with the same old subtask index). Mostly used for range partitioners.
         */
        // 模糊子任务标志。
        // 标记通道数据是否无法安全地划分给新的子任务。
        private final boolean mayHaveAmbiguousSubtasks;

        private SubtasksRescaleMapping(
                RescaleMappings rescaleMappings, boolean mayHaveAmbiguousSubtasks) {
            this.rescaleMappings = rescaleMappings;
            this.mayHaveAmbiguousSubtasks = mayHaveAmbiguousSubtasks;
        }

        public RescaleMappings getRescaleMappings() {
            return rescaleMappings;
        }

        public boolean isMayHaveAmbiguousSubtasks() {
            return mayHaveAmbiguousSubtasks;
        }
    }
}
