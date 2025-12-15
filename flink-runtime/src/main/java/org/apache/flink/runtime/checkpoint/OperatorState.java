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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Simple container class which contains the raw/managed operator state and key-group state handles
 * from all subtasks of an operator and therefore represents the complete state of a logical
 * operator.
 */
// OperatorState 类的作用是作为一个逻辑 Operator (操作符)在某个检查点时刻的完整状态表示。
// 在 Flink 中，一个逻辑 Operator (例如 SourceFunction) 可能被分配到多个并行子任务 (Subtasks) 上执行。OperatorState 的职责就是聚合：
// 该 Operator 所有并行子任务（Subtasks）的状态句柄 (OperatorSubtaskState）
// 该 Operator 的 Operator Coordinator（如果存在）的状态句柄。
// 它实现了 CompositeStateHandle 接口，意味着它是一个可以包含其他状态句柄的复合状态句柄。它存储了恢复 Operator 状态所需的所有元数据，是 Flink 实现无缝并行度伸缩 (Rescaling) 和故障恢复的基础
public class OperatorState implements CompositeStateHandle {

    private static final long serialVersionUID = -4845578005863201810L;

    /** The id of the operator. */
    // Operator 的唯一逻辑标识符。
    // 用于在 Job 图中识别这个状态属于哪个操作符。
    private final OperatorID operatorID;

    /** The handles to states created by the parallel tasks: subtaskIndex -> subtaskstate. */
    // 子任务状态的集合。 这是一个 Map，键是子任务索引（Integer，从 0 到 parallelism-1），
    // 值是该子任务捕获到的状态句柄 (OperatorSubtaskState)。
    // 它包含了所有并行子任务的状态。
    private final Map<Integer, OperatorSubtaskState> operatorSubtaskStates;

    /** The state of the operator coordinator. Null, if no such state exists. */
    // Operator Coordinator 的状态句柄。
    // 如果该 Operator 拥有一个 Operator Coordinator（例如 Kafka Source），
    // 其状态会被序列化成字节流 (ByteStreamStateHandle) 并存储在这里。如果不存在协调者状态，则为 null。
    @Nullable private ByteStreamStateHandle coordinatorState;

    /** The parallelism of the operator when it was checkpointed. */
    // 检查点发生时的 Operator 并行度。
    // 即该 Operator 拥有的子任务数量。
    private final int parallelism;

    /**
     * The maximum parallelism (for number of KeyGroups) of the operator when the job was first
     * created.
     */
    // Operator 的最大并行度。
    // 对应于 Key Group 的数量。它是一个关键元数据，用于在恢复时正确地重新分配键控状态（Keyed State）。
    private final int maxParallelism;

    public OperatorState(OperatorID operatorID, int parallelism, int maxParallelism) {
        if (parallelism > maxParallelism) {
            throw new IllegalArgumentException(
                    String.format(
                            "Parallelism %s is not smaller or equal to max parallelism %s.",
                            parallelism, maxParallelism));
        }

        this.operatorID = operatorID;

        this.operatorSubtaskStates = CollectionUtil.newHashMapWithExpectedSize(parallelism);

        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
    }

    public OperatorID getOperatorID() {
        return operatorID;
    }
    // 检查 Operator 是否已完全完成。
    // 当前实现总是返回 false。
    // 该方法在未来的 Flink 版本中可能用于支持部分完成/清理的场景，但在提供的代码中没有实现完整逻辑。
    public boolean isFullyFinished() {
        return false;
    }
    // 添加单个子任务的状态
    public void putState(int subtaskIndex, OperatorSubtaskState subtaskState) {
        Preconditions.checkNotNull(subtaskState);

        if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IndexOutOfBoundsException(
                    "The given sub task index "
                            + subtaskIndex
                            + " exceeds the maximum number of sub tasks "
                            + operatorSubtaskStates.size());
        } else {
            operatorSubtaskStates.put(subtaskIndex, subtaskState);
        }
    }
    // 获取单个子任务的状态
    public OperatorSubtaskState getState(int subtaskIndex) {
        if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IndexOutOfBoundsException(
                    "The given sub task index "
                            + subtaskIndex
                            + " exceeds the maximum number of sub tasks "
                            + operatorSubtaskStates.size());
        } else {
            return operatorSubtaskStates.get(subtaskIndex);
        }
    }
    // 设置 Operator Coordinator 的状态。 用于将协调者状态句柄设置给属性 coordinatorState。
    // 它通过 checkState 保证协调者状态只被设置一次。
    public void setCoordinatorState(@Nullable ByteStreamStateHandle coordinatorState) {
        checkState(this.coordinatorState == null, "coordinator state already set");
        this.coordinatorState = coordinatorState;
    }
    // 获取 Operator Coordinator 的状态句柄，如果不存在则返回 null。
    @Nullable
    public ByteStreamStateHandle getCoordinatorState() {
        return coordinatorState;
    }
    // 返回所有子任务状态的 Map，返回的是不可修改的视图，以保护内部状态。
    public Map<Integer, OperatorSubtaskState> getSubtaskStates() {
        return Collections.unmodifiableMap(operatorSubtaskStates);
    }
    // 返回所有子任务状态的集合（Map 的值集合）。
    public Collection<OperatorSubtaskState> getStates() {
        return operatorSubtaskStates.values();
    }

    public int getNumberCollectedStates() {
        return operatorSubtaskStates.size();
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }
    // 复制方法。 创建一个新的 OperatorState 实例，保留所有子任务状态和元数据，但使用一个新的 OperatorID。
    // 这在某些内部重映射场景中可能会用到。
    public OperatorState copyWithNewOperatorID(OperatorID newOperatorId) {
        OperatorState newState = new OperatorState(newOperatorId, parallelism, maxParallelism);
        operatorSubtaskStates.forEach(newState::putState);
        return newState;
    }
    // 创建丢弃传输中数据的副本
    public OperatorState copyAndDiscardInFlightData() {
        OperatorState newState = new OperatorState(operatorID, parallelism, maxParallelism);

        for (Map.Entry<Integer, OperatorSubtaskState> originalSubtaskStateEntry :
                operatorSubtaskStates.entrySet()) {
            newState.putState(
                    originalSubtaskStateEntry.getKey(),
                    // 其中每个子任务状态都通过 toBuilder() 复制，
                    // 并明确将输入通道状态 (InputChannelState) 和结果子分区状态 (ResultSubpartitionState) 设置为空
                    originalSubtaskStateEntry
                            .getValue()
                            .toBuilder()
                            .setResultSubpartitionState(StateObjectCollection.empty())
                            .setInputChannelState(StateObjectCollection.empty())
                            .build());
        }

        return newState;
    }
    // 获取所有可丢弃的状态句柄。
    // 收集所有子任务状态中包含的以及 coordinatorState 中包含的所有可以被丢弃（清理）的原始状态句柄，用于后续的资源清理。
    public List<StateObject> getDiscardables() {
        List<StateObject> toDispose =
                operatorSubtaskStates.values().stream()
                        .flatMap(op -> op.getDiscardables().stream())
                        .collect(Collectors.toList());

        if (coordinatorState != null) {
            toDispose.add(coordinatorState);
        }
        return toDispose;
    }
    // 清理状态。
    // 实现 CompositeStateHandle 接口的方法。
    // 遍历所有子任务状态和协调者状态，调用它们的 discardState() 方法，执行状态文件的物理清理和资源释放
    @Override
    public void discardState() throws Exception {
        for (OperatorSubtaskState operatorSubtaskState : operatorSubtaskStates.values()) {
            operatorSubtaskState.discardState();
        }

        if (coordinatorState != null) {
            coordinatorState.discardState();
        }
    }
    // 注册共享状态
    @Override
    public void registerSharedStates(SharedStateRegistry sharedStateRegistry, long checkpointID) {
        for (OperatorSubtaskState operatorSubtaskState : operatorSubtaskStates.values()) {
            operatorSubtaskState.registerSharedStates(sharedStateRegistry, checkpointID);
        }
    }

    public boolean hasSubtaskStates() {
        return operatorSubtaskStates.size() > 0;
    }

    @Override
    public long getStateSize() {
        return streamAllSubHandles().mapToLong(StateObject::getStateSize).sum();
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        streamAllSubHandles().forEach(handle -> handle.collectSizeStats(collector));
    }

    private Stream<StateObject> streamAllSubHandles() {
        return Stream.concat(Stream.of(coordinatorState), operatorSubtaskStates.values().stream())
                .filter(Objects::nonNull);
    }

    @Override
    public long getCheckpointedSize() {
        long result = coordinatorState == null ? 0L : coordinatorState.getStateSize();

        for (int i = 0; i < parallelism; i++) {
            OperatorSubtaskState operatorSubtaskState = operatorSubtaskStates.get(i);
            if (operatorSubtaskState != null) {
                result += operatorSubtaskState.getCheckpointedSize();
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OperatorState) {
            OperatorState other = (OperatorState) obj;

            return operatorID.equals(other.operatorID)
                    && parallelism == other.parallelism
                    && Objects.equals(coordinatorState, other.coordinatorState)
                    && operatorSubtaskStates.equals(other.operatorSubtaskStates);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return parallelism + 31 * Objects.hash(operatorID, operatorSubtaskStates);
    }

    @Override
    public String toString() {
        // KvStates are always null in 1.1. Don't print this as it might
        // confuse users that don't care about how we store it internally.
        return "OperatorState("
                + "operatorID: "
                + operatorID
                + ", parallelism: "
                + parallelism
                + ", maxParallelism: "
                + maxParallelism
                + ", coordinatorState: "
                + (coordinatorState == null ? "(none)" : coordinatorState.getStateSize() + " bytes")
                + ", sub task states: "
                + operatorSubtaskStates.size()
                + ", total size (bytes): "
                + getStateSize()
                + ')';
    }
}
