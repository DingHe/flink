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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava32.com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.NO_RESCALE;

/**
 * This class encapsulates state handles to the snapshots of all operator instances executed within
 * one task. A task can run multiple operator instances as a result of operator chaining, and all
 * operator instances from the chain can register their state under their operator id. Each operator
 * instance is a physical execution responsible for processing a partition of the data that goes
 * through a logical operator. This partitioning happens to parallelize execution of logical
 * operators, e.g. distributing a map function.
 *
 * <p>One instance of this class contains the information that one task will send to acknowledge a
 * checkpoint request by the checkpoint coordinator. Tasks run operator instances in parallel, so
 * the union of all {@link TaskStateSnapshot} that are collected by the checkpoint coordinator from
 * all tasks represent the whole state of a job at the time of the checkpoint.
 *
 * <p>This class should be called TaskState once the old class with this name that we keep for
 * backwards compatibility goes away.
 */
// Flink 中任务（Task）级别的状态快照 的封装类
// 封装任务状态： 封装一个 **TaskManager 上运行的单个物理任务（Task）**所包含的所有操作符实例（由于算子链，一个任务可能运行多个操作符）的状态句柄（State Handles）快照。
// 检查点确认信息： 它是 TaskManager 在完成检查点操作后，发送给 JobManager（检查点协调器）的确认信息的主体部分。
// 层次化结构： 提供了从整个任务级别的快照，到单个操作符子任务状态（OperatorSubtaskState）的映射，结构化地管理状态。
public class TaskStateSnapshot implements CompositeStateHandle {

    private static final long serialVersionUID = 1L;

    public static final TaskStateSnapshot FINISHED_ON_RESTORE =
            new TaskStateSnapshot(new HashMap<>(), true, true);

    /** Mapping from an operator id to the state of one subtask of this operator. */
    // 存储一个任务中所有操作符实例的子任务状态快照。键是操作符的唯一 ID (OperatorID)，值是该操作符的本地子任务状态 (OperatorSubtaskState)
    private final Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID;
    // 任务部署为完成。
    // true 表示这个任务在恢复时，其所有操作符就已经是逻辑上已完成或已终止的状态（例如，源头数据已耗尽）
    private final boolean isTaskDeployedAsFinished;
    // 任务已完成。
    // true 表示这个任务的所有操作符在本次检查点时已经调用了 finished 方法，处于终止状态
    private final boolean isTaskFinished;

    public TaskStateSnapshot() {
        this(10, false);
    }

    public TaskStateSnapshot(int size, boolean isTaskFinished) {
        this(CollectionUtil.newHashMapWithExpectedSize(size), false, isTaskFinished);
    }

    public TaskStateSnapshot(Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID) {
        this(subtaskStatesByOperatorID, false, false);
    }

    private TaskStateSnapshot(
            Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished) {
        this.subtaskStatesByOperatorID = Preconditions.checkNotNull(subtaskStatesByOperatorID);
        this.isTaskDeployedAsFinished = isTaskDeployedAsFinished;
        this.isTaskFinished = isTaskFinished;
    }

    /** Returns whether all the operators of the task are already finished on restoring. */
    public boolean isTaskDeployedAsFinished() {
        return isTaskDeployedAsFinished;
    }

    /** Returns whether all the operators of the task have called finished methods. */
    public boolean isTaskFinished() {
        return isTaskFinished;
    }

    /** Returns the subtask state for the given operator id (or null if not contained). */
    // 根据 OperatorID 返回对应操作符的子任务状态。如果不存在，则返回 null。
    @Nullable
    public OperatorSubtaskState getSubtaskStateByOperatorID(OperatorID operatorID) {
        return subtaskStatesByOperatorID.get(operatorID);
    }

    /**
     * Maps the given operator id to the given subtask state. Returns the subtask state of a
     * previous mapping, if such a mapping existed or null otherwise.
     */
    // 将给定的 OperatorID 映射到 OperatorSubtaskState。如果该 ID 之前存在映射，则返回旧的 OperatorSubtaskState。
    public OperatorSubtaskState putSubtaskStateByOperatorID(
            @Nonnull OperatorID operatorID, @Nonnull OperatorSubtaskState state) {

        return subtaskStatesByOperatorID.put(operatorID, Preconditions.checkNotNull(state));
    }

    /** Returns the set of all mappings from operator id to the corresponding subtask state. */
    // 返回内部 Map 的所有映射条目集合，方便遍历所有操作符子任务状态。
    public Set<Map.Entry<OperatorID, OperatorSubtaskState>> getSubtaskStateMappings() {
        return subtaskStatesByOperatorID.entrySet();
    }

    /**
     * Returns true if at least one {@link OperatorSubtaskState} in subtaskStatesByOperatorID has
     * state.
     */
    //是否至少存在一个状态
    public boolean hasState() {
        for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
            if (operatorSubtaskState != null && operatorSubtaskState.hasState()) {
                return true;
            }
        }
        return isTaskDeployedAsFinished;
    }

    /**
     * Returns the input channel mapping for rescaling with in-flight data or {@link
     * InflightDataRescalingDescriptor#NO_RESCALE}.
     */
    // 获取输入数据重缩放描述符。
    // 用于处理包含**飞行中数据（in-flight data）**的非对齐检查点在并行度变更时的重分配信息（Input Channel Mapping）
    public InflightDataRescalingDescriptor getInputRescalingDescriptor() {
        return getMapping(OperatorSubtaskState::getInputRescalingDescriptor);
    }

    /**
     * Returns the output channel mapping for rescaling with in-flight data or {@link
     * InflightDataRescalingDescriptor#NO_RESCALE}.
     */
    // 获取输出数据重缩放描述符。
    // 作用与输入类似，但针对的是输出数据重分配信息（Output Channel Mapping）
    public InflightDataRescalingDescriptor getOutputRescalingDescriptor() {
        return getMapping(OperatorSubtaskState::getOutputRescalingDescriptor);
    }
    // 弃状态。
    // 释放由快照持有的所有持久化状态（如文件句柄或 StateBackend 资源）。
    // 它通过调用 StateUtil.bestEffortDiscardAllStateObjects 尽力丢弃所有子状态。
    @Override
    public void discardState() throws Exception {
        StateUtil.bestEffortDiscardAllStateObjects(subtaskStatesByOperatorID.values());
    }
    // 获取状态总大小（已检查点）。
    // 计算并返回本次检查点中，所有操作符子任务状态的总大小。
    @Override
    public long getStateSize() {
        return streamOperatorSubtaskStates().mapToLong(StateObject::getStateSize).sum();
    }
    // 收集大小统计信息。
    // 递归地调用所有子状态的 collectSizeStats 方法，以便收集更细粒度的状态对象大小统计。
    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        streamOperatorSubtaskStates().forEach(oss -> oss.collectSizeStats(collector));
    }

    private Stream<OperatorSubtaskState> streamOperatorSubtaskStates() {
        return subtaskStatesByOperatorID.values().stream().filter(Objects::nonNull);
    }
    // 获取检查点总大小（已持久化）。
    // 计算并返回所有子状态已持久化到 Checkpoint 存储的总字节数。
    @Override
    public long getCheckpointedSize() {
        long size = 0L;

        for (OperatorSubtaskState subtaskState : subtaskStatesByOperatorID.values()) {
            if (subtaskState != null) {
                size += subtaskState.getCheckpointedSize();
            }
        }

        return size;
    }
    // 注册共享状态。
    // 遍历所有子状态，将其中的共享状态句柄注册到 SharedStateRegistry 中，确保共享状态能够被正确引用和计数。
    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
            if (operatorSubtaskState != null) {
                operatorSubtaskState.registerSharedStates(stateRegistry, checkpointID);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaskStateSnapshot that = (TaskStateSnapshot) o;

        return subtaskStatesByOperatorID.equals(that.subtaskStatesByOperatorID)
                && isTaskDeployedAsFinished == that.isTaskDeployedAsFinished
                && isTaskFinished == that.isTaskFinished;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskStatesByOperatorID, isTaskDeployedAsFinished, isTaskFinished);
    }

    @Override
    public String toString() {
        return "TaskOperatorSubtaskStates{"
                + "subtaskStatesByOperatorID="
                + subtaskStatesByOperatorID
                + ", isTaskDeployedAsFinished="
                + isTaskDeployedAsFinished
                + ", isTaskFinished="
                + isTaskFinished
                + '}';
    }

    /** Returns the only valid mapping as ensured by {@link StateAssignmentOperation}. */
    // 用于从所有 OperatorSubtaskState 中提取（并验证）唯一的、非 NO_RESCALE 的重缩放描述符。Flink 假设在一个任务链中，所有操作符的重缩放信息应该是一致的。
    private InflightDataRescalingDescriptor getMapping(
            Function<OperatorSubtaskState, InflightDataRescalingDescriptor> mappingExtractor) {
        return Iterators.getOnlyElement(
                subtaskStatesByOperatorID.values().stream()
                        .map(mappingExtractor)
                        .filter(mapping -> !mapping.equals(NO_RESCALE))
                        .iterator(),
                NO_RESCALE);
    }
    // 序列化辅助方法。
    // 将 TaskStateSnapshot 对象封装到 SerializedValue 中，便于在 Flink 内部进行高效传输。
    @Nullable
    public static SerializedValue<TaskStateSnapshot> serializeTaskStateSnapshot(
            TaskStateSnapshot subtaskState) {
        try {
            return subtaskState == null ? null : new SerializedValue<>(subtaskState);
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }
    // 反序列化辅助方法。
    // 从 SerializedValue 中反序列化出 TaskStateSnapshot 对象。
    @Nullable
    public static TaskStateSnapshot deserializeTaskStateSnapshot(
            SerializedValue<TaskStateSnapshot> subtaskState, ClassLoader classLoader) {
        try {
            return subtaskState == null ? null : subtaskState.deserializeValue(classLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
