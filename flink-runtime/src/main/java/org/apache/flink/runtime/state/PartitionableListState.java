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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of operator list state.
 *
 * @param <S> the type of an operator state partition.
 */
// 核心作用是实现 Flink 的 ListState 接口，专门用于存储 算子状态 (Operator State)。
// 算子状态实现： 它存储的数据与 Key 无关，而是属于整个算子实例。当 Flink Job 的并行度发生变化时（例如，从 4 个实例变成 8 个实例），
// 该状态中的每个元素（称为分区）都可以在新的算子实例之间重新分配，以确保状态的均匀负载和正确恢复
// 内存存储： 状态数据以 ArrayList 的形式存储在 JVM 堆内存中，提供快速的访问速度。
// 持久化与序列化： 它包含用于将状态内容（列表中的每个元素）序列化到外部存储（如 Checkpoint）的逻辑。在序列化时，每个元素都会被独立写入，并记录其偏移量。
// 是一个基于内存的、可重新分配的列表，用于存储 Flink 算子的非 Keyed 状态。
public final class PartitionableListState<S> implements ListState<S> {

    /** Meta information of the state, including state name, assignment mode, and typeSerializer */
    // 状态的元数据信息。
    // 它包含状态的名称、分区元素 $S$ 的序列化器 (PartitionStateSerializer) 以及状态的分配模式（如均匀分配 EVEN_SPLIT）
    private RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo;

    /** The internal list the holds the elements of the state */
    // 内部列表。
    // 这是实际在内存中存储状态元素的地方。
    // 它是一个 ArrayList，保存了所有当前算子实例负责的状态分区元素。
    private final ArrayList<S> internalList;

    /** A typeSerializer that allows to perform deep copies of internalList */
    // 一个专门用于对 internalList 及其内容进行深度复制的序列化器。
    // 这主要用于在 Checkpoint 或 Savepoint 期间创建状态的不可变快照副本。
    private ArrayListSerializer<S> internalListCopySerializer;

    PartitionableListState(RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo) {
        this(stateMetaInfo, new ArrayList<S>());
    }

    private PartitionableListState(
            RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo, ArrayList<S> internalList) {

        this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
        this.internalList = Preconditions.checkNotNull(internalList);
        this.internalListCopySerializer =
                new ArrayListSerializer<>(stateMetaInfo.getPartitionStateSerializer());
    }

    private PartitionableListState(PartitionableListState<S> toCopy) {

        this(
                toCopy.stateMetaInfo.deepCopy(),
                toCopy.internalListCopySerializer.copy(toCopy.internalList));
    }

    public void setStateMetaInfo(RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo) {
        this.internalListCopySerializer =
                new ArrayListSerializer<>(stateMetaInfo.getPartitionStateSerializer());
        this.stateMetaInfo = stateMetaInfo;
    }

    public RegisteredOperatorStateBackendMetaInfo<S> getStateMetaInfo() {
        return stateMetaInfo;
    }

    public PartitionableListState<S> deepCopy() {
        return new PartitionableListState<>(this);
    }

    @Override
    public void clear() {
        internalList.clear();
    }
    // 获取状态
    // 返回内部列表 internalList，表示当前算子实例的所有状态分区元素
    @Override
    public Iterable<S> get() {
        return internalList;
    }
    // 添加单个元素：
    // 将一个元素 $S$ 追加到内部列表 internalList 中。执行非空检查。
    @Override
    public void add(S value) {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        internalList.add(value);
    }

    @Override
    public String toString() {
        return "PartitionableListState{"
                + "stateMetaInfo="
                + stateMetaInfo
                + ", internalList="
                + internalList
                + '}';
    }

    // 写入 Checkpoint：
    // 将内部列表中的所有元素序列化并写入到给定的输出流 (FSDataOutputStream) 中。
    // 这是 Checkpoint/Savepoint 机制的关键部分。
    public long[] write(FSDataOutputStream out) throws IOException {

        long[] partitionOffsets = new long[internalList.size()];

        DataOutputView dov = new DataOutputViewStreamWrapper(out);

        for (int i = 0; i < internalList.size(); ++i) {
            S element = internalList.get(i);
            partitionOffsets[i] = out.getPos();
            getStateMetaInfo().getPartitionStateSerializer().serialize(element, dov);
        }

        return partitionOffsets;
    }
    // 覆盖状态：
    // 首先调用 clear() 清空当前列表，然后调用 addAll() 将传入的新列表元素设置进来，实现状态替换。
    @Override
    public void update(List<S> values) {
        internalList.clear();

        addAll(values);
    }
    // 追加列表：
    // 将给定的 List<S> 中的所有元素逐个添加到内部列表 internalList 中。
    // 它会检查传入的列表本身和列表中的每个元素是否为 null。
    @Override
    public void addAll(List<S> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        if (!values.isEmpty()) {
            for (S value : values) {
                checkNotNull(value, "Any value to add to a list cannot be null.");
                add(value);
            }
        }
    }

    @VisibleForTesting
    public ArrayListSerializer<S> getInternalListCopySerializer() {
        return internalListCopySerializer;
    }
}
