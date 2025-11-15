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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/** Interface of a state handle for operator state. */
// 专门用于封装和引用 Flink 算子状态（Operator State）的持久化数据，并携带状态的元数据信息。
// 算子状态是与并行实例绑定的状态（例如 Source 算子的偏移量）。与键控状态（Keyed State）不同，算子状态的恢复和分配（Assignment）机制更为复杂，需要知道以下信息：
// 数据在哪里： 通过继承 StreamStateHandle 知道数据存储的位置。
// 数据如何分块： 知道同一个状态的多个分区数据在流中的起始和结束位置（即偏移量 offsets）。
// 数据如何分配： 知道在恢复时，这些状态分区应该以何种模式（SPLIT_DISTRIBUTE, UNION, BROADCAST）重新分配给新的并行任务。
public interface OperatorStateHandle extends StreamStateHandle {

    /** Returns a map of meta data for all contained states by their name. */
    // 获取状态名称到元数据的映射
    // 键是用户为算子状态指定的名称（例如 "offset_state"），值是该状态对应的 StateMetaInfo 对象（包含偏移量和分配模式）。
    Map<String, StateMetaInfo> getStateNameToPartitionOffsets();

    /** Returns an input stream to read the operator state information. */
    @Override
    FSDataInputStream openInputStream() throws IOException;

    /** Returns the underlying stream state handle that points to the state data. */
    // 获取底层委托句柄。
    // 返回实际存储状态数据的那个 StreamStateHandle 实例（即该 OperatorStateHandle 委托其进行 I/O 操作的对象）。
    // 这个底层句柄包含了文件路径或存储位置的详细信息。
    StreamStateHandle getDelegateStateHandle();

    /**
     * The modes that determine how an {@link OperatorStreamStateHandle} is assigned to tasks during
     * restore.
     */
    // 定义了在恢复检查点时，算子状态分区如何从 State Handle 分配给新的并行任务。
    // 这对于支持 Flink 的运行时弹性伸缩（Rescaling）至关重要
    enum Mode {
        // 拆分分配模式。
        // 状态句柄中的状态分区是独立的。在恢复时，Flink 会将这些分区拆分（Split）开，并尝试将每个分区分配给一个并行任务。这是最常见和默认的模式，支持并行度增减。
        SPLIT_DISTRIBUTE, // The operator state partitions in the state handle are split and
        // distributed to one task each.
        // 联合模式
        // 在恢复时，来自所有旧并行任务的整个状态列表会被合并（Union）成一个单一列表，然后将这个完整的列表发送给所有新的并行任务。
        UNION, // The operator state partitions are UNION-ed upon restoring and sent to all tasks.
        // 广播模式。
        // 态数据在所有并行任务中是相同的。在恢复时，状态只读取一次，并被广播给所有新的并行任务。通常用于源头算子（Source）中状态（例如 Source Context 状态）
        BROADCAST // The operator states are identical, as the state is produced from a broadcast
        // stream.
    }

    /** Meta information about the operator state handle. */
    // 装了单个具名（Named）算子状态的元数据信息
    class StateMetaInfo implements Serializable {

        private static final long serialVersionUID = 3593817615858941166L;
        // 分区数据偏移量。
        // 这是一个 long 数组，存储了该具名状态的各个分区数据在底层流（由 StreamStateHandle 引用）中的起始位置和结束位置。
        // 这使得 Flink 可以只读取流中特定的状态块。
        private final long[] offsets;
        private final Mode distributionMode;

        public StateMetaInfo(long[] offsets, Mode distributionMode) {
            this.offsets = Preconditions.checkNotNull(offsets);
            this.distributionMode = Preconditions.checkNotNull(distributionMode);
        }

        public long[] getOffsets() {
            return offsets;
        }

        public Mode getDistributionMode() {
            return distributionMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StateMetaInfo that = (StateMetaInfo) o;

            return Arrays.equals(getOffsets(), that.getOffsets())
                    && getDistributionMode() == that.getDistributionMode();
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(getOffsets());
            result = 31 * result + getDistributionMode().hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "StateMetaInfo{"
                    + "offsets="
                    + Arrays.toString(offsets)
                    + ", distributionMode="
                    + distributionMode
                    + '}';
        }
    }
}
