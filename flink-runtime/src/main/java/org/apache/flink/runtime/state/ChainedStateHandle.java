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

package org.apache.flink.runtime.state;

import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/** Handle to state handles for the operators in an operator chain. */
// ChainedStateHandle<T> 是 Flink 状态管理中的一个特殊的聚合状态句柄。
// 它的主要作用是将一个算子链 (Operator Chain) 中所有算子的状态句柄集合在一起，作为一个单一的逻辑状态对象来处理。
// 算子链状态封装： 在 Flink 中，当多个算子被优化成一个算子链并在同一个 Task 中执行时，它们的状态快照也是一起发生的。ChainedStateHandle 用于将这个链中所有算子的 StateObject (如 OperatorStateHandle 或 KeyedStateHandle) 按顺序打包到一个列表中。
// 简化 Task 状态报告： TaskManager 在完成检查点后，不必为链中的每个算子单独报告状态句柄，只需报告一个 ChainedStateHandle，极大地简化了检查点元数据的结构。
// 统一生命周期管理： 实现了 StateObject 接口，使得 JobMaster 在决定废弃（discardState()）或计算大小（getStateSize()）时，可以对整个算子链的状态进行批量处理。

public class ChainedStateHandle<T extends StateObject> implements StateObject {

    private static final long serialVersionUID = 1L;

    /** The state handles for all operators in the chain */
    // 算子状态句柄列表。
    // 存储了算子链中每个算子的状态句柄列表。
    // 列表的顺序与算子链的顺序一致。
    private final List<? extends T> operatorStateHandles;

    /**
     * Wraps a list to the state handles for the operators in a chain. Individual state handles can
     * be null.
     *
     * @param operatorStateHandles list with the state handles for the states in the operator chain.
     */
    public ChainedStateHandle(List<? extends T> operatorStateHandles) {
        this.operatorStateHandles = Preconditions.checkNotNull(operatorStateHandles);
    }

    /**
     * Check if there are any states handles present. Notice that this can be true even if {@link
     * #getLength()} is greater than zero, because state handles can be null.
     *
     * @return true if there are no state handles for any operator.
     */
    public boolean isEmpty() {
        for (T state : operatorStateHandles) {
            if (state != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the length of the operator chain. This can be different from the number of operator
     * state handles, because the some operators in the chain can have no state and thus their state
     * handle can be null.
     *
     * @return length of the operator chain
     */
    public int getLength() {
        return operatorStateHandles.size();
    }

    /**
     * Get the state handle for a single operator in the operator chain by it's index.
     *
     * @param index the index in the operator chain
     * @return state handle to the operator at the given position in the operator chain. can be
     *     null.
     */
    public T get(int index) {
        return operatorStateHandles.get(index);
    }

    @Override
    public void discardState() throws Exception {
        StateUtil.bestEffortDiscardAllStateObjects(operatorStateHandles);
    }

    @Override
    public long getStateSize() {
        return streamInternalHandles().mapToLong(StateObject::getStateSize).sum();
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        streamInternalHandles().forEach(handle -> handle.collectSizeStats(collector));
    }

    private Stream<? extends T> streamInternalHandles() {
        if (operatorStateHandles.isEmpty()) {
            return Stream.empty();
        }
        return operatorStateHandles.stream().filter(Objects::nonNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChainedStateHandle<?> that = (ChainedStateHandle<?>) o;

        return operatorStateHandles.equals(that.operatorStateHandles);
    }

    @Override
    public int hashCode() {
        return operatorStateHandles.hashCode();
    }

    public static <T extends StateObject> ChainedStateHandle<T> wrapSingleHandle(
            T stateHandleToWrap) {
        return new ChainedStateHandle<T>(Collections.singletonList(stateHandleToWrap));
    }

    public static boolean isNullOrEmpty(ChainedStateHandle<?> chainedStateHandle) {
        return chainedStateHandle == null || chainedStateHandle.isEmpty();
    }
}
