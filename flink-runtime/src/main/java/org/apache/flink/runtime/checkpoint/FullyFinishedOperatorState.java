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
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import javax.annotation.Nullable;

/**
 * A special operator state implementation representing the operators whose instances are all
 * finished.
 */
// FullyFinishedOperatorState 是 OperatorState 的一个特殊子类，
// 专门用于表示一个逻辑 Operator (操作符) 在检查点发生时，其所有并行子任务都已完成执行并退出。
// 在 Flink 批处理作业（Batch Jobs）中，当一个 Operator 的所有实例都处理完所有输入数据并正常结束时，它被称为“已完成”（Finished）。
// 标记 Operator 终止状态: 它作为元数据存储在检查点中，明确地告诉系统：“这个 Operator 已经结束了，它不应该再有运行时状态。”
// 阻止状态写入: 它通过重写 putState 和 setCoordinatorState 方法并抛出 UnsupportedOperationException，强制确保已完成的 Operator 不能再接收任何子任务或协调者状态。
public class FullyFinishedOperatorState extends OperatorState {

    private static final long serialVersionUID = 1L;

    public FullyFinishedOperatorState(OperatorID operatorID, int parallelism, int maxParallelism) {
        super(operatorID, parallelism, maxParallelism);
    }

    @Override
    public boolean isFullyFinished() {
        return true;
    }

    @Override
    public void putState(int subtaskIndex, OperatorSubtaskState subtaskState) {
        throw new UnsupportedOperationException(
                "Could not put state to a fully finished operator state.");
    }

    @Override
    public void setCoordinatorState(@Nullable ByteStreamStateHandle coordinatorState) {
        throw new UnsupportedOperationException(
                "Could not set coordinator state to a fully finished operator state.");
    }

    @Override
    public OperatorState copyAndDiscardInFlightData() {
        return new FullyFinishedOperatorState(
                getOperatorID(), getParallelism(), getMaxParallelism());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FullyFinishedOperatorState) {
            return super.equals(obj);
        }

        return false;
    }

    @Override
    public String toString() {
        return "FullyFinishedOperatorState("
                + "operatorID: "
                + getOperatorID()
                + ", parallelism: "
                + getParallelism()
                + ", maxParallelism: "
                + getMaxParallelism()
                + ')';
    }
}
