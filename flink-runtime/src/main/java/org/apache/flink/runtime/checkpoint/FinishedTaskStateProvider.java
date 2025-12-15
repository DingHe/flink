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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;

/** Collects and fulfills the finished state for the subtasks or operators. */
// 在 Flink 作业恢复或重新启动的过程中，收集并处理**已完成（Finished）**的子任务（Subtask）或算子（Operator）的状态信息。
// 报告哪些任务在恢复时或运行时已完成其工作。
// 标记这些已完成的任务或算子，确保它们在检查点状态中被正确标识为 Finished。
public interface FinishedTaskStateProvider {

    /** Reports the {@code task} is finished on restoring. */
    // 告任务在恢复时已完成。
    // 当 Flink Job 从检查点恢复时，如果检测到某个 ExecutionVertex（即 Task/Subtask）在之前的 Job 运行中已经完成了它的工作（例如，输入数据源耗尽），
    // JobManager 会调用此方法报告该任务已完成。
    void reportTaskFinishedOnRestore(ExecutionVertex task);

    /** Reports the {@code task} has finished all the operators. */
    // 报告任务中所有算子已完成。
    // 当一个运行中的 ExecutionVertex（即 Task/Subtask）通知 JobManager 它已经完成了其包含的所有算子的执行时，会调用此方法。
    void reportTaskHasFinishedOperators(ExecutionVertex task);

    /** Fulfills the state for the finished subtasks and operators to indicate they are finished. */
    // 填充已完成任务状态。 这是核心方法。
    // 它接收 Job 恢复或触发检查点时的算子状态图 (operatorStates)，并负责遍历这些状态，为那些已经被标记为“已完成”的子任务或算子填充 (Fulfill) 它们的状态
    void fulfillFinishedTaskStatus(Map<OperatorID, OperatorState> operatorStates)
            throws PartialFinishingNotSupportedByStateException;

    /**
     * Thrown when some subtasks of the operator have been finished but state doesn't support that
     * (e.g. Union).
     */
    class PartialFinishingNotSupportedByStateException extends FlinkRuntimeException {

        public PartialFinishingNotSupportedByStateException(String message) {
            super(message);
        }

        public PartialFinishingNotSupportedByStateException(Throwable cause) {
            super(cause);
        }

        public PartialFinishingNotSupportedByStateException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
