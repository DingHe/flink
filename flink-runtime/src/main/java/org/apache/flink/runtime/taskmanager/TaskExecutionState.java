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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.util.SerializedThrowable;

import java.io.Serializable;

/**
 * This class represents an update about a task's execution state.
 *
 * <p><b>NOTE:</b> The exception that may be attached to the state update is not necessarily a Flink
 * or core Java exception, but may be an exception from the user code. As such, it cannot be
 * deserialized without a special class loader. For that reason, the class keeps the actual
 * exception field transient and deserialized it lazily, with the appropriate class loader.
 */
// 想象一下 Flink 的架构：TaskManager 负责干活（执行 Task），JobMaster 负责指挥（调度）。
// 当 TaskManager 上的一个任务状态发生变化（比如任务跑完了，或者报错挂了），它必须向 JobMaster 汇报。TaskExecutionState 就是这份“汇报单”。
// TaskExecutionState 是一个数据传输对象（DTO），用于在 TaskManager 和 JobMaster 之间传递关于任务执行状态的更新信息。
// 状态汇报：它封装了一个任务（Task）当前的执行状态（如 RUNNING、FINISHED、FAILED）
// 异常携带：如果任务失败，它会携带导致失败的异常信息（Throwable）
// 结果统计：在任务结束时，它会携带任务执行期间产生的累加器（Accumulators）数据和 I/O 指标（IOMetrics），供 JobMaster 统计和展示。
// 序列化与类加载隔离：它特殊处理了异常对象。因为用户的代码可能会抛出自定义异常，而 JobMaster 的系统类加载器可能无法识别这些用户定义的类。
// 因此，这个类将异常包装为 SerializedThrowable，直到需要时再用正确的用户类加载器去反序列化它。
public class TaskExecutionState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ExecutionAttemptID executionId;

    private final ExecutionState executionState;

    private final SerializedThrowable throwable;

    /** Serialized user-defined accumulators */
    private final AccumulatorSnapshot accumulators;

    private final IOMetrics ioMetrics;

    /**
     * Creates a new task execution state update, with no attached exception and no accumulators.
     *
     * @param executionId the ID of the task execution whose state is to be reported
     * @param executionState the execution state to be reported
     */
    public TaskExecutionState(ExecutionAttemptID executionId, ExecutionState executionState) {
        this(executionId, executionState, null, null, null);
    }

    /**
     * Creates a new task execution state update, with an attached exception but no accumulators.
     *
     * @param executionId the ID of the task execution whose state is to be reported
     * @param executionState the execution state to be reported
     */
    public TaskExecutionState(
            ExecutionAttemptID executionId, ExecutionState executionState, Throwable error) {
        this(executionId, executionState, error, null, null);
    }

    /**
     * Creates a new task execution state update, with an attached exception. This constructor may
     * never throw an exception.
     *
     * @param executionId the ID of the task execution whose state is to be reported
     * @param executionState the execution state to be reported
     * @param error an optional error
     * @param accumulators The flink and user-defined accumulators which may be null.
     */
    public TaskExecutionState(
            ExecutionAttemptID executionId,
            ExecutionState executionState,
            Throwable error,
            AccumulatorSnapshot accumulators,
            IOMetrics ioMetrics) {

        if (executionId == null || executionState == null) {
            throw new NullPointerException();
        }

        this.executionId = executionId;
        this.executionState = executionState;
        if (error != null) {
            this.throwable = new SerializedThrowable(error);
        } else {
            this.throwable = null;
        }
        this.accumulators = accumulators;
        this.ioMetrics = ioMetrics;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the attached exception, which is in serialized form. Returns null, if the status update
     * is no failure with an associated exception.
     *
     * @param userCodeClassloader The classloader that can resolve user-defined exceptions.
     * @return The attached exception, or null, if none.
     */
    public Throwable getError(ClassLoader userCodeClassloader) {
        if (this.throwable == null) {
            return null;
        } else {
            return this.throwable.deserializeError(userCodeClassloader);
        }
    }

    /**
     * Returns the ID of the task this result belongs to
     *
     * @return the ID of the task this result belongs to
     */
    public ExecutionAttemptID getID() {
        return this.executionId;
    }

    /**
     * Returns the new execution state of the task.
     *
     * @return the new execution state of the task
     */
    public ExecutionState getExecutionState() {
        return this.executionState;
    }

    /** Gets flink and user-defined accumulators in serialized form. */
    public AccumulatorSnapshot getAccumulators() {
        return accumulators;
    }

    public IOMetrics getIOMetrics() {
        return ioMetrics;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TaskExecutionState) {
            TaskExecutionState other = (TaskExecutionState) obj;
            return other.executionId.equals(this.executionId)
                    && other.executionState == this.executionState
                    && (other.throwable == null) == (this.throwable == null);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return executionId.hashCode() + executionState.ordinal();
    }

    @Override
    public String toString() {
        return String.format(
                "TaskExecutionState executionId=%s, state=%s, error=%s",
                executionId, executionState, throwable == null ? "(null)" : throwable.toString());
    }
}
