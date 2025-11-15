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
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import javax.annotation.Nullable;

/**
 * An invokable part of the task.
 *
 * <p>The TaskManager first calls the {@link #restore} method when executing a task. If the call
 * succeeds and the task isn't cancelled then TM proceeds to {@link #invoke()}. All operations of
 * the task happen in these two methods (setting up input output stream readers and writers as well
 * as the task's core operation).
 *
 * <p>After that, {@link #cleanUp(Throwable)} is called (regardless of an failures or cancellations
 * during the above calls).
 *
 * <p>Implementations must have a constructor with a single argument of type {@link
 * org.apache.flink.runtime.execution.Environment}.
 *
 * <p><i>Developer note: While constructors cannot be enforced at compile time, we did not yet
 * venture on the endeavor of introducing factories (it is only an internal API after all, and with
 * Java 8, one can use {@code Class::new} almost like a factory lambda.</i>
 *
 * @see CheckpointableTask
 * @see CoordinatedTask
 * @see AbstractInvokable
 */
// TaskInvokable（任务可调用对象）是 Flink 用户代码逻辑的容器和执行引擎。在 Flink 运行时，
// 每个并行的子任务（Subtask）都会实例化一个实现 TaskInvokable 接口的对象（例如 StreamTask、SourceStreamTask 等）来执行用户定义的算子逻辑。
// 定义任务执行流程： 它明确规定了任务执行的三个核心阶段：恢复状态 (restore)、执行核心逻辑 (invoke) 和清理资源 (cleanUp)。
// 生命周期管理契约： TaskInvokable 充当了 Flink 运行时系统（Task 类）与用户任务逻辑之间的桥梁。Task 负责创建和管理线程，而 TaskInvokable 负责定义具体的执行内容和状态管理。
@Internal
public interface TaskInvokable {

    /**
     * Starts the execution.
     *
     * <p>This method is called by the task manager when the actual execution of the task starts.
     *
     * <p>All resources should be cleaned up by calling {@link #cleanUp(Throwable)} after the method
     * returns.
     */
    // 核心执行
    // 启动任务的实际执行逻辑。
    // 这是用户定义的算子代码（如 SourceFunction、ProcessFunction 等）运行的地方。
    // 它负责设置输入和输出流，并开始数据处理。对于流处理任务，此方法通常会阻塞（即循环读取输入/处理数据），直到任务被取消或遇到终止条件。
    void invoke() throws Exception;

    /**
     * This method can be called before {@link #invoke()} to restore an invokable object for the
     * last valid state, if it has it.
     *
     * <p>If {@link #invoke()} is not called after this method for some reason (e.g. task
     * cancellation); then all resources should be cleaned up by calling {@link #cleanUp(Throwable)}
     * ()} after the method returns.
     */
    // 状态恢复
    // Flink 在调用 invoke() 之前首先调用的方法。它负责从检查点（Checkpoint）或保存点（Savepoint）加载和恢复任务的状态。
    // 如果任务是第一次运行（没有历史状态），这个方法通常不会执行实际的恢复逻辑。
    void restore() throws Exception;

    /**
     * Cleanup any resources used in {@link #invoke()} OR {@link #restore()}. This method must be
     * called regardless whether the aforementioned calls succeeded or failed.
     *
     * @param throwable iff failure happened during the execution of {@link #restore()} or {@link
     *     #invoke()}, null otherwise.
     *     <p>ATTENTION: {@link org.apache.flink.runtime.execution.CancelTaskException
     *     CancelTaskException} should not be treated as a failure.
     */
    // 资源清理
    // 无论 restore() 或 invoke() 是成功完成、抛出异常还是被取消，这个方法都必须被调用。
    // 它用于清理任务使用到的所有资源，包括关闭输入/输出通道、释放网络缓冲区、释放内存等。
    void cleanUp(@Nullable Throwable throwable) throws Exception;

    /**
     * This method is called when a task is canceled either as a result of a user abort or an
     * execution failure. It can be overwritten to respond to shut down the user code properly.取消任务
     */
    // 任务取消
    // 这是任务接收到取消请求时调用的方法。
    // 它应向用户代码发出信号（例如设置一个 volatile 标志位），优雅地关闭用户代码的执行循环，并中断任何阻塞调用，以加速任务退出。
    void cancel() throws Exception;

    /**
     * @return true if blocking input such as {@link InputGate#getNext()} is used (as opposed to
     *     {@link InputGate#pollNext()}. To be removed together with the DataSet API.判断任务是否使用阻塞式输入
     */
    // 检查输入类型
    // 返回 true 表示任务使用阻塞式输入（如 InputGate.getNext()），false 表示非阻塞式（如 InputGate.pollNext()）
    boolean isUsingNonBlockingInput();

    /**
     * Checks whether the task should be interrupted during cancellation and if so, execute the
     * specified {@code Runnable interruptAction}.
     *
     * @param toInterrupt
     * @param taskName optional taskName to log stack trace
     * @param timeout optional timeout to log stack trace在取消任务时，中断指定的线程
     */
    // 用于在取消任务时，根据配置（如配置了取消超时）来决定是否应该通过调用 toInterrupt.interrupt() 来强制中断任务执行线程
    void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout);
}
