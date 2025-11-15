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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FutureTaskWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@link java.util.concurrent.Executor} like interface for a build around a mailbox-based execution
 * model. {@code MailboxExecutor} can also execute downstream messages of a mailbox by yielding
 * control from the task thread.
 *
 * <p>All submission functions can be called from any thread and will enqueue the action for further
 * processing in a FIFO fashion.
 *
 * <p>The yielding functions avoid the following situation: One operator cannot fully process an
 * input record and blocks the task thread until some resources are available. However, since the
 * introduction of the mailbox model blocking the task thread will not only block new inputs but
 * also all events from being processed. If the resources depend on downstream operators being able
 * to process such events (e.g., timers), then we may easily arrive at some livelocks.
 *
 * <p>The yielding functions will only process events from the operator itself and any downstream
 * operator. Events of upstream operators are only processed when the input has been fully processed
 * or if they yield themselves. This method avoid congestion and potential deadlocks, but will
 * process mails slightly out-of-order, effectively creating a view on the mailbox that contains no
 * message from upstream operators.
 *
 * <p><b>All yielding functions must be called in the mailbox thread</b> to not violate the
 * single-threaded execution model. There are two typical cases, both waiting until the resource is
 * available. The main difference is if the resource becomes available through a mailbox message
 * itself or not.
 *
 * <p>If the resource becomes available through a mailbox mail, we can effectively block the task
 * thread. Implicitly, this requires the mail to be enqueued by a different thread.
 *
 * <pre>{@code
 * while (resource not available) {
 *     mailboxExecutor.yield();
 * }
 * }</pre>
 *
 * <pre>in some other thread{@code
 * mailboxExecutor.execute(() -> free resource, "freeing resource");
 * }</pre>
 *
 * <p>If the resource becomes available through an external mechanism or the corresponding mail
 * needs to be enqueued in the task thread, we cannot block.
 *
 * <pre>{@code
 * while (resource not available) {
 *     if (!mailboxExecutor.tryYield()) {
 *         // do stuff or sleep for a small amount of time
 *         if (special condition) {
 *             free resource
 *         }
 *     }
 * }
 * }</pre>
 */
// Flink 的 Task Mailbox 模型旨在将所有 Task 相关的活动（数据处理、事件处理、定时器、Checkpoint 等）串行化到单个线程中执行，从而避免复杂的并发控制和死锁问题。
// 统一任务提交： 允许任何线程（包括 Task 线程、网络 I/O 线程、定时器线程等）以线程安全的方式，将各种**待办事项（Mail）**提交到 Task Mailbox 中排队。
// 实现协作式多任务： 它提供了 yield() 和 tryYield() 方法。这使得 Task 线程在处理数据时，如果遇到阻塞或需要处理高优先级事件（如 Checkpoint），可以主动让出控制权，去执行 Mailbox 中排队的下一个任务，从而避免了 Task 线程的僵死（Livelock
// 保障单线程执行： 无论 Mailbox 中的任务来自哪里，它们都保证在同一个 Task Mailbox 线程中以 FIFO 顺序执行，维护了 Flink 状态访问的单线程模型。

@PublicEvolving
public interface MailboxExecutor { //主要作用是向 TaskMailbox 中投递 Mail
    /** A constant for empty args to save on object allocation. */
    // 空参数常量
    Object[] EMPTY_ARGS = new Object[0];

    /** Extra options to configure enqueued mails. */
    // 用于配置提交给 Mailbox 的任务（Mail）行为的接口
    @PublicEvolving
    interface MailOptions {
        static MailOptions options() {
            return MailOptionsImpl.DEFAULT;
        }

        /**
         * Mark this mail as deferrable.
         * 是否可以延迟
         * <p>Runtime can decide to defer execution of deferrable mails. For example, to unblock
         * subtask thread as quickly as possible, deferrable mails are not executed during {@link
         * #yield()} or {@link #tryYield()}. This is done to speed up checkpointing, by skipping
         * execution of potentially long-running mails.
         */
        // 标记该 Mail 可以被运行时延迟执行。例如，在 Task 线程主动调用 yield() 或 tryYield() 时，可延迟的 Mail 不会被执行。
        // 用途： 主要是为了加速 Checkpoint 过程，让 Task 线程在 Checkpoint 期间优先处理高优先级事件，跳过可能耗时的低优先级 Mail。
        static MailOptions deferrable() {
            return MailOptionsImpl.DEFERRABLE;
        }
    }

    /**
     * Executes the given command at some time in the future in the mailbox thread.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the runnable task to add to the mailbox for execution.
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default void execute(ThrowingRunnable<? extends Exception> command, String description) {
        execute(command, description, EMPTY_ARGS);
    }

    /**
     * Executes the given command at some time in the future in the mailbox thread.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param mailOptions additional options to configure behaviour of the {@code command}
     * @param command the runnable task to add to the mailbox for execution.
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default void execute(
            MailOptions mailOptions,
            ThrowingRunnable<? extends Exception> command,
            String description) {
        execute(mailOptions, command, description, EMPTY_ARGS);
    }

    /**
     * Executes the given command at some time in the future in the mailbox thread.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the runnable task to add to the mailbox for execution.
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default void execute(
            ThrowingRunnable<? extends Exception> command,
            String descriptionFormat,
            Object... descriptionArgs) {
        execute(MailOptions.options(), command, descriptionFormat, descriptionArgs);
    }

    /**
     * Executes the given command at some time in the future in the mailbox thread.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param mailOptions additional options to configure behaviour of the {@code command}
     * @param command the runnable task to add to the mailbox for execution.
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    // 提交并执行任务。
    // 功能： 将一个 ThrowingRunnable 任务添加到 Mailbox 队列中，以待在 Mailbox 线程中执行。
    // 关键参数： command（要执行的逻辑），descriptionFormat 和 descriptionArgs（可选的格式化描述，用于调试和报错）。
    // 执行保证： 任务将在未来某个时间点被 Mailbox 线程执行。
    void execute(
            MailOptions mailOptions,
            ThrowingRunnable<? extends Exception> command,
            String descriptionFormat,
            Object... descriptionArgs);

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     * 提交command在mailbox线程执行，并返回Futrue
     * <p>WARNING: Exception raised by the {@code command} will not fail the task but are stored in
     * the future. Thus, it's an anti-pattern to call {@code submit} without handling the returned
     * future and {@link #execute(ThrowingRunnable, String, Object...)} should be used instead.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    // 提交带有 Future 的任务。
    // 功能： 将一个 RunnableWithException 或 Callable 任务添加到 Mailbox 队列，并立即返回一个 Future 对象。
    // 区别： 允许外部线程等待或检查任务的执行结果和异常
    default @Nonnull Future<Void> submit(
            @Nonnull RunnableWithException command,
            String descriptionFormat,
            Object... descriptionArgs) {
        FutureTaskWithException<Void> future = new FutureTaskWithException<>(command);
        execute(future, descriptionFormat, descriptionArgs); //执行FutureTask
        return future;
    }

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>WARNING: Exception raised by the {@code command} will not fail the task but are stored in
     * the future. Thus, it's an anti-pattern to call {@code submit} without handling the returned
     * future and {@link #execute(ThrowingRunnable, String, Object...)} should be used instead.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull Future<Void> submit(
            @Nonnull RunnableWithException command, String description) {
        FutureTaskWithException<Void> future = new FutureTaskWithException<>(command);
        execute(future, description, EMPTY_ARGS);
        return future;
    }

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>WARNING: Exception raised by the {@code command} will not fail the task but are stored in
     * the future. Thus, it's an anti-pattern to call {@code submit} without handling the returned
     * future and {@link #execute(ThrowingRunnable, String, Object...)} should be used instead.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull <T> Future<T> submit(
            @Nonnull Callable<T> command, String descriptionFormat, Object... descriptionArgs) {
        FutureTaskWithException<T> future = new FutureTaskWithException<>(command);
        execute(future, descriptionFormat, descriptionArgs);
        return future;
    }

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>WARNING: Exception raised by the {@code command} will not fail the task but are stored in
     * the future. Thus, it's an anti-pattern to call {@code submit} without handling the returned
     * future and {@link #execute(ThrowingRunnable, String, Object...)} should be used instead.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull <T> Future<T> submit(@Nonnull Callable<T> command, String description) {
        FutureTaskWithException<T> future = new FutureTaskWithException<>(command);
        execute(future, description, EMPTY_ARGS);
        return future;
    }

    /**
     * This method starts running the command at the head of the mailbox and is intended to be used
     * by the mailbox thread to yield from a currently ongoing action to another command. The method
     * blocks until another command to run is available in the mailbox and must only be called from
     * the mailbox thread. Must only be called from the mailbox thread to not violate the
     * single-threaded execution model.
     *
     * @throws InterruptedException on interruption.
     * @throws IllegalStateException if the mailbox is closed and can no longer supply runnables for
     *     yielding.
     * @throws FlinkRuntimeException if executed {@link RunnableWithException} thrown an exception.
     */
    // 阻塞式让步。
    // 功能： Task 线程阻塞，从 Mailbox 中取出并执行下一个可用的 Mail。
    // 用途： 当 Task 线程需要等待某个资源（该资源将在另一个 Mail 中被释放）时使用。它会一直阻塞，直到 Mailbox 中有 Mail 被执行
    void yield() throws InterruptedException, FlinkRuntimeException;

    /**
     * This method attempts to run the command at the head of the mailbox. This is intended to be
     * used by the mailbox thread to yield from a currently ongoing action to another command. The
     * method returns true if a command was found and executed or false if the mailbox was empty.
     * Must only be called from the mailbox thread to not violate the single-threaded execution
     * model.
     *
     * @return true on successful yielding to another command, false if there was no command to
     *     yield to.
     * @throws IllegalStateException if the mailbox is closed and can no longer supply runnables for
     *     yielding.
     * @throws RuntimeException if executed {@link RunnableWithException} thrown an exception.
     */
    // 非阻塞式让步。
    // 功能： 尝试从 Mailbox 中取出并执行下一个可用的 Mail。
    // 返回值： 如果 Mailbox 中有 Mail 且成功执行，返回 true；否则（Mailbox 为空），返回 false 并立即返回控制权。
    boolean tryYield() throws FlinkRuntimeException;

    /**
     * Return if operator/function should interrupt a longer computation and return from the
     * currently processed elemenent/watermark, for example in order to let Flink perform a
     * checkpoint.
     *
     * @return whether operator/function should interrupt its computation.
     */
    // 是否应该中断检查
    boolean shouldInterrupt();
}
