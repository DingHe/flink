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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class handles the finish, endInput and other related logic of a {@link StreamOperator}. It
 * also automatically propagates the finish operation to the next wrapper that the {@link #next}
 * points to, so we can use {@link #next} to link all operator wrappers in the operator chain and
 * finish all operators only by calling the {@link #finish(StreamTaskActionExecutor, StopMode)}
 * method of the header operator wrapper.
 */
// 负责封装并管理一个或一组链式操作符的生命周期、状态通知和关闭逻辑。它在 Flink 的 Task Mailbox 模型和 操作符链 中扮演着重要的角色。
// 管理操作符链： Task 内部链在一起的多个 StreamOperator 实例，会分别被封装到 StreamOperatorWrapper 实例中，这些 Wrapper 通过 next 和 previous 字段形成一个双向链表。
// 生命周期同步： 它确保像 finish()、endInput() 和 close() 这样的生命周期方法能够按正确的顺序沿链表传播，从而保证所有链上的操作符都能被正确关闭。
// Mailbox 集成： 它利用 MailboxExecutor 来安全地处理异步操作，例如确保在关闭操作符之前，所有悬而未决的定时器（Timers）和 Mailbox 中的异步请求都已完成，防止竞态条件。
// 状态通知转发： 它转发 Checkpoint 完成和 Checkpoint 被取代的通知给被包装的操作符及其状态后端。
@Internal
public class StreamOperatorWrapper<OUT, OP extends StreamOperator<OUT>> {
    // 被包装的 StreamOperator 实例。
    // 这是该 Wrapper 实际控制的 Flink 操作符。
    private final OP wrapped;
    // 处理时间服务。
    // 用于管理操作符的定时器（Timers）。
    // 在 Task 关闭时，需要对其进行静默处理（quiesce），以防止在关闭过程中触发新的定时器。
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<ProcessingTimeService> processingTimeService;
    // 邮箱执行器。
    // Flink Task 的核心调度组件。
    // 用于将操作（如 finish()）作为一个 Mail 发送到 Task 线程中安全执行。
    private final MailboxExecutor mailboxExecutor;
    // 是否为链头。
    // true 表示这是操作符链中的第一个操作符。
    // 这在 finish() 逻辑中很重要，因为它影响非链头操作符的 endInput 行为。
    private final boolean isHead;
    // 前一个 Wrapper。
    // 指向链表中的前一个操作符 Wrapper。
    private StreamOperatorWrapper<?, ?> previous;
    // 下一个 Wrapper。
    // 指向链表中的下一个操作符 Wrapper。
    // 用于传播 finish 和 close 操作。
    private StreamOperatorWrapper<?, ?> next;
    // 关闭状态标志。
    // 标记当前 Wrapper 所包装的操作符是否已被逻辑关闭。
    private boolean closed;

    StreamOperatorWrapper(
            OP wrapped,
            Optional<ProcessingTimeService> processingTimeService,
            MailboxExecutor mailboxExecutor,
            boolean isHead) {

        this.wrapped = checkNotNull(wrapped);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.isHead = isHead;
    }

    /**
     * Checks if the wrapped operator has been closed.
     *
     * <p>Note that this method must be called in the task thread.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Ends an input of the operator contained by this wrapper.
     *
     * @param inputId the input ID starts from 1 which indicates the first input.
     */
    // 通知操作符某个输入流已结束
    // 检查被封装的操作符是否实现了 BoundedOneInput（单输入有界流结束）或 BoundedMultiInput（多输入有界流结束），
    // 然后调用相应的 endInput 方法。inputId 从 1 开始编号。
    public void endOperatorInput(int inputId) throws Exception {
        if (wrapped instanceof BoundedOneInput) {
            ((BoundedOneInput) wrapped).endInput();
        } else if (wrapped instanceof BoundedMultiInput) {
            ((BoundedMultiInput) wrapped).endInput(inputId);
        }
    }
    // 通知操作符 Checkpoint 已成功完成
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!closed) {
            wrapped.notifyCheckpointComplete(checkpointId);
        }
    }
    // 通知操作符一个较老的 Checkpoint 已被更新的 Checkpoint 所取代（Subsumed）
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        if (!closed) {
            KeyedStateBackend<?> keyedStateBackend = null;
            if (wrapped instanceof AbstractStreamOperator) {
                keyedStateBackend = ((AbstractStreamOperator<?>) wrapped).getKeyedStateBackend();
            } else if (wrapped instanceof AbstractStreamOperatorV2) {
                keyedStateBackend = ((AbstractStreamOperatorV2<?>) wrapped).getKeyedStateBackend();
            }

            if (keyedStateBackend instanceof InternalCheckpointListener) {
                ((InternalCheckpointListener) keyedStateBackend)
                        .notifyCheckpointSubsumed(checkpointId);
            }
        }
    }

    public OP getStreamOperator() {
        return wrapped;
    }

    void setPrevious(StreamOperatorWrapper previous) {
        this.previous = previous;
    }

    void setNext(StreamOperatorWrapper next) {
        this.next = next;
    }

    /**
     * Finishes the wrapped operator and propagates the finish operation to the next wrapper that
     * the {@link #next} points to.
     *
     * <p>Note that this method must be called in the task thread, because we need to call {@link
     * MailboxExecutor#yield()} to take the mails of closing operator and running timers and run
     * them.
     */
    // 优雅地结束当前操作符的生命周期，并将指令沿着链表向下传播。
    public void finish(StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws Exception {
        if (!isHead && stopMode == StopMode.DRAIN) {
            // 如果当前 Wrapper 不是链头且处于 DRAIN 停止模式，
            // 它会调用 endOperatorInput(1) 触发下游操作符的 endInput，确保所有剩余数据被耗尽。
            // NOTE: This only do for the case where the operator is one-input operator. At present,
            // any non-head operator on the operator chain is one-input operator.
            actionExecutor.runThrowing(() -> endOperatorInput(1));
        }
        // 确保所有异步任务和定时器完成，然后执行操作符的 finish() 方法
        quiesceTimeServiceAndFinishOperator(actionExecutor, stopMode);

        // propagate the close operation to the next wrapper
        if (next != null) {
            next.finish(actionExecutor, stopMode);
        }
    }

    /** Close the operator. */
    // 关闭被封装的操作符。
    public void close() throws Exception {
        closed = true;
        wrapped.close();
    }
    // 确保先静默时间服务（阻止新的定时器），然后将 finish() 操作作为 Mailbox 任务执行，
    // 并在等待所有任务完成后才返回，以保证操作符的优雅关闭。
    private void quiesceTimeServiceAndFinishOperator(
            StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws InterruptedException, ExecutionException {

        // step 1. to ensure that there is no longer output triggered by the timers before invoking
        // the "finish()" method of the operator, we quiesce the processing time service to prevent
        // the pending timers from firing, but wait the timers in running to finish
        // step 2. invoke the "finish()" method of the operator. executing the close operation must
        // be deferred to the mailbox to ensure that mails already in the mailbox are finished
        // before closing the operator
        // step 3. send a closed mail to ensure that the mails that are from the operator and still
        // in the mailbox are completed before exiting the following mailbox processing loop
        CompletableFuture<Void> finishedFuture =
                quiesceProcessingTimeService()
                        .thenCompose(
                                unused -> deferFinishOperatorToMailbox(actionExecutor, stopMode))
                        .thenCompose(unused -> sendFinishedMail());

        // run the mailbox processing loop until all operations are finished
        while (!finishedFuture.isDone()) {
            while (mailboxExecutor.tryYield()) {}

            // we wait a little bit to avoid unnecessary CPU occupation due to empty loops,
            // such as when all mails of the operator have been processed but the closed future
            // has not been set to completed state
            try {
                finishedFuture.get(1, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                // do nothing
            }
        }

        // expose the exception thrown when finishing
        finishedFuture.get();
    }
    // 将操作符的 finish() 方法的执行延迟到 Mailbox 中，以确保按顺序执行
    private CompletableFuture<Void> deferFinishOperatorToMailbox(
            StreamTaskActionExecutor actionExecutor, StopMode stopMode) {
        if (stopMode == StopMode.NO_DRAIN) {
            return CompletableFuture.completedFuture(null);
        }

        final CompletableFuture<Void> finishOperatorFuture = new CompletableFuture<>();

        mailboxExecutor.execute(
                () -> {
                    try {
                        finishOperator(actionExecutor);
                        finishOperatorFuture.complete(null);
                    } catch (Throwable t) {
                        finishOperatorFuture.completeExceptionally(t);
                    }
                },
                "StreamOperatorWrapper#finishOperator for " + wrapped);
        return finishOperatorFuture;
    }
    // 静默处理时间服务
    private CompletableFuture<Void> quiesceProcessingTimeService() {
        return processingTimeService
                .map(ProcessingTimeService::quiesce)
                .orElse(CompletableFuture.completedFuture(null));
    }
    //
    private CompletableFuture<Void> sendFinishedMail() {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        mailboxExecutor.execute(
                () -> future.complete(null),
                "StreamOperatorWrapper#sendFinishedMail for " + wrapped);
        return future;
    }

    private void finishOperator(StreamTaskActionExecutor actionExecutor) throws Exception {
        actionExecutor.runThrowing(wrapped::finish);
    }

    static class ReadIterator
            implements Iterator<StreamOperatorWrapper<?, ?>>,
                    Iterable<StreamOperatorWrapper<?, ?>> {

        private final boolean reverse;

        private StreamOperatorWrapper<?, ?> current;

        ReadIterator(StreamOperatorWrapper<?, ?> first, boolean reverse) {
            this.current = first;
            this.reverse = reverse;
        }

        @Override
        public boolean hasNext() {
            return this.current != null;
        }

        @Override
        public StreamOperatorWrapper<?, ?> next() {
            if (hasNext()) {
                StreamOperatorWrapper<?, ?> next = current;
                current = reverse ? current.previous : current.next;
                return next;
            }

            throw new NoSuchElementException();
        }

        @Nonnull
        @Override
        public Iterator<StreamOperatorWrapper<?, ?>> iterator() {
            return this;
        }
    }
}
