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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MailboxClosedException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MIN_PRIORITY;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class encapsulates the logic of the mailbox-based execution model. At the core of this model
 * {@link #runMailboxLoop()} that continuously executes the provided {@link MailboxDefaultAction} in
 * a loop. On each iteration, the method also checks if there are pending actions in the mailbox and
 * executes such actions. This model ensures single-threaded execution between the default action
 * (e.g. record processing) and mailbox actions (e.g. checkpoint trigger, timer firing, ...).
 *
 * <p>The {@link MailboxDefaultAction} interacts with this class through the {@link
 * MailboxController} to communicate control flow changes to the mailbox loop, e.g. that invocations
 * of the default action are temporarily or permanently exhausted.
 *
 * <p>The design of {@link #runMailboxLoop()} is centered around the idea of keeping the expected
 * hot path (default action, no mail) as fast as possible. This means that all checking of mail and
 * other control flags (mailboxLoopRunning, suspendedDefaultAction) are always connected to #hasMail
 * indicating true. This means that control flag changes in the mailbox thread can be done directly,
 * but we must ensure that there is at least one action in the mailbox so that the change is picked
 * up. For control flag changes by all other threads, that must happen through mailbox actions, this
 * is automatically the case.

 * <p>This class has an open-prepareClose-close lifecycle that is connected with and maps to the
 * lifecycle of the encapsulated {@link TaskMailbox} (which is open-quiesce-close).
 */
// MailboxProcessor 是 Flink 流处理任务 (StreamTask) 的核心执行模型，
// 它实现了 Flink **单线程邮箱模型（Mailbox Execution Model）**的逻辑
// MailboxProcessor 的核心作用是确保 Flink 任务的单线程执行和并发安全。它通过一个事件循环（Event Loop）来协调普通数据处理和控制面操作（如 Checkpoint、定时器、状态访问）。
// 统一调度： 它将任务的所有工作（包括数据记录处理和异步控制事件）统一抽象为 **"Mail"（邮件）**或 "Default Action"（默认行为）。
// 事件循环 (runMailboxLoop)： 任务的主线程进入一个无限循环。在循环中：
// 优先处理 Mail： 首先检查并执行邮箱 (TaskMailbox) 中的所有待处理 Mail（通常是高优先级的控制操作）。
// 执行 Default Action： 如果邮箱为空或处理完 Mail 后，主线程会执行 MailboxDefaultAction（通常是正常的记录处理，如 SourceStreamTask.runDefaultAction）
// 单线程执行保证： 由于所有操作都在主线程（邮箱线程）内串行执行，这消除了数据处理与控制操作之间的并发竞争，极大地简化了 Flink 任务的容错和状态管理。
@Internal
public class MailboxProcessor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MailboxProcessor.class);

    /**
     * The mailbox data-structure that manages request for special actions, like timers,
     * checkpoints, ...
     */
    // 任务邮箱数据结构。存储所有待执行的特殊操作请求 (Mail)，如定时器触发、Checkpoint 触发等。
    protected final TaskMailbox mailbox;

    /**
     * Action that is repeatedly executed if no action request is in the mailbox. Typically record
     * processing.
     */
    // 默认行为。
    // 代表任务的主工作负载，通常是数据记录的处理逻辑。
    // 当邮箱没有 Mail 时，它会被重复执行。
    protected final MailboxDefaultAction mailboxDefaultAction;

    /**
     * Control flag to terminate the mailbox processor. Once it was terminated could not be
     * restarted again. Must only be accessed from mailbox thread.
     */
    // 循环运行标志。
    // 控制邮箱主循环 (runMailboxLoop) 是否应继续运行。
    // 一旦设置为 false，邮箱处理器将终止，且不能重启。只能由邮箱线程访问。
    private boolean mailboxLoopRunning;

    /**
     * Control flag to temporary suspend the mailbox loop/processor. After suspending the mailbox
     * processor can be still later resumed. Must only be accessed from mailbox thread.
     */
    // 挂起标志。
    // 控制邮箱主循环是否应该暂时暂停（挂起）。
    // 如果为 true，循环会停止执行 Default Action，但可以恢复。只能由邮箱线程访问。
    private boolean suspended;

    /**
     * Remembers a currently active suspension of the default action. Serves as flag to indicate a
     * suspended default action (suspended if not-null) and to reuse the object as return value in
     * consecutive suspend attempts. Must only be accessed from mailbox thread.
     */
    // 默认行为挂起状态。
    // 当默认行为被挂起时，该字段不为 null，并存储一个 DefaultActionSuspension 实例。
    // 它既是挂起状态的指示器，也是后续恢复操作的对象。只能由邮箱线程访问。
    private DefaultActionSuspension suspendedDefaultAction;
    // 任务操作执行器。
    // 用于在执行邮箱操作时，确保并发安全地访问任务的共享资源（如状态锁）
    private final StreamTaskActionExecutor actionExecutor;
    // 邮箱指标控制器。
    // 用于收集和控制与邮箱相关的运行时指标，如邮件处理延迟和数量统计。
    private final MailboxMetricsController mailboxMetricsControl;

    @VisibleForTesting
    public MailboxProcessor() {
        this(MailboxDefaultAction.Controller::suspendDefaultAction);
    }

    public MailboxProcessor(MailboxDefaultAction mailboxDefaultAction) {
        this(mailboxDefaultAction, StreamTaskActionExecutor.IMMEDIATE);
    }

    public MailboxProcessor(
            MailboxDefaultAction mailboxDefaultAction, StreamTaskActionExecutor actionExecutor) {
        this(mailboxDefaultAction, new TaskMailboxImpl(Thread.currentThread()), actionExecutor); //mailbox默认当前线程
    }

    public MailboxProcessor(
            MailboxDefaultAction mailboxDefaultAction,
            TaskMailbox mailbox,
            StreamTaskActionExecutor actionExecutor) {
        this(
                mailboxDefaultAction,
                mailbox,
                actionExecutor,
                new MailboxMetricsController(
                        new DescriptiveStatisticsHistogram(10), new SimpleCounter()));
    }

    public MailboxProcessor(
            MailboxDefaultAction mailboxDefaultAction,
            TaskMailbox mailbox,
            StreamTaskActionExecutor actionExecutor,
            MailboxMetricsController mailboxMetricsControl) {
        this.mailboxDefaultAction = Preconditions.checkNotNull(mailboxDefaultAction);
        this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
        this.mailbox = Preconditions.checkNotNull(mailbox);
        this.mailboxLoopRunning = true;
        this.suspendedDefaultAction = null;
        this.mailboxMetricsControl = mailboxMetricsControl;
    }
    // 获取主邮箱执行器。
    // 返回一个门面（Facade），允许其他组件以 最低优先级 向邮箱提交操作
    public MailboxExecutor getMainMailboxExecutor() {
        return new MailboxExecutorImpl(mailbox, MIN_PRIORITY, actionExecutor);
    }

    /**
     * Returns an executor service facade to submit actions to the mailbox.
     *
     * @param priority the priority of the {@link MailboxExecutor}.
     */
    // 获取带优先级的邮箱执行器。
    // 允许其他线程或组件以指定的优先级向邮箱提交 Mail。
    public MailboxExecutor getMailboxExecutor(int priority) {
        return new MailboxExecutorImpl(mailbox, priority, actionExecutor, this);
    }

    /**
     * Gets {@link MailboxMetricsController} for control and access to mailbox metrics.
     *
     * @return {@link MailboxMetricsController}.
     */
    @VisibleForTesting
    public MailboxMetricsController getMailboxMetricsControl() {
        return this.mailboxMetricsControl;
    }

    /** Lifecycle method to close the mailbox for action submission. */
    public void prepareClose() {
        mailbox.quiesce();
    }

    /**
     * Lifecycle method to close the mailbox for action submission/retrieval. This will cancel all
     * instances of {@link java.util.concurrent.RunnableFuture} that are still contained in the
     * mailbox.
     */
    @Override
    public void close() {
        List<Mail> droppedMails = mailbox.close();
        if (!droppedMails.isEmpty()) {
            LOG.debug("Closing the mailbox dropped mails {}.", droppedMails);
            Optional<RuntimeException> maybeErr = Optional.empty();
            for (Mail droppedMail : droppedMails) {
                try {
                    droppedMail.tryCancel(false);
                } catch (RuntimeException x) {
                    maybeErr =
                            Optional.of(ExceptionUtils.firstOrSuppressed(x, maybeErr.orElse(null)));
                }
            }
            maybeErr.ifPresent(
                    e -> {
                        throw e;
                    });
        }
    }

    /**
     * Finishes running all mails in the mailbox. If no concurrent write operations occurred, the
     * mailbox must be empty after this method.
     */
    public void drain() throws Exception {
        for (final Mail mail : mailbox.drain()) {
            runMail(mail);
        }
    }

    /**
     * Runs the mailbox processing loop. This is where the main work is done. This loop can be
     * suspended at any time by calling {@link #suspend()}. For resuming the loop this method should
     * be called again.
     */
    // 运行邮箱处理主循环。
    // 任务的主线程会调用此方法进入无限循环，直到任务结束或被挂起。
    // 循环中会交替执行 processMail 和 runDefaultAction。
    public void runMailboxLoop() throws Exception {
        suspended = !mailboxLoopRunning;

        final TaskMailbox localMailbox = mailbox;

        checkState(
                localMailbox.isMailboxThread(),
                "Method must be executed by declared mailbox thread!");

        assert localMailbox.getState() == TaskMailbox.State.OPEN : "Mailbox must be opened!";
        //创建mailbox控制器
        final MailboxController mailboxController = new MailboxController(this);

        while (isNextLoopPossible()) { //如果没有被挂起，suspended = false
            // The blocking `processMail` call will not return until default action is available.
            processMail(localMailbox, false);
            if (isNextLoopPossible()) {
                mailboxDefaultAction.runDefaultAction(
                        mailboxController); // lock is acquired inside default action as needed
            }
        }
    }

    /** Suspend the running of the loop which was started by {@link #runMailboxLoop()}}. */
    public void suspend() {
        sendPoisonMail(() -> suspended = true);
    }

    /**
     * Execute a single (as small as possible) step of the mailbox.
     *
     * @return true if something was processed.
     */
    @VisibleForTesting
    public boolean runSingleMailboxLoop() throws Exception {
        suspended = !mailboxLoopRunning;
        boolean processed = processMail(mailbox, true);
        if (isDefaultActionAvailable() && isNextLoopPossible()) {
            mailboxDefaultAction.runDefaultAction(new MailboxController(this));
            processed = true;
        }
        return processed;
    }

    /**
     * Execute a single (as small as possible) step of the mailbox.
     *
     * @return true if something was processed.
     */
    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        suspended = !mailboxLoopRunning;

        if (processMail(mailbox, true)) {
            return true;
        }
        if (isDefaultActionAvailable() && isNextLoopPossible()) {
            mailboxDefaultAction.runDefaultAction(new MailboxController(this));
            return true;
        }
        return false;
    }

    /**
     * Check if the current thread is the mailbox thread.
     *
     * @return only true if called from the mailbox thread.
     */
    public boolean isMailboxThread() {
        return mailbox.isMailboxThread();
    }

    /**
     * Reports a throwable for rethrowing from the mailbox thread. This will clear and cancel all
     * other pending mails.
     *
     * @param throwable to report by rethrowing from the mailbox loop.
     */
    public void reportThrowable(Throwable throwable) {
        sendControlMail(
                () -> {
                    if (throwable instanceof Exception) {
                        throw (Exception) throwable;
                    } else if (throwable instanceof Error) {
                        throw (Error) throwable;
                    } else {
                        throw WrappingRuntimeException.wrapIfNecessary(throwable);
                    }
                },
                "Report throwable %s",
                throwable);
    }

    /**
     * This method must be called to end the stream task when all actions for the tasks have been
     * performed.
     */
    public void allActionsCompleted() {
        sendPoisonMail(
                () -> {
                    mailboxLoopRunning = false;
                    suspended = true;
                });
    }

    /** Send mail in first priority for internal needs. */
    private void sendPoisonMail(RunnableWithException mail) {
        mailbox.runExclusively(
                () -> {
                    // keep state check and poison mail enqueuing atomic, such that no intermediate
                    // #close may cause a
                    // MailboxStateException in #sendPriorityMail.
                    if (mailbox.getState() == TaskMailbox.State.OPEN) {
                        sendControlMail(mail, "poison mail"); //OPEN状态，就把mail放在队列的头
                    }
                });
    }

    /**
     * Sends the given <code>mail</code> using {@link TaskMailbox#putFirst(Mail)} . Intended use is
     * to control this <code>MailboxProcessor</code>; no interaction with tasks should be performed;
     */
    // 控制邮件放在最前面
    private void sendControlMail(
            RunnableWithException mail, String descriptionFormat, Object... descriptionArgs) {
        mailbox.putFirst(
                new Mail(
                        mail,
                        Integer.MAX_VALUE /*not used with putFirst*/,
                        descriptionFormat,
                        descriptionArgs));
    }

    /**  处理邮件
     * This helper method handles all special actions from the mailbox. In the current design, this
     * method also evaluates all control flag changes. This keeps the hot path in {@link
     * #runMailboxLoop()} free from any other flag checking, at the cost that all flag changes must
     * make sure that the mailbox signals mailbox#hasMail.
     *
     * @return true if a mail has been processed.
     */
    // 负责处理邮箱中的所有控制邮件（Mail），并根据任务状态决定是否需要阻塞等待新邮件。
    private boolean processMail(TaskMailbox mailbox, boolean singleStep) throws Exception {
        // Doing this check is an optimization to only have a volatile read in the expected hot
        // path, locks are only
        // acquired after this point.
        // 尝试在邮箱中创建一个邮件批次
        boolean isBatchAvailable = mailbox.createBatch();

        // Take mails in a non-blockingly and execute them.
        boolean processed = isBatchAvailable && processMailsNonBlocking(singleStep);
        if (singleStep) {
            return processed;
        }

        // If the default action is currently not available, we can run a blocking mailbox execution
        // until the default action becomes available again.
        // 检查默认操作是否可用，如果不可用（如任务空闲或挂起），它将阻塞等待新的 Mail 到达并执行它们
        processed |= processMailsWhenDefaultActionUnavailable();

        return processed;
    }
    // 负责在默认操作（数据处理）不可用（即任务空闲或被挂起）时，阻塞式地处理邮箱中的控制邮件。
    // 目的是确保即使 Flink 任务由于空闲（没有输入数据）或反压/暂停（isDefaultActionAvailable() 为 false）而无法进行常规数据处理时，
    // 它仍然能够以阻塞等待的方式处理高优先级的控制邮件
    private boolean processMailsWhenDefaultActionUnavailable() throws Exception {
        boolean processedSomething = false;
        Optional<Mail> maybeMail;
        // 默认操作（数据处理）不可用（即处于暂停或空闲状态）。
        // 邮箱循环仍在运行且未被临时挂起 (!suspended)。
        while (!isDefaultActionAvailable() && isNextLoopPossible()) {
            maybeMail = mailbox.tryTake(MIN_PRIORITY);
            if (!maybeMail.isPresent()) {
                // 检查是否取到邮件：如果上一步没有取到邮件（即 maybeMail 为空）
                // 阻塞等待：此时转为阻塞式调用 mailbox.take(MIN_PRIORITY)。执行线程会暂停，直到有新的邮件到达邮箱
                maybeMail = Optional.of(mailbox.take(MIN_PRIORITY));
            }
            maybePauseIdleTimer();

            runMail(maybeMail.get());

            maybeRestartIdleTimer();
            processedSomething = true;
        }
        return processedSomething;
    }
    //负责在非阻塞模式下，从邮箱的当前**邮件批次 (Mail Batch)**中尽可能快地取出并执行待处理的控制邮件。
    // boolean singleStep： 控制执行模式。如果为 true，方法在处理完一个邮件后就会退出；
    // 如果为 false，它会尝试处理当前批次中的所有邮件。
    private boolean processMailsNonBlocking(boolean singleStep) throws Exception {
        long processedMails = 0;
        Optional<Mail> maybeMail;
        // 检查邮箱循环是否仍在运行且未被临时挂起
        // 非阻塞地尝试从邮箱的当前批次中取出一个邮件
        while (isNextLoopPossible() && (maybeMail = mailbox.tryTakeFromBatch()).isPresent()) {
            if (processedMails++ == 0) {
                // 如果默认行为 (MailboxDefaultAction) 此前因空闲而被挂起，并且正在使用计时器测量空闲时间，则调用此方法暂停计时器
                maybePauseIdleTimer();
            }
            // 执行邮件和单步控制
            runMail(maybeMail.get());
            if (singleStep) {
                break;
            }
        }
        if (processedMails > 0) {
            maybeRestartIdleTimer();
            return true;
        } else {
            return false;
        }
    }
    // 作用是执行单个控制邮件 (Mail)，并管理与邮件处理相关的运行时指标
    private void runMail(Mail mail) throws Exception {
        mailboxMetricsControl.getMailCounter().inc();
        mail.run();
        // 检查是否挂起：判断 MailboxProcessor 的默认行为循环是否处于非临时挂起状态
        if (!suspended) {
            // start latency measurement on first mail that is not suspending mailbox execution,
            // i.e., on first non-poison mail, otherwise latency measurement is not started to avoid
            // overhead
            //判断延迟测量是否尚未启动。
            if (!mailboxMetricsControl.isLatencyMeasurementStarted()
                    && mailboxMetricsControl.isLatencyMeasurementSetup()) {
                mailboxMetricsControl.startLatencyMeasurement();
            }
        }
    }
    // 暂停空闲时间服务
    private void maybePauseIdleTimer() {
        if (suspendedDefaultAction != null && suspendedDefaultAction.suspensionTimer != null) {
            suspendedDefaultAction.suspensionTimer.markEnd();
        }
    }

    private void maybeRestartIdleTimer() {
        if (suspendedDefaultAction != null && suspendedDefaultAction.suspensionTimer != null) {
            suspendedDefaultAction.suspensionTimer.markStart();
        }
    }

    /**
     * Calling this method signals that the mailbox-thread should (temporarily) stop invoking the
     * default action, e.g. because there is currently no input available.
     */
    // 挂起状态就是设置suspendedDefaultAction为一个值
    private MailboxDefaultAction.Suspension suspendDefaultAction(
            @Nullable PeriodTimer suspensionTimer) {

        checkState(
                mailbox.isMailboxThread(),
                "Suspending must only be called from the mailbox thread!");

        checkState(suspendedDefaultAction == null, "Default action has already been suspended");
        if (suspendedDefaultAction == null) {
            suspendedDefaultAction = new DefaultActionSuspension(suspensionTimer);
        }

        return suspendedDefaultAction;
    }

    @VisibleForTesting
    public boolean isDefaultActionAvailable() {
        return suspendedDefaultAction == null;
    }
    //任务没有被挂起
    private boolean isNextLoopPossible() {
        // 'Suspended' can be false only when 'mailboxLoopRunning' is true.
        return !suspended;
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxLoopRunning;
    }

    public boolean hasMail() {
        return mailbox.hasMail();
    }

    /**
     * Implementation of {@link MailboxDefaultAction.Controller} that is connected to a {@link
     * MailboxProcessor} instance.
     */
    // MailboxDefaultAction的Controller接口通过MailboxProcessor的对应方法来实现
    protected static final class MailboxController implements MailboxDefaultAction.Controller {

        private final MailboxProcessor mailboxProcessor;

        protected MailboxController(MailboxProcessor mailboxProcessor) {
            this.mailboxProcessor = mailboxProcessor;
        }

        @Override
        public void allActionsCompleted() {
            mailboxProcessor.allActionsCompleted();
        }

        @Override
        public MailboxDefaultAction.Suspension suspendDefaultAction(
                PeriodTimer suspensionPeriodTimer) {
            return mailboxProcessor.suspendDefaultAction(suspensionPeriodTimer);
        }

        @Override
        public MailboxDefaultAction.Suspension suspendDefaultAction() {
            return mailboxProcessor.suspendDefaultAction(null);
        }
    }

    /**
     * Represents the suspended state of the default action and offers an idempotent method to
     * resume execution.
     */
    // Mailbox挂起状态的恢复类
    private final class DefaultActionSuspension implements MailboxDefaultAction.Suspension {
        @Nullable private final PeriodTimer suspensionTimer; //暂停时间

        public DefaultActionSuspension(@Nullable PeriodTimer suspensionTimer) {
            this.suspensionTimer = suspensionTimer;
        }

        @Override
        public void resume() {
            if (mailbox.isMailboxThread()) {
                resumeInternal();
            } else {
                try {
                    sendControlMail(this::resumeInternal, "resume default action");
                } catch (MailboxClosedException ex) {
                    // Ignored
                }
            }
        }
        //恢复的时候把挂起状态设置为空就可以
        private void resumeInternal() {
            if (suspendedDefaultAction == this) {
                suspendedDefaultAction = null;
            }
        }
    }
}
