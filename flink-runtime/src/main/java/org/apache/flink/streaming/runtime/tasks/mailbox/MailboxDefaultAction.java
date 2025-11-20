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

/** Interface for the default action that is repeatedly invoked in the mailbox-loop. */
// 定义了邮箱事件循环中默认执行的操作，即任务的主工作负载。
// 抽象化 Flink 任务中用于处理数据流和普通消息（如数据记录、Checkpoint Barrier、Watermark 等）的逻辑
// 定义主工作负载： Flink 的任务（如 SourceStreamTask、TwoInputTask 等）会实现这个接口。在邮箱循环中，当没有高优先级的控制 Mail 需要处理时，MailboxProcessor 就会重复调用 runDefaultAction 方法来处理数据。
// 实现协作控制： 通过 Controller 接口，MailboxDefaultAction 可以在运行时通知 MailboxProcessor 任务的执行状态，例如：数据源耗尽、需要等待输入等，从而暂停或结束主循环。


@Internal
public interface MailboxDefaultAction {

    /**
     * This method implements the default action of the mailbox loop (e.g. processing one event from
     * the input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the default action
     *     and the mailbox loop.
     * @throws Exception on any problems in the action.
     */
    // 执行默认操作。
    // 这是任务主工作负载的具体实现。它应该是一个非阻塞的操作（例如，从输入队列中拉取一个或少量事件进行处理）。
    void runDefaultAction(Controller controller) throws Exception;

    /** Represents the suspended state of a {@link MailboxDefaultAction}, ready to resume. */
    // Suspension 接口代表了默认操作被暂停后的状态，提供了恢复执行的方法。
    @Internal
    interface Suspension {
       //恢复执行
        /** Resume execution of the default action. */
        void resume();
    }

    /**
     * This controller is a feedback interface for the default action to interact with the mailbox
     * execution. In particular, it offers ways to signal that the execution of the default action
     * should be finished or temporarily suspended.
     */
    // Controller 是 MailboxDefaultAction 用来向 MailboxProcessor 反馈控制信号的接口。
    @Internal
    interface Controller {

        /**
         * This method must be called to end the stream task when all actions for the tasks have
         * been performed. This method can be invoked from any thread.
         */
        // 所有操作完成。当任务的默认操作（数据处理）确定没有更多工作需要执行时（例如，批处理模式下的数据源已读完），
        // 调用此方法通知 MailboxProcessor 结束整个流任务的执行。可从任何线程调用。
        void allActionsCompleted();

        /**
         * Calling this method signals that the mailbox-thread should (temporarily) stop invoking
         * the default action, e.g. because there is currently no input available. This method must
         * be invoked from the mailbox-thread only!
         *
         * @param suspensionPeriodTimer started (ticking) {@link PeriodTimer} that measures how long
         *     the default action was suspended/idling. If mailbox loop is busy processing mails,
         *     this timer should be paused for the time required to process the mails.
         */
        // 暂停默认操作（带计时器）。用于临时停止 runDefaultAction 的调用。通常在任务遇到空闲（如输入缓冲区为空）时调用。
        // suspensionPeriodTimer 用于精确测量暂停/空闲时间，这在指标统计中很重要。
        // 必须从邮箱线程调用。
        Suspension suspendDefaultAction(PeriodTimer suspensionPeriodTimer);

        /**
         * Same as {@link #suspendDefaultAction(PeriodTimer)} but without any associated timer
         * measuring the idle time.
         */
        // 暂停默认操作（无计时器）。
        // 功能同上，但没有传入用于测量空闲时间的计时器。必须从邮箱线程调用。
        Suspension suspendDefaultAction();
    }
}
