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
@Internal
public interface MailboxDefaultAction {
  //用于数据流上的普通消息处理（如正常的数据记录，barrier）等
    /**
     * This method implements the default action of the mailbox loop (e.g. processing one event from
     * the input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the default action
     *     and the mailbox loop.
     * @throws Exception on any problems in the action.
     */
    void runDefaultAction(Controller controller) throws Exception;

    /** Represents the suspended state of a {@link MailboxDefaultAction}, ready to resume. */
    @Internal
    interface Suspension {  //代表 MailboxDefaultAction 的暂停状态，提供 resume() 方法来恢复默认操作
       //恢复执行
        /** Resume execution of the default action. */
        void resume();
    }

    /**
     * This controller is a feedback interface for the default action to interact with the mailbox
     * execution. In particular, it offers ways to signal that the execution of the default action
     * should be finished or temporarily suspended.
     */
    @Internal
    interface Controller {

        /**
         * This method must be called to end the stream task when all actions for the tasks have
         * been performed. This method can be invoked from any thread.
         */
        void allActionsCompleted(); //当任务所有操作都执行完成时，调用此方法结束流任务的执行

        /**
         * Calling this method signals that the mailbox-thread should (temporarily) stop invoking
         * the default action, e.g. because there is currently no input available. This method must
         * be invoked from the mailbox-thread only!
         *
         * @param suspensionPeriodTimer started (ticking) {@link PeriodTimer} that measures how long
         *     the default action was suspended/idling. If mailbox loop is busy processing mails,
         *     this timer should be paused for the time required to process the mails.
         */
        Suspension suspendDefaultAction(PeriodTimer suspensionPeriodTimer);//当没有输入事件需要处理时，可以调用此方法暂时停止默认操作。PeriodTimer 用来记录暂停的时间，确保处理邮件时暂停计时器

        /**
         * Same as {@link #suspendDefaultAction(PeriodTimer)} but without any associated timer
         * measuring the idle time.
         */
        Suspension suspendDefaultAction();//与上面的方法类似，但没有使用计时器来记录空闲时间
    }
}
