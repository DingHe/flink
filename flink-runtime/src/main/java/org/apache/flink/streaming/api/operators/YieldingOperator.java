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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;

/**
 * A V1 operator that needs access to the {@link MailboxExecutor} should implement this interface.
 * Note, this interface is not needed when using {@link StreamOperatorFactory} or {@link
 * AbstractStreamOperatorV2} as those have access to the {@link MailboxExecutor} via {@link
 * StreamOperatorParameters#getMailboxExecutor()}
 */
// YieldingOperator 是 Flink 任务执行模型（Task Execution Model） 演进过程中的一个特定接口，它与 Flink 的Mailbox 模型紧密相关
// Mailbox 模型： Flink 现代的任务执行模型被称为 Mailbox 模型。在这个模型中，所有的输入数据处理、计时器事件、状态访问、Checkpoint Barrier，以及控制消息（如调度、用户代码发送的异步结果等），
// 都被抽象为“邮件”并放入一个统一的“邮箱”(Mailbox)中，由一个单独的线程循环处理。
// MailboxExecutor： 它是用于向这个“邮箱”中提交任务（或称为“邮件”）的接口。
// V1 Operator： 指的是 Flink 较早的算子实现方式。在 Mailbox 模型被引入后，一些 V1 算子如果需要利用 Mailbox 模型的特性（例如，在算子内部提交一个可放弃执行权（Yielding） 的任务到 Mailbox 中），
// 就需要这个接口来获取 MailboxExecutor 实例。
@Internal
public interface YieldingOperator<OUT> extends StreamOperator<OUT> {
    // 设置 Mailbox 执行器
    void setMailboxExecutor(MailboxExecutor mailboxExecutor);
}
