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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper class to let operators emit watermarks incrementally from mailbox. Instead of emitting
 * all the watermarks at once in a single {@code processWatermark} call, if a mail in mailbox is
 * present, the process of firing timers is interrupted and a continuation to finish it off later is
 * scheduled via a mailbox mail.
 *
 * <p>Note that interrupting firing timers can change order of some invocations. It is possible that
 * between firing timers, some records might be processed.
 */
// MailboxWatermarkProcessor 是 Flink 在启用**可拆分定时器（Splittable Timers）或非对齐检查点（Unaligned Checkpoints）**特性时使用的内部辅助类。
// Watermark 的增量处理： 改变了传统的 Watermark 处理方式。在 Watermark 到来时，它不再试图在一个方法调用中立即触发所有事件时间定时器。相反，它将 Watermark 的**推进（advance）和定时器触发（fire timers）**视为一个可能很耗时的操作。
// 避免阻塞 Mailbox： 当定时器触发过程过长时（例如，一次性有大量定时器到期），它允许 Watermark 推进过程中断，并将剩余的工作（继续推进 Watermark）重新调度为一个**新的 Mail（邮件）**提交回 MailboxExecutor
// 保证响应性和公平性： 通过这种增量和重新调度机制，它确保了 Flink 的单线程 MailboxProcessor 不会被长时间的定时器触发阻塞，从而可以及时处理其他高优先级或等待中的控制 Mail（如 Checkpoint Barrier、其他输入数据），提高了任务的整体响应性和公平性。
// 使用 Mailbox 机制来拆分 Watermark 推进和定时器触发的耗时工作，防止长时间阻塞算子的主事件循环
@Internal
public class MailboxWatermarkProcessor<OUT> {
    protected static final Logger LOG = LoggerFactory.getLogger(MailboxWatermarkProcessor.class);
    // 输出接口。
    // 用于将 Watermark 最终发送到下游算子。
    private final Output<StreamRecord<OUT>> output;
    // 邮箱执行器。
    // 用于将 Watermark 推进的剩余工作封装成 Mail，并重新提交到 Flink Task 的 Mailbox 中进行延迟执行。
    private final MailboxExecutor mailboxExecutor;
    //内部时间服务管理器。
    // 负责管理和执行所有事件时间定时器的核心组件。 Watermark 的推进和定时器的触发都是通过它来完成的。
    private final InternalTimeServiceManager<?> internalTimeServiceManager;
    /**
     * Flag to indicate whether a progress watermark is scheduled in the mailbox. This is used to
     * avoid duplicate scheduling in case we have multiple watermarks to process.
     */
    // 进度 Watermark 调度标志。
    // 一个布尔标志，用于指示是否已有一个 Watermark 推进的延续任务被调度在 Mailbox 中等待执行。
    // 这用于防止重复调度 Watermark 推进 Mail。
    private boolean progressWatermarkScheduled = false;
    // 最大输入 Watermark。
    // 用于跟踪和存储到目前为止接收到的所有 Watermark 中的最大值。这是因为 Watermark 必须单调递增。
    private Watermark maxInputWatermark = Watermark.UNINITIALIZED;

    public MailboxWatermarkProcessor(
            Output<StreamRecord<OUT>> output,
            MailboxExecutor mailboxExecutor,
            InternalTimeServiceManager<?> internalTimeServiceManager) {
        this.output = checkNotNull(output);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.internalTimeServiceManager = checkNotNull(internalTimeServiceManager);
    }
    // 外部 Watermark 入口。
    // 这是外部（如 AbstractStreamOperator.processWatermark）接收到新的 Watermark 时调用的入口方法。
    public void emitWatermarkInsideMailbox(Watermark mark) throws Exception {
        maxInputWatermark =
                new Watermark(Math.max(maxInputWatermark.getTimestamp(), mark.getTimestamp()));
        emitWatermarkInsideMailbox();
    }
    // 核心推进逻辑。
    // 负责实际的 Watermark 推进和调度。
    private void emitWatermarkInsideMailbox() throws Exception {
        // Try to progress min watermark as far as we can.
        // 尝试将 Watermark 推进到 maxInputWatermark 的值
        // 返回 true（表示 Watermark 已完全推进且所有定时器已触发），则通过 output.emitWatermark(maxInputWatermark) 将 Watermark 发送给下游
        if (internalTimeServiceManager.tryAdvanceWatermark(
                maxInputWatermark, mailboxExecutor::shouldInterrupt)) {
            // In case output watermark has fully progressed emit it downstream.
            output.emitWatermark(maxInputWatermark);
        } else if (!progressWatermarkScheduled) {
            // 则将剩余工作封装为一个 Mail，并使用 mailboxExecutor.execute(...) 提交回 Mailbox。
            // 这个 Mail 的逻辑是再次调用 emitWatermarkInsideMailbox()，以在下一个循环中继续推进 Watermark。
            progressWatermarkScheduled = true;
            // We still have work to do, but we need to let other mails to be processed first.
            mailboxExecutor.execute(
                    MailboxExecutor.MailOptions.deferrable(),
                    () -> {
                        progressWatermarkScheduled = false;
                        emitWatermarkInsideMailbox();
                    },
                    "emitWatermarkInsideMailbox");
        } else {
            // We're not guaranteed that MailboxProcessor is going to process all mails before
            // processing additional input, so the advanceWatermark could be called before the
            // previous watermark is fully processed.
            LOG.debug("emitWatermarkInsideMailbox is already scheduled, skipping.");
        }
    }
}
