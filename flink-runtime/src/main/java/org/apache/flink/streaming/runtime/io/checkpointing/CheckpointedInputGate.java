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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * The {@link CheckpointedInputGate} uses {@link CheckpointBarrierHandler} to handle incoming {@link
 * CheckpointBarrier} from the {@link InputGate}.
 */
// 核心作用是将底层的网络输入（InputGate）与 Flink 的 Checkpoint 机制（CheckpointBarrierHandler）集成在一起，实现数据的拉取、事件分发以及 Checkpoint Barrier 的处理
// 容错封装： 它封装了标准的 InputGate，拦截所有从网络接收到的 **Barrier（屏障）**和 Cancel Marker（取消标记），并将它们转发给 CheckpointBarrierHandler（这是实现对齐或非对齐 Checkpoint 逻辑的地方）。
// 数据/事件分发： 它从 InputGate 持续拉取 BufferOrEvent。如果是数据 Buffer，它会将其传递给下游（例如 StreamTaskNetworkInput）并通知 barrierHandler 累加对齐期间处理的字节数；如果是控制事件，它会根据事件类型进行专门处理或转发。
// 它是 Task 内部的网络输入组件，专门用于处理和协调 Checkpoint Barrier，确保 Flink 的容错语义（如一致性）在数据传输过程中得到正确维护。


@Internal
public class CheckpointedInputGate implements PullingAsyncDataInput<BufferOrEvent>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointedInputGate.class);
    // Checkpoin 屏障处理器。
    // 核心组件，负责实现具体的 Checkpoint 协议（如对齐、阻塞或非对齐逻辑），由它来决定如何处理 Checkpoint Barrier 和 Cancel Marker。
    private final CheckpointBarrierHandler barrierHandler;
    // 上游恢复追踪器。 用于追踪上游所有输入通道是否都已完成状态恢复
    private final UpstreamRecoveryTracker upstreamRecoveryTracker;

    /** The gate that the buffer draws its input from. */
    // 底层网络输入门。 原始的网络 I/O 组件，负责从网络接收 Buffer 和 Event。
    private final InputGate inputGate;
    // Mailbox 执行器。 用于将优先级网络事件提交给 Task 的 Mailbox 线程执行，以避免 Task 主线程阻塞在 I/O 时错过重要事件。
    private final MailboxExecutor mailboxExecutor;

    /** Indicate end of the input. */
    // 输入结束标志
    private boolean isFinished;

    /**
     * Creates a new checkpoint stream aligner.
     *
     * <p>The aligner will allow only alignments that buffer up to the given number of bytes. When
     * that number is exceeded, it will stop the alignment and notify the task that the checkpoint
     * has been cancelled.
     *
     * @param inputGate The input gate to draw the buffers and events from.
     * @param barrierHandler Handler that controls which channels are blocked.
     */
    public CheckpointedInputGate(
            InputGate inputGate,
            CheckpointBarrierHandler barrierHandler,
            MailboxExecutor mailboxExecutor) {
        this(inputGate, barrierHandler, mailboxExecutor, UpstreamRecoveryTracker.NO_OP);
    }

    public CheckpointedInputGate(
            InputGate inputGate,
            CheckpointBarrierHandler barrierHandler,
            MailboxExecutor mailboxExecutor,
            UpstreamRecoveryTracker upstreamRecoveryTracker) {
        this.inputGate = inputGate;
        this.barrierHandler = barrierHandler;
        this.mailboxExecutor = mailboxExecutor;
        this.upstreamRecoveryTracker = upstreamRecoveryTracker;

        waitForPriorityEvents(inputGate, mailboxExecutor);
    }

    /**
     * Eagerly pulls and processes all priority events. Must be called from task thread.
     *
     * <p>Basic assumption is that no priority event needs to be handled by the {@link
     * StreamTaskNetworkInput}.
     */
    private void processPriorityEvents() throws IOException, InterruptedException {
        // check if the priority event is still not processed (could have been pulled before mail
        // was being executed)
        boolean hasPriorityEvent = inputGate.getPriorityEventAvailableFuture().isDone();
        while (hasPriorityEvent) {
            // process as many priority events as possible
            final Optional<BufferOrEvent> bufferOrEventOpt = pollNext();
            if (!bufferOrEventOpt.isPresent()) {
                break;
            }
            final BufferOrEvent bufferOrEvent = bufferOrEventOpt.get();
            checkState(bufferOrEvent.hasPriority(), "Should only poll priority events");
            hasPriorityEvent = bufferOrEvent.morePriorityEvents();
        }

        // re-enqueue mail to process future priority events
        waitForPriorityEvents(inputGate, mailboxExecutor);
    }
    // 等待优先级事件
    // 当底层 InputGate 收到优先级事件时（如恢复状态事件），此回调会被触发
    private void waitForPriorityEvents(InputGate inputGate, MailboxExecutor mailboxExecutor) {
        final CompletableFuture<?> priorityEventAvailableFuture =
                inputGate.getPriorityEventAvailableFuture();
        assertNoException(
                priorityEventAvailableFuture.thenRun(
                        () -> {
                            try {
                                mailboxExecutor.execute(
                                        this::processPriorityEvents,
                                        "process priority event @ gate %s",
                                        inputGate);
                            } catch (RejectedExecutionException ex) {
                                LOG.debug(
                                        "Ignored RejectedExecutionException in CheckpointedInputGate.waitForPriorityEvents");
                            }
                        }));
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return inputGate.getAvailableFuture();
    }
    // 拉取下一个 Buffer 或 Event
    // 1. 从 inputGate 拉取下一个 BufferOrEvent。
    // 2. 如果拉取到 Buffer，通知 barrierHandler.addProcessedBytes() 累加对齐期间处理的字节数，并返回 Buffer。
    // 3. 如果拉取到 Event，调用 handleEvent() 进行处理。
    // 4. 如果没有数据，调用 handleEmptyBuffer() 检查是否已完成
    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
        Optional<BufferOrEvent> next = inputGate.pollNext();

        if (!next.isPresent()) {
            return handleEmptyBuffer();
        }

        BufferOrEvent bufferOrEvent = next.get();

        if (bufferOrEvent.isEvent()) {
            return handleEvent(bufferOrEvent);
        } else if (bufferOrEvent.isBuffer()) {
            /**
             * https://issues.apache.org/jira/browse/FLINK-19537 This is not entirely true, as it's
             * ignoring the buffer/bytes accumulated in the record deserializers. If buffer is
             * processed here, it doesn't mean it was fully processed (so we can over estimate the
             * amount of processed bytes). On the other hand some records/bytes might be processed
             * without polling anything from this {@link CheckpointedInputGate} (underestimating the
             * amount of processed bytes). All in all this should have been calculated on the {@link
             * StreamTaskNetworkInput} level, where we have an access to the records deserializers.
             * However the current is on average accurate and it might be just good enough (at least
             * for the time being).
             */
            barrierHandler.addProcessedBytes(bufferOrEvent.getBuffer().getSize());
        }
        return next;
    }
    // 处理拦截到的网络事件
    private Optional<BufferOrEvent> handleEvent(BufferOrEvent bufferOrEvent) throws IOException {
        Class<? extends AbstractEvent> eventClass = bufferOrEvent.getEvent().getClass();
        if (eventClass == CheckpointBarrier.class) {
            CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
            barrierHandler.processBarrier(checkpointBarrier, bufferOrEvent.getChannelInfo(), false);
        } else if (eventClass == CancelCheckpointMarker.class) {
            barrierHandler.processCancellationBarrier(
                    (CancelCheckpointMarker) bufferOrEvent.getEvent(),
                    bufferOrEvent.getChannelInfo());
        } else if (eventClass == EndOfData.class) {
            inputGate.acknowledgeAllRecordsProcessed(bufferOrEvent.getChannelInfo());
        } else if (eventClass == EndOfPartitionEvent.class) {
            barrierHandler.processEndOfPartition(bufferOrEvent.getChannelInfo());
        } else if (eventClass == EventAnnouncement.class) {
            EventAnnouncement eventAnnouncement = (EventAnnouncement) bufferOrEvent.getEvent();
            AbstractEvent announcedEvent = eventAnnouncement.getAnnouncedEvent();
            checkState(
                    announcedEvent instanceof CheckpointBarrier,
                    "Only CheckpointBarrier announcement are currently supported, but found [%s]",
                    announcedEvent);
            CheckpointBarrier announcedBarrier = (CheckpointBarrier) announcedEvent;
            barrierHandler.processBarrierAnnouncement(
                    announcedBarrier,
                    eventAnnouncement.getSequenceNumber(),
                    bufferOrEvent.getChannelInfo());
        } else if (bufferOrEvent.getEvent().getClass() == EndOfChannelStateEvent.class) {
            upstreamRecoveryTracker.handleEndOfRecovery(bufferOrEvent.getChannelInfo());
        }
        return Optional.of(bufferOrEvent);
    }

    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        return barrierHandler.getAllBarriersReceivedFuture(checkpointId);
    }

    private Optional<BufferOrEvent> handleEmptyBuffer() {
        if (inputGate.isFinished()) {
            isFinished = true;
        }

        return Optional.empty();
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        return inputGate.hasReceivedEndOfData();
    }

    /**
     * Cleans up all internally held resources.
     *
     * @throws IOException Thrown if the cleanup of I/O resources failed.
     */
    public void close() throws IOException {
        barrierHandler.close();
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    /**
     * Gets the ID defining the current pending, or just completed, checkpoint.
     *
     * @return The ID of the pending of completed checkpoint.
     */
    @VisibleForTesting
    long getLatestCheckpointId() {
        return barrierHandler.getLatestCheckpointId();
    }

    /**
     * Gets the time that the latest alignment took, in nanoseconds. If there is currently an
     * alignment in progress, it will return the time spent in the current alignment so far.
     *
     * @return The duration in nanoseconds
     */
    @VisibleForTesting
    long getAlignmentDurationNanos() {
        return barrierHandler.getAlignmentDurationNanos();
    }

    /**
     * @return the time that elapsed, in nanoseconds, between the creation of the latest checkpoint
     *     and the time when it's first {@link CheckpointBarrier} was received by this {@link
     *     InputGate}.
     */
    @VisibleForTesting
    long getCheckpointStartDelayNanos() {
        return barrierHandler.getCheckpointStartDelayNanos();
    }

    /** @return number of underlying input channels. */
    public int getNumberOfInputChannels() {
        return inputGate.getNumberOfInputChannels();
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return barrierHandler.toString();
    }

    public InputChannel getChannel(int channelIndex) {
        return inputGate.getChannel(channelIndex);
    }

    public List<InputChannelInfo> getChannelInfos() {
        return inputGate.getChannelInfos();
    }

    public boolean allChannelsRecovered() {
        return upstreamRecoveryTracker.allChannelsRecovered();
    }

    @VisibleForTesting
    CheckpointBarrierHandler getCheckpointBarrierHandler() {
        return barrierHandler;
    }
}
