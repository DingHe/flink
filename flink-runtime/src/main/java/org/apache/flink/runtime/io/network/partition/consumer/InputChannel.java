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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 *
 * <p>For each channel, the consumption life cycle is as follows:
 *
 * <ol>
 *   <li>{@link #requestSubpartitions()}
 *   <li>{@link #getNextBuffer()}
 *   <li>{@link #releaseAllResources()}
 * </ol>
 */
// InputChannel 表示 Task 的一个输入通道，是下游 Task 从上游获取数据的唯一入口。
// 一个上游 ResultPartition 由多个 ResultSubpartition 构成
// 下游每一个 InputChannel 消费一个或多个子分区（ResultSubpartitionView）
// 数据流从上游 Task → Netty → 下游 Task，就是经由 InputChannel
// InputChannel = 下游 Task 读取上游某几个 Subpartition 的“吸管”
public abstract class InputChannel {
    /** The info of the input channel to identify it globally within a task. */
    // 全局唯一标识一个 InputChannel
    protected final InputChannelInfo channelInfo;

    /** The parent partition of the subpartitions consumed by this channel. */
    // 这个 InputChannel 所要消费的上游 ResultPartition 的 ID（全局唯一）
    // 输入通道向上游发起 partition 请求
    // 反向事件（如失败、重试）传递
    protected final ResultPartitionID partitionId;

    /** The indexes of the subpartitions consumed by this channel. */
    // 表示 InputChannel 将要消费 哪些子分区
    protected final ResultSubpartitionIndexSet consumedSubpartitionIndexSet;
    // nputGate 是多个 InputChannel 的“总控”。
    // select 哪个 channel 有数据
    protected final SingleInputGate inputGate;

    // - Asynchronous error notification --------------------------------------
    // 异步错误通知机制。
    private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

    // - Partition request backoff --------------------------------------------

    /** The initial backoff (in ms). */
    // 初始退避时间（毫秒）。
    // 分区请求失败后第一次重试等待的最小时间。
    protected final int initialBackoff;

    /** The maximum backoff (in ms). */
    // 最大退避时间（毫秒）。
    // 分区请求重试等待的最大时间，用于限制指数退避的时间上限。
    protected final int maxBackoff;
    // 输入字节数计数器。
    // 用于 Flink Metrics，统计此通道接收到的字节总数。
    protected final Counter numBytesIn;
    // 输入 Buffer 计数器。
    // 用于 Flink Metrics，统计此通道接收到的 Buffer 总数。
    protected final Counter numBuffersIn;

    /**
     * The index of the subpartition if {@link #consumedSubpartitionIndexSet} contains only one
     * subpartition, or -1.
     */
    // 单一子分区 ID。如果 consumedSubpartitionIndexSet 只包含一个子分区，则存储该 ID；否则为 -1。
    // 用于优化单子分区消费场景的逻辑。
    private final int subpartitionId;

    /** The current backoff (in ms). */
    // 当前退避时间（毫秒）。
    // 用于指数退避逻辑，存储当前等待重试的时间。
    protected int currentBackoff;

    protected InputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn) {

        checkArgument(channelIndex >= 0);

        int initial = initialBackoff;
        int max = maxBackoff;

        checkArgument(initial >= 0 && initial <= max);

        this.inputGate = checkNotNull(inputGate);
        this.channelInfo = new InputChannelInfo(inputGate.getGateIndex(), channelIndex);
        this.partitionId = checkNotNull(partitionId);

        this.consumedSubpartitionIndexSet = consumedSubpartitionIndexSet;
        this.subpartitionId =
                consumedSubpartitionIndexSet.size() > 1
                        ? -1
                        : consumedSubpartitionIndexSet.values().iterator().next();

        this.initialBackoff = initial;
        this.maxBackoff = max;
        this.currentBackoff = 0;

        this.numBytesIn = numBytesIn;
        this.numBuffersIn = numBuffersIn;
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    /** Returns the index of this channel within its {@link SingleInputGate}. */
    public int getChannelIndex() {
        return channelInfo.getInputChannelIdx();
    }

    /**
     * Returns the info of this channel, which uniquely identifies the channel in respect to its
     * operator instance.
     */
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public ResultSubpartitionIndexSet getConsumedSubpartitionIndexSet() {
        return consumedSubpartitionIndexSet;
    }

    /**
     * After sending a {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} of
     * exactly-once mode, the upstream will be blocked and become unavailable. This method tries to
     * unblock the corresponding upstream and resume data consumption.
     */
    // 恢复消费。
    // 抽象方法，在 Checkpoint 完成后，用于解除上游的阻塞状态，恢复数据消费。
    public abstract void resumeConsumption() throws IOException;

    /**
     * When received {@link EndOfData} from one channel, it need to acknowledge after this event get
     * processed.
     */
    // 确认记录已处理。
    // 当 Task 完成处理 EndOfData 事件后，通知上游（如果是流批一体的场景）所有记录已处理。
    public abstract void acknowledgeAllRecordsProcessed() throws IOException;

    /**
     * Notifies the owning {@link SingleInputGate} that this channel became non-empty.
     *
     * <p>This is guaranteed to be called only when a Buffer was added to a previously empty input
     * channel. The notion of empty is atomically consistent with the flag {@link
     * BufferAndAvailability#moreAvailable()} when polling the next buffer from this channel.
     *
     * <p><b>Note:</b> When the input channel observes an exception, this method is called
     * regardless of whether the channel was empty before. That ensures that the parent InputGate
     * will always be notified about the exception.
     */
    // 当 channel 原本为空，现在变成“有数据可读”时，必须通知 inputGate
    protected void notifyChannelNonEmpty() {
        inputGate.notifyChannelNonEmpty(this);
    }
    //当 channel 收到优先事件（如 barrier）时通知 InputGate
    // InputGate 会优先调度 barrier
    public void notifyPriorityEvent(int priorityBufferNumber) {
        inputGate.notifyPriorityEvent(this, priorityBufferNumber);
    }
    // 供 LocalInputChannel 覆盖，当本地缓冲更新时通知 Netty 或上游，不同实现不同。默认空
    protected void notifyBufferAvailable(int numAvailableBuffers) throws IOException {}

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    /**
     * Requests the subpartitions specified by {@link #partitionId} and {@link
     * #consumedSubpartitionIndexSet}.
     */
    // 请求子分区。抽象方法，触发向远程/本地上游 Task 发送请求，开始拉取数据。
    // 具体的实现（如 Netty 请求）由子类完成。
    abstract void requestSubpartitions() throws IOException, InterruptedException;

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    // 预先查看下一个 Buffer 的子分区 ID。
    // 用于在多子分区消费场景中，帮助 InputGate 判断下一个 Buffer 属于哪个子分区。如果只有单个子分区，则直接返回 subpartitionId
    public int peekNextBufferSubpartitionId() throws IOException {
        if (subpartitionId >= 0) {
            return subpartitionId;
        }
        return peekNextBufferSubpartitionIdInternal();
    }

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    // 内部预先查看。
    // 抽象方法，由子类实现具体的查看逻辑
    protected abstract int peekNextBufferSubpartitionIdInternal() throws IOException;

    /**
     * Returns the next buffer from the consumed subpartitions or {@code Optional.empty()} if there
     * is no data to return.
     */
    // 获取下一个 Buffer。
    // 抽象方法，从内部队列（如网络接收队列）中获取下一个可用的 Buffer 或 Event。返回 BufferAndAvailability 封装了数据和后续可用性信息。
    public abstract Optional<BufferAndAvailability> getNextBuffer()
            throws IOException, InterruptedException;

    /**
     * Called by task thread when checkpointing is started (e.g., any input channel received
     * barrier).
     */
    // 检查点开始。
    // 当接收到 CheckpointBarrier 时，通知通道（子类可重写此方法来处理 Barrier）
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {}

    /** Called by task thread on cancel/complete to clean-up temporary data. */
    // 检查点停止。在取消/完成 Checkpoint 时调用，用于清理临时数据。
    public void checkpointStopped(long checkpointId) {}

    public void convertToPriorityEvent(int sequenceNumber) throws IOException {}

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    /**
     * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
     *
     * <p><strong>Important</strong>: The producing task has to be running to receive backwards
     * events. This means that the result type needs to be pipelined and the task logic has to
     * ensure that the producer will wait for all backwards events. Otherwise, this will lead to an
     * Exception at runtime.
     */
    // 发送 Task 事件。
    // 抽象方法，用于向上游 Task 发送特定的控制事件（TaskEvent），通常用于反压控制或用户自定义事件。
    abstract void sendTaskEvent(TaskEvent event) throws IOException;

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    abstract boolean isReleased();

    /** Releases all resources of the channel. */
    abstract void releaseAllResources() throws IOException;

    abstract void announceBufferSize(int newBufferSize);

    abstract int getBuffersInUseCount();

    // ------------------------------------------------------------------------
    // Error notification
    // ------------------------------------------------------------------------

    /**
     * Checks for an error and rethrows it if one was reported.
     *
     * <p>Note: Any {@link PartitionException} instances should not be transformed and make sure
     * they are always visible in task failure cause.
     */
    protected void checkError() throws IOException {
        final Throwable t = cause.get();

        if (t != null) {
            if (t instanceof CancelTaskException) {
                throw (CancelTaskException) t;
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        }
    }

    /**
     * Atomically sets an error for this channel and notifies the input gate about available data to
     * trigger querying this channel by the task thread.
     */
    protected void setError(Throwable cause) {
        if (this.cause.compareAndSet(null, checkNotNull(cause))) {
            // Notify the input gate.
            notifyChannelNonEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // Partition request exponential backoff
    // ------------------------------------------------------------------------

    /** Returns the current backoff in ms. */
    protected int getCurrentBackoff() {
        return currentBackoff <= 0 ? 0 : currentBackoff;
    }

    /**
     * Increases the current backoff and returns whether the operation was successful.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    protected boolean increaseBackoff() {
        // Backoff is disabled
        if (initialBackoff == 0) {
            return false;
        }

        if (currentBackoff == 0) {
            // This is the first time backing off
            currentBackoff = initialBackoff;

            return true;
        }

        // Continue backing off
        else if (currentBackoff < maxBackoff) {
            currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

            return true;
        }

        // Reached maximum backoff
        return false;
    }

    // ------------------------------------------------------------------------
    // Metric related method
    // ------------------------------------------------------------------------

    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return 0;
    }

    /**
     * Notify the upstream the id of required segment that should be sent to netty connection.
     *
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) throws IOException {}

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and a flag indicating availability of further buffers, and
     * the backlog length indicating how many non-event buffers are available in the subpartitions.
     */
    public static final class BufferAndAvailability {

        private final Buffer buffer;
        private final Buffer.DataType nextDataType;
        private final int buffersInBacklog;
        private final int sequenceNumber;

        public BufferAndAvailability(
                Buffer buffer,
                Buffer.DataType nextDataType,
                int buffersInBacklog,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.nextDataType = checkNotNull(nextDataType);
            this.buffersInBacklog = buffersInBacklog;
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean moreAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public boolean morePriorityEvents() {
            return nextDataType.hasPriority();
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean hasPriority() {
            return buffer.getDataType().hasPriority();
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "BufferAndAvailability{"
                    + "buffer="
                    + buffer
                    + ", nextDataType="
                    + nextDataType
                    + ", buffersInBacklog="
                    + buffersInBacklog
                    + ", sequenceNumber="
                    + sequenceNumber
                    + '}';
        }
    }

    void setup() throws IOException {}
}
