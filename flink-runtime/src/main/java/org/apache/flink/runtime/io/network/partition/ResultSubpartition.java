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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
// Flink 网络栈中数据传输的最小逻辑单元，它直接负责数据的缓冲和向单个下游消费者的发送。
// ResultPartition 的一个抽象子组件，一个 ResultPartition 包含多个 ResultSubpartition 实例。它的核心作用是：
// 数据的最终目的地（生产者侧）： 它是上游任务通过 ResultPartitionWriter 写入的记录和事件的最终排队（Queuing）点。每个子分区对应一个下游任务实例。
// 缓冲区管理： 负责管理其内部的缓冲区队列，接收来自上游任务的 BufferConsumer（代表一个或多个 Buffer），并将它们排队等待发送。
// 提供消费视图： 允许下游任务（消费者）通过创建 ResultSubpartitionView 来读取其排队的数据。
/** A single subpartition of a {@link ResultPartition} instance. */
public abstract class ResultSubpartition {

    // The error code when adding a buffer fails.
    // 添加缓冲区失败错误码。
    // 一个常量（-1），用于在 add() 操作失败时返回，表示无法添加缓冲区。
    public static final int ADD_BUFFER_ERROR_CODE = -1;

    /** The info of the subpartition to identify it globally within a task. */
    // 子分区信息。
    // 该子分区的全局标识信息，包括其所属的父分区索引和自身的子分区索引，用于在 Checkpoint 等场景中全局标识该通道。
    protected final ResultSubpartitionInfo subpartitionInfo;

    /** The parent partition this subpartition belongs to. */
    // 父分区引用。
    // 指向该子分区所属的 ResultPartition 实例，用于向上级报告状态（如：子分区被消费）
    protected final ResultPartition parent;

    // - Statistics ----------------------------------------------------------

    public ResultSubpartition(int index, ResultPartition parent) {
        this.parent = parent;
        this.subpartitionInfo = new ResultSubpartitionInfo(parent.getPartitionIndex(), index);
    }

    public ResultSubpartitionInfo getSubpartitionInfo() {
        return subpartitionInfo;
    }

    /** Gets the total numbers of buffers (data buffers plus events). */
    // 获取总 Buffer 数量（非同步）。
    // 获取该子分区写入过的 Buffer（包括数据 Buffer 和事件）的总数量，通常用于指标统计。
    protected abstract long getTotalNumberOfBuffersUnsafe();
    // 获取总字节数（非同步）。
    // 获取该子分区写入过的所有数据的总字节数，用于统计
    protected abstract long getTotalNumberOfBytesUnsafe();

    public int getSubPartitionIndex() {
        return subpartitionInfo.getSubPartitionIdx();
    }

    /** Notifies the parent partition about a consumed {@link ResultSubpartitionView}. */
    // 通知父分区被消费。
    // 当有消费者创建 ResultSubpartitionView 并开始消费时，此方法会通知父 ResultPartition
    protected void onConsumedSubpartition() {
        parent.onConsumedSubpartition(getSubPartitionIndex());
    }
    // 对齐屏障超时。
    // 当 Checkpoint 对齐操作超时时，上游调用此方法通知子分区，以便将其 Checkpoint 屏障转换为非对齐模式，防止任务阻塞。
    public abstract void alignedBarrierTimeout(long checkpointId) throws IOException;
    // 中止 Checkpoint。
    // 当 Checkpoint 过程失败时，通知子分区中止指定的 Checkpoint 屏障处理。
    public abstract void abortCheckpoint(long checkpointId, CheckpointException cause);

    // 添加缓冲区（简化版）。
    // 调用主 add 方法，partialRecordLength 默认为 0。
    @VisibleForTesting
    public final int add(BufferConsumer bufferConsumer) throws IOException {
        return add(bufferConsumer, 0);
    }

    /**
     * Adds the given buffer.
     *
     * <p>The request may be executed synchronously, or asynchronously, depending on the
     * implementation.
     *
     * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must
     * be in finished state. Because of the performance reasons, this is only enforced during the
     * data reading. Priority events can be added while the previous buffer consumer is still open,
     * in which case the open buffer consumer is overtaken.
     *
     * @param bufferConsumer the buffer to add (transferring ownership to this writer)
     * @param partialRecordLength the length of bytes to skip in order to start with a complete
     *     record, from position index 0 of the underlying {@cite MemorySegment}.
     * @return the preferable buffer size for this subpartition or {@link #ADD_BUFFER_ERROR_CODE} if
     *     the add operation fails.
     * @throws IOException thrown in case of errors while adding the buffer
     */
    // 添加缓冲区。
    // 上游任务通过此方法将包含数据的 BufferConsumer 实例添加到子分区队列
    public abstract int add(BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException;

    // 强制刷新。
    // 强制将队列中所有已添加但尚未被消费者读取的 Buffer 暴露给消费者。
    public abstract void flush();

    /**
     * Writing of data is finished.
     *
     * @return the size of data written for this subpartition inside of finish.
     */
    // 完成写入。
    // 标记上游任务已完成向该子分区的写入。
    // 通常会确保所有待处理的数据都被处理和可读，并可能返回完成时写入的数据大小。
    public abstract int finish() throws IOException;
    // 释放资源。
    // 释放该子分区占用的所有资源，如内存缓冲区、文件句柄等。
    public abstract void release() throws IOException;
    // 创建读取视图。
    // 为下游消费者创建一个 ResultSubpartitionView 实例，允许其开始读取数据。
    public abstract ResultSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) throws IOException;
    // 检查是否释放。返回该子分区是否已被释放。
    public abstract boolean isReleased();

    /** Gets the number of non-event buffers in this subpartition. */
    // 获取积压数据 Buffer 数量（非同步）。
    // 获取当前队列中等待读取的**非事件（Non-event）**数据 Buffer 的数量，用于流控。
    abstract int getBuffersInBacklogUnsafe();

    /**
     * Makes a best effort to get the current size of the queue. This method must not acquire locks
     * or interfere with the task and network threads in any way.
     */
    // 获取排队 Buffer 数（非同步）。
    // 在不需要加锁或高精度计时的场景下，快速获取当前队列中等待发送的 Buffer 数量。
    public abstract int unsynchronizedGetNumberOfQueuedBuffers();

    /** Get the current size of the queue. */
    // 获取排队 Buffer 数（同步）。
    // 获取当前队列中等待发送的 Buffer 数量，该操作通常是线程安全的。
    public abstract int getNumberOfQueuedBuffers();
    // 设置/调整缓冲区大小。
    // 通知子分区，新的期望缓冲区大小是多少，以便子分区可以相应地调整其分配和行为。
    public abstract void bufferSize(int desirableNewBufferSize);

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and the backlog length indicating how many non-event
     * buffers are available in the subpartition.
     */
    public static final class BufferAndBacklog {
        private final Buffer buffer;
        private final int buffersInBacklog;
        private final Buffer.DataType nextDataType;
        private final int sequenceNumber;

        public BufferAndBacklog(
                Buffer buffer,
                int buffersInBacklog,
                Buffer.DataType nextDataType,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.buffersInBacklog = buffersInBacklog;
            this.nextDataType = checkNotNull(nextDataType);
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean isDataAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean isEventAvailable() {
            return nextDataType.isEvent();
        }

        public Buffer.DataType getNextDataType() {
            return nextDataType;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public static BufferAndBacklog fromBufferAndLookahead(
                Buffer current, Buffer.DataType nextDataType, int backlog, int sequenceNumber) {
            return new BufferAndBacklog(current, backlog, nextDataType, sequenceNumber);
        }
    }
}
