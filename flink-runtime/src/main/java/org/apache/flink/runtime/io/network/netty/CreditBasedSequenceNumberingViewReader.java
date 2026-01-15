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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound handler about
 * non-emptiness, similar to the {@link LocalInputChannel}.
 */
// 实现**基于信用的流量控制（Credit-based Flow Control）**的核心类。
// 它作为 NetworkSequenceViewReader 接口的具体实现，运行在发送端的 Netty 线程池中。
// 该类的主要作用是**“有节制地拉取数据”**。它在 ResultSubpartitionView（数据源）和 Netty 发送队列之间增加了一个“阀门”：
// 信用控制：只有当下游发送了“信用（Credit）”，即告知自己有空闲的接收 Buffer 时，该类才允许从子分区拉取数据。
// 状态同步：它实时监控子分区的可用性，并将自己注册到 Netty 的 PartitionRequestQueue 中，等待轮询发送。
// 事件优先：对于控制事件（如 Barrier），它可以无视信用限制优先下发。

class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {
    // 内部锁对象
    // 用于同步分区的请求和创建过程，确保 subpartitionView 的初始化是线程安全的。
    private final Object requestLock = new Object();
    // 下游接收端 InputChannel 的唯一标识
    private final InputChannelID receiverId;
    // Netty 的发送队列。
    // 本类会将自己注册到这个队列中，由队列统一调度发送。
    private final PartitionRequestQueue requestQueue;
    // 初始信用值
    private final int initialCredit;

    /**
     * Cache of the index of the only subpartition if the underlining {@link ResultSubpartitionView}
     * only consumes one subpartition, or -1 otherwise.
     */
    // 缓存子分区索引。
    // 如果当前 Reader 只处理一个子分区，记录其 ID 以优化性能。
    private int subpartitionId;
    // 指向底层的子分区读取视图，真正的 Buffer 数据是从这里拿的。
    private volatile ResultSubpartitionView subpartitionView;
    // 当请求的分区尚未产生时，负责监听分区的创建事件。
    private volatile PartitionRequestListener partitionRequestListener;

    /**
     * The status indicating whether this reader is already enqueued in the pipeline for
     * transferring data or not.
     *
     * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
     * thread only since there is no synchronisation.
     */
    // 标记该 Reader 是否已经在 Netty 的待处理列表中，防止重复注册。
    private boolean isRegisteredAsAvailable = false;

    /** The number of available buffers for holding data on the consumer side. */
    // 最核心属性。表示当前剩余的可发送配额。
    // 每发送一个数据 Buffer 减 1，收到下游反馈加N。
    private int numCreditsAvailable;

    CreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
        checkArgument(initialCredit >= 0, "Must be non-negative.");

        this.receiverId = receiverId;
        this.initialCredit = initialCredit;
        this.numCreditsAvailable = initialCredit;
        this.requestQueue = requestQueue;
        this.subpartitionId = -1;
    }
    // 下游任务（Consumer）通过 Netty 向本节点请求数据时的入口函数。
    // 它的核心逻辑是：如果上游数据分区（Partition）已经准备好了，就直接建立读取连接；如果还没准备好，就注册一个监听器等着。
    @Override
    public void requestSubpartitionViewOrRegisterListener(
            ResultPartitionProvider partitionProvider, // partitionProvider：通常是 ResultPartitionManager，负责查找和管理本地分区。
            ResultPartitionID resultPartitionId, // 请求的目标分区唯一标识。
            ResultSubpartitionIndexSet subpartitionIndexSet) // 请求的子分区索引集合（可能请求一个或多个子分区）
            throws IOException {
        synchronized (requestLock) {
            // 确保这个 Reader 实例是干净的。
            // 如果 subpartitionView 或监听器已经存在，说明该请求已经处理过，直接抛出异常防止重复请求。
            checkState(subpartitionView == null, "Subpartitions already requested");
            checkState(
                    partitionRequestListener == null, "Partition request listener already created");
            // 如果目标分区现在不存在（例如上游 Task 还没启动），这个监听器会被注册到管理器中，等分区一旦被上游 Task 注册，就会回调这个监听器来通知 Reader。
            partitionRequestListener =
                    new NettyPartitionRequestListener(
                            partitionProvider, this, subpartitionIndexSet, resultPartitionId);
            // The partition provider will create subpartitionView if resultPartition is
            // registered, otherwise it will register a listener of partition request to the result
            // partition manager.
            // 尝试获取视图或注册监听。
            Optional<ResultSubpartitionView> subpartitionViewOptional =
                    partitionProvider.createSubpartitionViewOrRegisterListener(
                            resultPartitionId,
                            subpartitionIndexSet,
                            this,
                            partitionRequestListener);
            // 如果分区已存在：立即创建一个 ResultSubpartitionView 并返回。
            if (subpartitionViewOptional.isPresent()) {
                this.subpartitionView = subpartitionViewOptional.get();
                if (subpartitionIndexSet.size() == 1) {
                    // 如果请求的只有一个子分区，将该索引记录在 subpartitionId 变量中，方便后续读取时快速定位。
                    subpartitionId = subpartitionIndexSet.values().iterator().next();
                }
            // 如果分区不存在：将刚才创建的 partitionRequestListener 挂载到该 Partition ID 的等待队列下，返回 Optional.empty()
            } else {
                // If the subpartitionView is not exist, it means that the requested partition is
                // not registered.
                return;
            }
        }
        // 激活数据传输。
        notifyDataAvailable(subpartitionView);
        // 告诉 Netty 的 PartitionRequestQueue，一个新的 Reader 诞生了。
        // 由于 Flink 是 Credit-based 模式，Reader 创建后会触发积压量（Backlog）的同步，让下游知道该申请多少 Credit。
        requestQueue.notifyReaderCreated(this);
    }

    @Override
    public void notifySubpartitionsCreated(
            ResultPartition partition, ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException {
        synchronized (requestLock) {
            checkState(subpartitionView == null, "Subpartitions already requested");
            subpartitionView = partition.createSubpartitionView(subpartitionIndexSet, this);
            if (subpartitionIndexSet.size() == 1) {
                subpartitionId = subpartitionIndexSet.values().iterator().next();
            }
        }

        notifyDataAvailable(subpartitionView);
        requestQueue.notifyReaderCreated(this);
    }

    @Override
    public void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        subpartitionView.notifyRequiredSegmentId(subpartitionId, segmentId);
    }

    @Override
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }
        subpartitionView.resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed() {
        subpartitionView.acknowledgeAllDataProcessed();
    }

    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        this.isRegisteredAsAvailable = isRegisteredAvailable;
    }

    @Override
    public boolean isRegisteredAsAvailable() {
        return isRegisteredAsAvailable;
    }

    /**
     * Returns true only if the next buffer is an event or the reader has both available credits and
     * buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getNextDataType(BufferAndBacklog)}, such
     *     that {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     */
    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        return subpartitionView.getAvailabilityAndBacklog(numCreditsAvailable > 0);
    }

    /**
     * Returns the {@link org.apache.flink.runtime.io.network.buffer.Buffer.DataType} of the next
     * buffer in line.
     *
     * <p>Returns the next data type only if the next buffer is an event or the reader has both
     * available credits and buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getAvailabilityAndBacklog()}, such that
     *     {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     * @param bufferAndBacklog current buffer and backlog including information about the next
     *     buffer
     * @return the next data type if the next buffer can be pulled immediately or {@link
     *     Buffer.DataType#NONE}
     */
    private Buffer.DataType getNextDataType(BufferAndBacklog bufferAndBacklog) {
        final Buffer.DataType nextDataType = bufferAndBacklog.getNextDataType();
        if (numCreditsAvailable > 0 || nextDataType.isEvent()) {
            return nextDataType;
        }
        return Buffer.DataType.NONE;
    }

    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        subpartitionView.notifyNewBufferSize(newBufferSize);
    }

    @Override
    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        requestQueue.notifyPartitionRequestTimeout(partitionRequestListener);
        this.partitionRequestListener = null;
    }

    @VisibleForTesting
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    @VisibleForTesting
    ResultSubpartitionView.AvailabilityWithBacklog hasBuffersAvailable() {
        return subpartitionView.getAvailabilityAndBacklog(true);
    }

    @Override
    public int peekNextBufferSubpartitionId() throws IOException {
        if (subpartitionId >= 0) {
            return subpartitionId;
        }
        return subpartitionView.peekNextBufferSubpartitionId();
    }

    @Nullable
    @Override
    public BufferAndAvailability getNextBuffer() throws IOException {
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
                throw new IllegalStateException("no credit available");
            }

            final Buffer.DataType nextDataType = getNextDataType(next);
            return new BufferAndAvailability(
                    next.buffer(), nextDataType, next.buffersInBacklog(), next.getSequenceNumber());
        } else {
            return null;
        }
    }

    @Override
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (partitionRequestListener != null) {
            partitionRequestListener.releaseListener();
        }
        subpartitionView.releaseAllResources();
    }
    // 实现了从“数据存储层”到“网络发送层”的跨线程通知。
    // 将当前的 CreditBasedSequenceNumberingViewReader 实例（即 this）添加到一个**“活跃读取器队列”**中。
    @Override
    public void notifyDataAvailable(ResultSubpartitionView view) {
        requestQueue.notifyReaderNonEmpty(this);
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        notifyDataAvailable(this.subpartitionView);
    }

    @Override
    public String toString() {
        return "CreditBasedSequenceNumberingViewReader{"
                + "requestLock="
                + requestLock
                + ", receiverId="
                + receiverId
                + ", numCreditsAvailable="
                + numCreditsAvailable
                + ", isRegisteredAsAvailable="
                + isRegisteredAsAvailable
                + '}';
    }
}
