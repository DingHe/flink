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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

// 为下游任务（消费者）提供一个统一的接口，用于从上游任务的单个或多个结果子分区（ResultSubpartition）中顺序、安全、高效地消费数据（Buffer 和 Event）
// 充当了消费者和**数据源（ResultSubpartition）**之间的桥梁。下游任务（如 InputChannel）通过这个视图来：
// 获取数据： 按需请求下一个 Buffer 或 Event。
// 流控（Flow Control）： 报告当前消费状态，检查数据可用性和待处理数据量（backlog）
/** A view to consume a {@link ResultSubpartition} instance. */
public interface ResultSubpartitionView {

    /**
     * Returns the next {@link Buffer} instance of this queue iterator.
     *
     * <p>If there is currently no instance available, it will return <code>null</code>. This might
     * happen for example when a pipelined queue producer is slower than the consumer or a spilled
     * queue needs to read in more data.
     *
     * <p><strong>Important</strong>: The consumer has to make sure that each buffer instance will
     * eventually be recycled with {@link Buffer#recycleBuffer()} after it has been consumed.
     */
    // 获取下一个 Buffer。这是核心消费方法。
    // 它返回下一个可用的数据块（Buffer）及其当前的待处理数据量（backlog）。
    // 如果当前没有数据，则返回 null。注意： 消费者必须确保在使用后调用 Buffer.recycleBuffer() 回收缓冲区。
    @Nullable
    BufferAndBacklog getNextBuffer() throws IOException;
    // 通知数据可用。
    // 用于将 ResultSubpartitionView 注册到外部调度器，当新的数据或事件写入子分区时，子分区会调用此方法，通知视图有数据可供读取。
    void notifyDataAvailable();

    // 通知优先级事件。默认方法。
    // 当优先级事件（如 Checkpoint Barrier 的宣告消息）到达时，通知视图。
    // priorityBufferNumber 可能指示当前队列中有多少个优先级 Buffer。
    default void notifyPriorityEvent(int priorityBufferNumber) {}
    // 释放所有资源。
    // 在消费完成、任务取消或失败时调用，用于清理该视图相关的内存、文件句柄或网络连接等所有资源。
    void releaseAllResources() throws IOException;
    // 检查是否释放。
    // 返回该视图是否已经释放了所有资源
    boolean isReleased();
    // 恢复消费。
    // 当消费端被暂停（例如由于反压或 Checkpoint 对齐）后，通过此方法恢复数据的读取和处理。
    void resumeConsumption();
    // 确认所有数据已处理。
    // 通知上游分区，下游任务已处理完所有数据记录（在接收到 EndOfData 事件之后），通常用于触发上游资源释放。
    void acknowledgeAllDataProcessed();

    /**
     * {@link ResultSubpartitionView} can decide whether the failure cause should be reported to
     * consumer as failure (primary failure) or {@link ProducerFailedException} (secondary failure).
     * Secondary failure can be reported only if producer (upstream task) is guaranteed to failover.
     *
     * <p><strong>BEWARE:</strong> Incorrectly reporting failure cause as primary failure, can hide
     * the root cause of the failure from the user.
     */
    // 获取失败原因。
    // 获取该视图底层的子分区发生的失败原因。
    // 它帮助消费端判断失败是主要故障（Primary Failure，需要报告给 JobManager）还是次要故障（Secondary Failure，如上游任务故障，下游只需转换为 ProducerFailedException）
    Throwable getFailureCause();

    /**
     * Get the availability and backlog of the view. The availability represents if the view is
     * ready to get buffer from it. The backlog represents the number of available data buffers.
     *
     * @param isCreditAvailable the availability of credits for this {@link ResultSubpartitionView}.
     * @return availability and backlog.
     */
    // 获取可用性和积压量。用于流控。
    // 返回一个包含：1. 视图是否可用 (isAvailable)；2. 视图中排队数据的数量 (backlog) 的对象。
    // isCreditAvailable 参数用于告知视图当前是否有足够的流控信用（Credit）来获取数据。
    AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable);
    // 获取排队缓冲区数量（非同步）。
    // 获取排队等待读取的缓冲区数量。由于是非同步方法，它提供了一个快速但可能不完全精确的值，用于非关键路径的检查或指标报告。
    int unsynchronizedGetNumberOfQueuedBuffers();
    // 获取排队缓冲区数量（同步）。
    // 获取排队等待读取的缓冲区数量，通常用于需要精确计数的场景。
    int getNumberOfQueuedBuffers();
    // 通知新的缓冲区大小。
    // 通知视图，其底层的网络缓冲区大小已发生变化，以便视图可以相应地调整其 I/O 逻辑。
    void notifyNewBufferSize(int newBufferSize);

    /**
     * In tiered storage shuffle mode, only required segments will be sent to prevent the redundant
     * buffer usage. Downstream will notify the upstream by this method to send required segments.
     *
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    // 通知所需分段 ID。默认方法。
    // 在分层存储（Tiered Storage）模式下，下游可以通过此方法通知上游发送特定的分段（Segment）数据，实现按需读取，防止冗余缓冲区使用。
    default void notifyRequiredSegmentId(int subpartitionId, int segmentId) {}

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    // 预读下一个 Buffer 的子分区 ID。
    // 返回下一个将要被读取的 Buffer 所属的子分区索引，如果当前没有可用 Buffer，则返回 -1
    int peekNextBufferSubpartitionId() throws IOException;

    /**
     * Availability of the {@link ResultSubpartitionView} and the backlog in the corresponding
     * {@link ResultSubpartition}.
     */
    // 该嵌套类用于将当前视图的流控状态和数据积压状态组合起来返回给消费者。
    class AvailabilityWithBacklog {

        private final boolean isAvailable;

        private final int backlog;

        public AvailabilityWithBacklog(boolean isAvailable, int backlog) {
            checkArgument(backlog >= 0, "Backlog must be non-negative.");

            this.isAvailable = isAvailable;
            this.backlog = backlog;
        }

        public boolean isAvailable() {
            return isAvailable;
        }

        public int getBacklog() {
            return backlog;
        }
    }
}
