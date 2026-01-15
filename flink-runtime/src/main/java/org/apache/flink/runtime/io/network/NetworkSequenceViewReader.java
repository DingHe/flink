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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Simple wrapper for the partition readerQueue iterator, which increments a sequence number for
 * each returned buffer and remembers the receiver ID.
 */
// NetworkSequenceViewReader 是一个至关重要的接口。
// 它定义了 Netty 服务器端如何读取上游 ResultPartition（结果分区）数据并将其发送给下游消费者的行为规范。
// NetworkSequenceViewReader 充当了 Netty 层与存储层（ResultSubpartition）之间的调度员。
// 桥接作用：它包装了 ResultSubpartitionView，将底层的 Buffer 读取逻辑转化为网络层可用的带有序列号（Sequence Number）的数据流。
// 流量控制：通过 Credit 机制（信贷/令牌）控制数据的读取速度，确保不会撑爆下游的接收缓冲区。
// 生命周期管理：处理分区请求、建立读取视图、处理超时以及资源的释放。
// 可用性追踪：记录哪些 Reader 当前有数据可读，并配合 Netty 的 PartitionRequestQueue 进行数据发送调度。

public interface NetworkSequenceViewReader {

    /**
     * When the netty server receives the downstream task's partition request and the upstream task
     * has registered its partition, it will process the partition request immediately, otherwise it
     * will create a {@link PartitionRequestListener} for given {@link ResultPartitionID} in {@link
     * ResultPartitionManager} and notify the listener when the upstream task registers its
     * partition.
     *
     * @param partitionProvider the result partition provider
     * @param resultPartitionId the result partition id
     * @param subpartitionIndexSet the sub partition indexes
     * @throws IOException the thrown exception
     */
    // 当下游 Task 向 Netty 发送“请求数据”的消息时调用
    // 它会尝试从 partitionProvider 获取读取视图。如果上游分区还没产生（例如上游还没启动），它会注册一个监听器，等分区出来后自动连接。
    void requestSubpartitionViewOrRegisterListener(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException;

    /**
     * When the {@link ResultPartitionManager} registers {@link ResultPartition}, it will get the
     * {@link PartitionRequestListener} via given {@link ResultPartitionID}, and create subpartition
     * view reader for downstream task.
     *
     * @param partition the result partition
     * @param subpartitionIndexSet the sub partition indexes
     * @throws IOException the thrown exception
     */
    // 当异步请求的分区终于创建好时，由 ResultPartitionManager 回调此方法，正式建立数据读取通道。
    void notifySubpartitionsCreated(
            ResultPartition partition, ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException;
    // 在不取出 Buffer 的情况下，预看下一个 Buffer 属于哪个子分区。这在多子分区读取（如合并读取）时非常有用。
    int peekNextBufferSubpartitionId() throws IOException;
    // 从底层的子分区中取出一个 Buffer，并附加上序列号和可用性信息（BufferAndAvailability）
    @Nullable
    BufferAndAvailability getNextBuffer() throws IOException;

    /** Returns true if the producer backlog need to be announced to the consumer. */
    // 判断是否需要向下游广播当前的积压情况，用于下游动态申请 Credit。
    boolean needAnnounceBacklog();

    /**
     * The credits from consumer are added in incremental way.
     *
     * @param creditDeltas The credit deltas
     */
    // 下游 Task 告诉 Reader：“我又腾出了 X 个空位，你可以多发 X 个 Buffer”。
    // 增加 Reader 的可用配额，只有 Credit > 0 时，getNextBuffer() 才会真正被调用发送数据。
    void addCredit(int creditDeltas);

    /**
     * Notify the id of required segment from consumer.
     *
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    // 在支持分段存储的场景下（如 Sort-Merge Shuffle），下游请求特定的数据段 ID。
    void notifyRequiredSegmentId(int subpartitionId, int segmentId);

    /** Resumes data consumption after an exactly once checkpoint. */
    // 在 Exactly-once 检查点对齐完成后，恢复被暂停的数据消费。
    void resumeConsumption();

    /** Acknowledges all the user records are processed. */
    // 当下游确认处理完所有数据记录时调用的确认信号。
    void acknowledgeAllRecordsProcessed();

    /**
     * Checks whether this reader is available or not and returns the backlog at the same time.
     *
     * @return A boolean flag indicating whether the reader is available together with the backlog.
     */
    // 获取当前是否有数据可读（Availability）以及还有多少积压（Backlog）。
    // Netty 调度器根据这个判断是否要调度该 Reader。
    ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog();
    // 标记该 Reader 是否已经进入了 Netty 的“待发送队列”中。防止重复入队。
    boolean isRegisteredAsAvailable();

    /**
     * Updates the value to indicate whether the reader is enqueued in the pipeline or not.
     *
     * @param isRegisteredAvailable True if this reader is already enqueued in the pipeline.
     */
    void setRegisteredAsAvailable(boolean isRegisteredAvailable);
    // 检查是否释放以及执行最后的资源清理工作（关闭视图、回收内存）
    boolean isReleased();

    void releaseAllResources() throws IOException;
    // 如果读取出错，通过此方法获取具体的异常原因。
    Throwable getFailureCause();
    // 获取下游接收者的 InputChannelID。
    InputChannelID getReceiverId();
    // 通知 Reader 下游期望的新缓冲区大小（用于 Buffer Debloating 动态调整）
    void notifyNewBufferSize(int newBufferSize);

    /**
     * When the partition request from the given downstream task is timeout, it should notify the
     * reader in netty server and send {@link PartitionNotFoundException} to the task.
     *
     * @param partitionRequestListener the timeout message of given {@link PartitionRequestListener}
     */
    // 如果分区请求长时间得不到响应（超时），通知该 Reader 报错并向下游发送 PartitionNotFoundException。
    void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener);
}
