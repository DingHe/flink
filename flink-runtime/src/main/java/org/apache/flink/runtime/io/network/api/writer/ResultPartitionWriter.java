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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A record-oriented runtime result writer API for producing results.
 * <p>If {@link ResultPartitionWriter#close()} is called before {@link
 * ResultPartitionWriter#fail(Throwable)} or {@link ResultPartitionWriter#finish()}, it abruptly
 * triggers failure and cancellation of production. In this case {@link
 * ResultPartitionWriter#fail(Throwable)} still needs to be called afterwards to fully release all
 * resources associated the partition and propagate failure cause to the consumer if possible.
 */
// ResultPartitionWriter 接口是 Flink **任务结果输出端（生产任务/Producer Task）**的核心 API。
// 数据写入： 提供面向记录（record-oriented）的 API，负责将任务处理完的序列化数据记录和系统事件写入其管理的结果分区（ResultPartition）中。
// 数据分发： 管理如何将记录发送到多个下游子分区（Subpartitions），包括定向发送和广播。
// 下游连接： 允许创建子分区视图 (ResultSubpartitionView)，以便下游任务（消费者）可以连接并读取数据。
// 代表了 Flink 任务中数据产生和发送的逻辑终点
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {

    /** Setup partition, potentially heavy-weight, blocking operation comparing to just creation. */
    // 初始化分区。
    // 在分区写入操作开始之前，初始化必要的资源（例如缓冲区分配、网络连接设置等）。这是一个可能比较耗时或阻塞的操作，必须在开始写入数据前调用
    void setup() throws IOException;
   // 获取当前分区的唯一标识符（ResultPartitionID）
    ResultPartitionID getPartitionId();
    // 获取子分区数量。返回当前结果分区包含的子分区总数。
    // 通常对应于下游消费任务实例的数量。
    int getNumberOfSubpartitions();
    // 获取目标 Key Groups 数量。
    // 用于分区键的分配，帮助确定数据应该被分配到下游的哪个逻辑键组中，
    // 这与 Flink 的 Keyed State 路由相关。
    int getNumTargetKeyGroups();

    /** Sets the max overdraft buffer size of per gate. */
    // 设置最大透支缓冲区大小。
    // 用于流控，管理每个任务网关（Gate）可以“透支”使用的缓冲区资源上限，确保缓冲区资源不被过度消耗。
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /** Writes the given serialized record to the target subpartition.将处理过的记录写入特定的子分区进行后续处理 */
    // 写入记录。
    // 将给定的序列化记录 (ByteBuffer) 写入指定的目标子分区 (targetSubpartition)。这是任务写入用户数据的主要方法。
    void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException;

    /**
     * Writes the given serialized record to all subpartitions. One can also achieve the same effect
     * by emitting the same record to all subpartitions one by one, however, this method can have
     * better performance for the underlying implementation can do some optimizations, for example
     * coping the given serialized record only once to a shared channel which can be consumed by all
     * subpartitions.
     */
    // 广播记录。将相同的序列化记录同时发送到所有子分区。底层实现通常会优化，只复制一次数据到共享通道，以提高广播性能。
    void broadcastRecord(ByteBuffer record) throws IOException;

    /** Writes the given {@link AbstractEvent} to all subpartitions. */
    // 广播事件。
    // 将给定的系统事件 (AbstractEvent) 广播到所有子分区。isPriorityEvent 参数指示该事件是否应作为优先级事件发送，以绕过数据队列。
    void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

    /** Timeout the aligned barrier to unaligned barrier. */
    // 对齐屏障超时。
    // 在处理 Checkpoint 时，如果对齐屏障在指定时间后仍未完成对齐，此方法允许将其转换为非对齐屏障，以防止阻塞时间过长。
    void alignedBarrierTimeout(long checkpointId) throws IOException;

    /** Abort the checkpoint. */
    // 中止 Checkpoint。
    // 通知结果分区中止指定的 Checkpoint 过程，通常是由于上游或协调器失败所致。同时会传播失败原因
    void abortCheckpoint(long checkpointId, CheckpointException cause);

    /**
     * Notifies the downstream tasks that this {@code ResultPartitionWriter} have emitted all the
     * user records.
     * @param mode tells if we should flush all records or not (it is false in case of
     *     stop-with-savepoint (--no-drain))
     */
    // 通知数据流结束。通知下游任务当前分区已发射完所有用户记录
    void notifyEndOfData(StopMode mode) throws IOException;

    /**
     * Gets the future indicating whether all the records has been processed by the downstream
     * tasks. 获取一个表示所有数据已被下游任务处理完毕的 CompletableFuture
     */
    CompletableFuture<Void> getAllDataProcessedFuture();

    /** Sets the metric group for the {@link ResultPartitionWriter}. */
    void setMetricGroup(TaskIOMetricGroup metrics);

    /** Returns a reader for the subpartition with the given index range. 创建一个子分区视图，允许下游任务读取指定索引范围的子分区数据*/
    ResultSubpartitionView createSubpartitionView(
            ResultSubpartitionIndexSet indexSet, BufferAvailabilityListener availabilityListener)
            throws IOException;

    /** Manually trigger the consumption of data from all subpartitions. */
    // 强制刷新所有子分区。手动触发所有子分区中缓冲数据的发送，确保数据被及时推送到下游消费者。
    void flushAll();

    /** Manually trigger the consumption of data from the given subpartitions. */
    // 强制刷新特定子分区。手动触发指定子分区 (subpartitionIndex) 中缓冲数据的发送。
    void flush(int subpartitionIndex);

    /**
     * Fail the production of the partition.
     *
     * <p>This method propagates non-{@code null} failure causes to consumers on a best-effort
     * basis. This call also leads to the release of all resources associated with the partition.
     * Closing of the partition is still needed afterwards if it has not been done before.
     *
     * @param throwable failure cause
     */
    void fail(@Nullable Throwable throwable);

    /**
     * Successfully finish the production of the partition.
     *
     * <p>Closing of partition is still needed afterwards.
     */
    // 成功完成生产。
    // 标记分区数据生产成功结束。资源释放（通过 close() 或 release()）仍需在之后调用。
    void finish() throws IOException;

    boolean isFinished();

    /**
     * Releases the partition writer which releases the produced data and no reader can consume the
     * partition any more.
     */
    void release(Throwable cause);

    boolean isReleased();

    /**
     * Closes the partition writer which releases the allocated resource, for example the buffer
     * pool.
     */
    void close() throws Exception;
}
