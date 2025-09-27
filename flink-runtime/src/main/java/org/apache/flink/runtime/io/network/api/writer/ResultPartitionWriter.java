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
 * 它负责将数据写入特定的结果分区，并提供了多种方法来处理数据的写入、事件广播、数据消费管理、检查点等操作。这个接口与 Flink 的数据流系统紧密集成，允许任务将输出数据写入到多个下游操作的输入中
 * <p>If {@link ResultPartitionWriter#close()} is called before {@link
 * ResultPartitionWriter#fail(Throwable)} or {@link ResultPartitionWriter#finish()}, it abruptly
 * triggers failure and cancellation of production. In this case {@link
 * ResultPartitionWriter#fail(Throwable)} still needs to be called afterwards to fully release all
 * resources associated the partition and propagate failure cause to the consumer if possible.
 */
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {
    //在分区写入操作开始之前，初始化必要的资源。这个方法需要在分区写入之前调用
    /** Setup partition, potentially heavy-weight, blocking operation comparing to just creation. */
    void setup() throws IOException;
   //获取当前分区的唯一标识符（ResultPartitionID）
    ResultPartitionID getPartitionId();
    //确定当前分区中包含多少个子分区，通常每个子分区会处理流中的一部分数据
    int getNumberOfSubpartitions();
    //用于分区键的分配，帮助分配数据到不同的目标键组中
    int getNumTargetKeyGroups();

    /** Sets the max overdraft buffer size of per gate. 管理每个任务网关的缓冲区资源，确保资源不被过度消耗*/
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /** Writes the given serialized record to the target subpartition.将处理过的记录写入特定的子分区进行后续处理 */
    void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException;

    /**
     * Writes the given serialized record to all subpartitions. One can also achieve the same effect
     * by emitting the same record to all subpartitions one by one, however, this method can have
     * better performance for the underlying implementation can do some optimizations, for example
     * coping the given serialized record only once to a shared channel which can be consumed by all
     * subpartitions.将相同的记录同时发送到所有子分区，通常用于广播事件或公共数据
     */
    void broadcastRecord(ByteBuffer record) throws IOException;

    /** Writes the given {@link AbstractEvent} to all subpartitions. 用于广播特殊事件（如水印、停止事件等），并可以设置事件的优先级*/
    void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

    /** Timeout the aligned barrier to unaligned barrier.处理检查点时，若发生超时，允许转换为非对齐屏障 */
    void alignedBarrierTimeout(long checkpointId) throws IOException;

    /** Abort the checkpoint. */
    void abortCheckpoint(long checkpointId, CheckpointException cause);

    /**
     * Notifies the downstream tasks that this {@code ResultPartitionWriter} have emitted all the
     * user records.
     * 标记数据流已经结束，通知下游任务开始处理
     * @param mode tells if we should flush all records or not (it is false in case of
     *     stop-with-savepoint (--no-drain))
     */
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

    /** Manually trigger the consumption of data from all subpartitions.强制刷新所有子分区的数据，确保数据被及时消费 */
    void flushAll();

    /** Manually trigger the consumption of data from the given subpartitions.强制刷新某个子分区的数据，通常在特定子分区的缓冲区已满时使用 */
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
