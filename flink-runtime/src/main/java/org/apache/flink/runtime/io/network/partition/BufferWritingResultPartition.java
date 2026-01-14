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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
// Flink 网络栈中负责中间结果分发的核心类。
// 它是一个抽象类，位于生产者端，负责将算子生成的记录（Record）或事件（Event）写入具体的子分区（ResultSubpartition）。
// 主要任务：将上层算子发送的字节数据填充到网络缓冲区（Buffer）中，并分配给下游消费者的子分区。
// 数据填充与切分：它管理 BufferBuilder，将记录写入缓冲区。如果一条记录大于单个缓冲区，它负责将记录切分并跨缓冲区存储。
// 分发模式支持：支持单播（Unicast）和广播（Broadcast）。单播将数据发往特定子分区，广播则发往所有子分区。
// 背压监控：当 BufferPool（缓冲区池）用尽时，它会记录“硬背压”时间，并阻塞等待可用缓冲区。
// 对接存储层：与 ResultSubpartition 交互，将填满的缓冲区提交到队列中供下游读取。

public abstract class BufferWritingResultPartition extends ResultPartition {
    // 存储该分区下所有的子分区对象。
    // 数组长度等于下游并行的消费者数量。
    /** The subpartitions of this partition. At least one. */
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    // 单播模式下的缓冲区构建器数组。每个子分区维护一个，确保发往不同下游的数据互不干扰。
    private final BufferBuilder[] unicastBufferBuilders;
    // 广播模式下的共享构建器。
    // 当需要把同一条数据发给所有下游时，为了节省内存，只使用一个 Buffer。
    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    private BufferBuilder broadcastBufferBuilder;
    // 监控指标。记录由于没有空闲 Buffer 可用导致线程被阻塞（硬背压）的时间。
    private TimerGauge hardBackPressuredTimeMsPerSecond = new TimerGauge();
    // 计数器，记录该分区自启动以来写入的总字节数。
    private long totalWrittenBytes;

    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.subpartitions = checkNotNull(subpartitions);
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    protected void setupInternal() throws IOException {
        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    @Override //返回当前分区中已排队的缓冲区数量以及已排队缓冲区的字节数
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        return totalWrittenBytes - totalNumberOfBytes;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }
   //用于刷新指定子分区或所有子分区的缓冲区，将数据提交给下游操作
    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }
    // 将记录写入指定的子分区
    // 首先尝试追加到现有的 BufferBuilder，如果缓冲区满了，则调用 finish 提交该 Buffer，并申请新 Buffer 存放剩余数据（Record Continuation）
    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        totalWrittenBytes += record.remaining();

        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishUnicastBufferBuilder(targetSubpartition);
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder(targetSubpartition);
        }

        // partial buffer, full record
    }
    // 将记录发送给所有子分区。
    // 类似于单播，但使用的是 broadcastBufferBuilder，数据填满后，其生成的 BufferConsumer 会分发拷贝给所有子分区。
    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        totalWrittenBytes += ((long) record.remaining() * numSubpartitions);

        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }
    // 分发特殊事件（如 Checkpoint Barrier）
    // 先清空（Finish）当前的各种 Builder，确保事件在数据流中的顺序。将事件序列化后，立即发往所有子分区。
    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            totalWrittenBytes += ((long) eventBufferConsumer.getWrittenBytes() * numSubpartitions);
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each subpartition of
                // targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        hardBackPressuredTimeMsPerSecond = metrics.getHardBackPressuredTimePerSecond();
    }

    @Override
    protected ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");

        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            totalWrittenBytes += subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }
    // 单播模式下处理新记录写入的入口。它的核心职责是：为特定的子分区找到（或申请）一个可用的缓冲区，并开启数据的追加过程。
    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == null) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0, record.remaining());
        }

        append(record, buffer);

        return buffer;
    }
    // 负责将序列化后的记录（record）写入到 BufferBuilder 中
    // 将 record 中的字节数据追加到指定的 buffer 中，并返回实际写入的字节数。
    private int append(ByteBuffer record, BufferBuilder buffer) {
        // Try to avoid hard back-pressure in the subsequent calls to request buffers
        // by ignoring Buffer Debloater hints and extending the buffer if possible (trim).
        // This decreases the probability of hard back-pressure in cases when
        // the output size varies significantly and BD suggests too small values.
        // The hint will be re-applied on the next iteration.
        // 检查当前 Buffer 的剩余空间是否不足以容纳这条记录
        if (record.remaining() >= buffer.getWritableBytes()) {
            // This 2nd check is expensive, so it shouldn't be re-ordered.
            // However, it has the same cost as the subsequent call to request buffer, so it doesn't
            // affect the performance much.
            // 检查内存池（BufferPool）是否已经没有空闲 Buffer 了
            if (!bufferPool.isAvailable()) {
                // add 1 byte to prevent immediately flushing the buffer and potentially fit the
                // next record
                // 计算一个能强行容纳下当前记录的新容量。
                int newSize =
                        buffer.getMaxCapacity()
                                + (record.remaining() - buffer.getWritableBytes())
                                + 1;
                // 强行“扩容”或恢复 Buffer 的逻辑容量。
                buffer.trim(Math.max(buffer.getMaxCapacity(), newSize));
            }
        }
        // 执行真实的写入操作。
        return buffer.appendAndCommit(record);
    }
    // 将内存缓冲区（Buffer）与逻辑通道（Subpartition）正式关联的关键步骤。
    // 它不仅负责数据的传递，还深度参与了 Flink 的动态缓冲区调优（Buffer Debloating）。
    // partialRecordLength: 如果当前 Buffer 的开头是一条记录的剩余部分（跨 Buffer 存储），该参数表示这部分数据的长度；如果是新记录开头，则通常为 0。
    private void addToSubpartition(
            BufferBuilder buffer,
            int targetSubpartition,
            int partialRecordLength,
            int minDesirableBufferSize)
            throws IOException {
        // buffer.createBufferConsumerFromBeginning(): 为当前的 BufferBuilder 创建一个“消费者视图”（BufferConsumer）。
        // 这个消费者会从 Buffer 的起点开始读取数据。
        int desirableBufferSize =
                subpartitions[targetSubpartition].add(
                        buffer.createBufferConsumerFromBeginning(), partialRecordLength);

        resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
    }

    protected int addToSubpartition(
            int targetSubpartition, BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException {
        totalWrittenBytes += bufferConsumer.getWrittenBytes();
        return subpartitions[targetSubpartition].add(bufferConsumer, partialRecordLength);
    }
    // 核心作用是：根据当前网络状况，动态调整缓冲区的大小，以减少反压时的排队延迟。
    // buffer：当前正在操作的 BufferBuilder 对象，代表一块内存。
    // desirableBufferSize：期望的缓冲区大小。这是由 Flink 的反压控制器（或 Buffer Debloater）计算出来的“理想大小”，旨在保证数据在网络中排队的时间不超过设定的阈值（默认 1 秒）。
    // minDesirableBufferSize：最小要求的缓冲区大小。通常是当前已经写入该 Buffer 的数据长度。
    private void resizeBuffer(
            BufferBuilder buffer, int desirableBufferSize, int minDesirableBufferSize) {
        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(Math.max(minDesirableBufferSize, desirableBufferSize));
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0, record.remaining());
        }

        append(record, buffer);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        createBroadcastBufferConsumers(buffer, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(
            BufferBuilder buffer, int partialRecordBytes, int minDesirableBufferSize)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            int desirableBufferSize = Integer.MAX_VALUE;
            for (ResultSubpartition subpartition : subpartitions) {
                int subPartitionBufferSize = subpartition.add(consumer.copy(), partialRecordBytes);
                if (subPartitionBufferSize != ResultSubpartition.ADD_BUFFER_ERROR_CODE) {
                    desirableBufferSize = Math.min(desirableBufferSize, subPartitionBufferSize);
                }
            }
            resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
        }
    }
    // 单播（Unicast）模式下申请新缓冲区的入口方法。
    // 当现有的缓冲区已满，或者第一次向某个特定的子分区发送数据时，会调用此方法。
    // 用于为编号为 targetSubpartition 的子分区请求一个新的 BufferBuilder。
    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        checkInProduceState();
        // 会强制结束（Finish）当前可能存在的 broadcastBufferBuilder（广播构建器）
        ensureUnicastMode();
        // 先尝试非阻塞获取，如果池空了则进入阻塞等待，并记录“硬背压”时间。
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        // 将申请到的 BufferBuilder 存入 unicastBufferBuilders 数组中对应的下标位置
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }
    // 主要任务是从 BufferPool（缓冲区池）中获取一个新的 BufferBuilder。
    // Flink 如何处理背压（Backpressure）：它采用了“先礼后兵”的策略，即先尝试非阻塞获取，失败后再进入阻塞等待并记录指标。
    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {
        // 如果当前内存池中还有空闲的内存段（Memory Segment），
        // 此方法会立即返回。这是最理想的情况，意味着系统运行顺畅，没有背压。
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        if (bufferBuilder != null) {
            return bufferBuilder;
        }
        // 如果非阻塞获取失败（返回 null），说明缓冲区已用尽。
        // 在进入阻塞等待之前，调用 markStart() 开始记录**“硬背压”**时间。
        hardBackPressuredTimeMsPerSecond.markStart();
        try {
            // 调用阻塞方法。当前线程会停在这里，直到有其他 Task 释放了缓冲区并将其归还给池。
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            hardBackPressuredTimeMsPerSecond.markEnd();
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }
    // 负责结束并关闭某个特定子分区的单播缓冲区。
    // 当一个缓冲区被填满，或者任务需要强制下刷（Flush）数据时，该方法确保缓冲区的数据被标记为“可读”并更新相关的统计指标。
    // 接收一个参数 targetSubpartition，表示要关闭哪一个子分区的缓冲区构建器。
    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            // 调用 finish() 会在内存层面标记该缓冲区已写完。这会通知下游的 BufferConsumer，数据已经完整，可以被网络层（Netty）拉取发送了
            int bytes = bufferBuilder.finish();
            resultPartitionBytes.inc(targetSubpartition, bytes);
            numBytesOut.inc(bytes);
            numBuffersOut.inc();
            unicastBufferBuilders[targetSubpartition] = null;
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int subpartition = 0; subpartition < numSubpartitions; subpartition++) {
            finishUnicastBufferBuilder(subpartition);
        }
    }
    // 非常关键的“收尾”方法。当广播模式下的缓冲区（Buffer）填满，或者需要强制发送当前数据时，该方法负责将内存中的数据状态锁定并转化为可消费状态。
    private void finishBroadcastBufferBuilder() {
        // 检查是否存在正在使用的广播缓冲区构建器
        if (broadcastBufferBuilder != null) {
            // 告诉底层的内存段：数据已经写完了。
            // 此时会更新 Buffer 的元数据（如写索引），并触发所有关联的 BufferConsumer（下游消费者），使它们感知到数据已就绪。
            int bytes = broadcastBufferBuilder.finish();
            resultPartitionBytes.incAll(bytes);
            numBytesOut.inc(bytes * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            // 关闭 BufferBuilder 本身
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }
    // 意为“确保进入单播模式”。
    // Flink 的 ResultPartition 支持两种写入模式：
    // 单播（Unicast）：数据发送到某一个特定的下游 Subtask（例如 KeyBy 或 Rebalance）。
    // 广播（Broadcast）：数据发送到所有下游 Subtask（例如广播状态）。
    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }
    // 确保进入广播模式
    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getHardBackPressuredTimeMsPerSecond() {
        return hardBackPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}
