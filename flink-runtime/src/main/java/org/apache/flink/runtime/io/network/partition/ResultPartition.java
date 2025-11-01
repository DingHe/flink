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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.metrics.ResultPartitionBytesCounter;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for data produced by a single task.
 * 一个task产生的数据结果
 * <p>This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * a result partition is a collection of {@link Buffer} instances. The buffers are organized in one
 * or more {@link ResultSubpartition} instances or in a joint structure which further partition the
 * data depending on the number of consuming tasks and the data {@link DistributionPattern}.
 *
 * <p>Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link
 * LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 *
 * <p>The life-cycle of each result partition has three (possibly overlapping) phases:
 *
 * <ol>
 *   <li><strong>Produce</strong>:
 *   <li><strong>Consume</strong>:
 *   <li><strong>Release</strong>:
 * </ol>
 *
 * <h2>Buffer management</h2>
 * <h2>State management</h2>
 */
// ResultPartition 类是 Flink 网络栈中数据生产的核心抽象。
// 它是一个任务（Task）产生的所有数据的运行时容器和管理器，是逻辑上的 IntermediateResultPartition 在运行时的具体实现
// 数据容器与分发中心： 它负责管理一个任务产生的所有数据。这些数据被进一步划分为多个子分区（ResultSubpartition），每个子分区通常对应一个下游消费者任务。
// 缓冲区资源管理： 它管理一个共享的 缓冲区池（BufferPool），为所有子分区提供内存缓冲区资源，这是 Flink 流控和高性能 I/O 的基础。
// 消费者连接点： 它允许下游任务通过 createSubpartitionView 方法连接并消费（读取）数据，无论是本地消费还是远程消费。
// ResultPartition 是连接 Flink 任务之间数据流的输出网关。
public abstract class ResultPartition implements ResultPartitionWriter {

    protected static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);
    // 产生该分区数据的任务名称。
    // 它帮助追踪数据流的来源任务
    private final String owningTaskName;
    // 分区索引。
    // 该分区在中间结果集（Intermediate Result）中的索引
    private final int partitionIndex;
    // 分区唯一标识符
    protected final ResultPartitionID partitionId;
    // 分区的类型，决定了如何创建和管理子分区，分配缓冲区，消费模式等
    /** Type of this partition. Defines the concrete subpartition implementation to use. */
    protected final ResultPartitionType partitionType;
    // ResultPartitionManager 管理当前 TaskManager 所有的 ResultPartition
    protected final ResultPartitionManager partitionManager;
    // 分区中的子分区数量，每个子分区可以被不同的消费者（下游任务）并行读取
    protected final int numSubpartitions;
    // 目标键组的数量。
    // 分区可能需要将数据按键分组，分配到不同的键组中
    private final int numTargetKeyGroups;

    // - Runtime state --------------------------------------------------------
    // 用于标记分区是否已被释放
    private final AtomicBoolean isReleased = new AtomicBoolean();
    // 管理缓冲区的池，多个子分区共享一个缓冲池
    protected BufferPool bufferPool;
    // 标记分区是否已完成生产数据
    private boolean isFinished;
    // 如果分区因故障而失败，保存失败的原因
    private volatile Throwable cause;
    // 用于创建缓冲池的工厂，负责在需要时提供 BufferPool
    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Used to compress buffer to reduce IO.
     * 用于压缩缓冲区数据，减少 I/O 操作时的资源消耗 */
    @Nullable protected final BufferCompressor bufferCompressor;
    // 记录写出到下游的字节数
    protected Counter numBytesOut = new SimpleCounter();
    // 记录写出的缓冲区数量
    protected Counter numBuffersOut = new SimpleCounter();
    // 用于统计分区的字节数
    protected ResultPartitionBytesCounter resultPartitionBytes;
   // 是否未定义消费分区的数量，通常用于分区的消费模式尚未明确时
    private boolean isNumberOfPartitionConsumerUndefined = false;

    public ResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= partitionIndex, "The partition index must be positive.");
        this.partitionIndex = partitionIndex;
        this.partitionId = checkNotNull(partitionId);
        this.partitionType = checkNotNull(partitionType);
        this.numSubpartitions = numSubpartitions;
        this.numTargetKeyGroups = numTargetKeyGroups;
        this.partitionManager = checkNotNull(partitionManager);
        this.bufferCompressor = bufferCompressor;
        this.bufferPoolFactory = bufferPoolFactory;
        this.resultPartitionBytes = new ResultPartitionBytesCounter(numSubpartitions);
    }

    /**
     * Registers a buffer pool with this result partition.
     *
     * <p>There is one pool for each result partition, which is shared by all its sub partitions.
     * 需要在开始数据写入之前调用
     * <p>The pool is registered with the partition *after* it as been constructed in order to
     * conform to the life-cycle of task registrations in the {@link TaskExecutor}.
     */
    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in result partition setup logic: Already registered buffer pool.");

        this.bufferPool = checkNotNull(bufferPoolFactory.get());
        setupInternal();
        partitionManager.registerResultPartition(this);
    }

    /** Do the subclass's own setup operation. */
    protected abstract void setupInternal() throws IOException;

    public String getOwningTaskName() {
        return owningTaskName;
    }

    @Override
    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int getNumberOfSubpartitions() {
        return numSubpartitions;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public void isNumberOfPartitionConsumerUndefined(boolean isNumberOfPartitionConsumerUndefined) {
        this.isNumberOfPartitionConsumerUndefined = isNumberOfPartitionConsumerUndefined;
    }

    public boolean isNumberOfPartitionConsumerUndefined() {
        return isNumberOfPartitionConsumerUndefined;
    }

    /** Returns the total number of queued buffers of all subpartitions. */
    public abstract int getNumberOfQueuedBuffers();

    /** Returns the total size in bytes of queued buffers of all subpartitions. */
    public abstract long getSizeOfQueuedBuffersUnsafe();

    /** Returns the number of queued buffers of the given target subpartition. */
    public abstract int getNumberOfQueuedBuffers(int targetSubpartition);
   //设置每个网关的最大超额缓冲区数量
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        this.bufferPool.setMaxOverdraftBuffersPerGate(maxOverdraftBuffersPerGate);
    }

    /**
     * Returns the type of this result partition.
     *
     * @return result partition type
     */
    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    public ResultPartitionBytesCounter getResultPartitionBytes() {
        return resultPartitionBytes;
    }

    // ------------------------------------------------------------------------

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        throw new UnsupportedOperationException();
    }

    /**
     * The subpartition notifies that the corresponding downstream task have processed all the user
     * records.
     *
     * @see EndOfData
     * @param subpartition The index of the subpartition sending the notification.
     */
    public void onSubpartitionAllDataProcessed(int subpartition) {}

    /**
     * Finishes the result partition.
     *
     * <p>After this operation, it is not possible to add further data to the result partition.
     *
     * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
     */
    @Override
    public void finish() throws IOException {
        checkInProduceState();

        isFinished = true;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    public void release() {
        release(null);
    }

    @Override
    public void release(Throwable cause) {
        if (isReleased.compareAndSet(false, true)) {
            LOG.debug("{}: Releasing {}.", owningTaskName, this);

            // Set the error cause
            if (cause != null) {
                this.cause = cause;
            }

            releaseInternal();
        }
    }

    /** Releases all produced data including both those stored in memory and persisted on disk. */
    protected abstract void releaseInternal();

    private void closeBufferPool() {
        if (bufferPool != null) {
            bufferPool.lazyDestroy();
        }
    }

    @Override
    public void close() {
        closeBufferPool();
    }

    @Override
    public void fail(@Nullable Throwable throwable) {
        // the task canceler thread will call this method to early release the output buffer pool
        closeBufferPool();
        partitionManager.releasePartition(partitionId, throwable);
    }

    public Throwable getFailureCause() {
        return cause;
    }

    @Override
    public int getNumTargetKeyGroups() {
        return numTargetKeyGroups;
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        numBytesOut = metrics.getNumBytesOutCounter();
        numBuffersOut = metrics.getNumBuffersOutCounter();
        metrics.registerResultPartitionBytesCounter(
                partitionId.getPartitionId(), resultPartitionBytes);
    }

    @Override //为给定的子分区索引集合创建一个合并的子分区视图
    public ResultSubpartitionView createSubpartitionView(
            ResultSubpartitionIndexSet indexSet, BufferAvailabilityListener availabilityListener)
            throws IOException {
        if (indexSet.size() == 1) {
            return createSubpartitionView(
                    indexSet.values().iterator().next(), availabilityListener);
        } else {
            UnionResultSubpartitionView unionView =
                    new UnionResultSubpartitionView(availabilityListener, indexSet.size());
            try {
                for (int i : indexSet.values()) {
                    ResultSubpartitionView view = createSubpartitionView(i, unionView);
                    unionView.notifyViewCreated(i, view);
                }
                return unionView;
            } catch (Exception e) {
                unionView.releaseAllResources();
                throw e;
            }
        }
    }

    /**
     * Returns a reader for the subpartition with the given index.
     *
     * <p>Given that the function to merge outputs from multiple subpartition views is supported
     * uniformly in {@link UnionResultSubpartitionView}, subclasses of {@link ResultPartition} only
     * needs to take care of creating subpartition view for a single subpartition.
     */
    protected abstract ResultSubpartitionView createSubpartitionView(
            int index, BufferAvailabilityListener availabilityListener) throws IOException;

    /**
     * Whether this partition is released.
     *
     * <p>A partition is released when each subpartition is either consumed and communication is
     * closed by consumer or failed. A partition is also released if task is cancelled.
     */
    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return bufferPool.getAvailableFuture();
    }

    @Override
    public String toString() {
        return "ResultPartition "
                + partitionId.toString()
                + " ["
                + partitionType
                + ", "
                + numSubpartitions
                + " subpartitions]";
    }

    // ------------------------------------------------------------------------

    /** Notification when a subpartition is released. */
    void onConsumedSubpartition(int subpartitionIndex) {

        if (isReleased.get()) {
            return;
        }

        LOG.debug(
                "{}: Received release notification for subpartition {}.", this, subpartitionIndex);
    }

    // ------------------------------------------------------------------------

    protected void checkInProduceState() throws IllegalStateException {
        checkState(!isFinished, "Partition already finished.");
    }

    @VisibleForTesting
    public ResultPartitionManager getPartitionManager() {
        return partitionManager;
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    protected boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }
}
