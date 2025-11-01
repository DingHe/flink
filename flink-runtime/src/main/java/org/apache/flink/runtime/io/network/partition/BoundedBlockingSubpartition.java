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
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the ResultSubpartition for a bounded result transferred in a blocking
 * manner: The result is first produced, then consumed. The result can be consumed possibly multiple
 * times.
 *
 * <p>Depending on the supplied implementation of {@link BoundedData}, the actual data is stored for
 * example in a file, or in a temporary memory mapped file.
 *
 * <h2>Important Notes on Thread Safety</h2>
 *
 * <p>This class does not synchronize every buffer access. It assumes the threading model of the
 * Flink network stack and is not thread-safe beyond that.
 *
 * <p>This class assumes a single writer thread that adds buffers, flushes, and finishes the write
 * phase. That same thread is also assumed to perform the partition release, if the release happens
 * during the write phase.
 *
 * <p>The implementation supports multiple concurrent readers, but assumes a single thread per
 * reader. That same thread must also release the reader. In particular, after the reader was
 * released, no buffers obtained from this reader may be accessed any more, or segmentation faults
 * might occur in some implementations.
 *
 * <p>The method calls to create readers, dispose readers, and dispose the partition are thread-safe
 * vis-a-vis each other.
 */
// 阻塞式传输： 实现结果分区的阻塞式数据传输模型，即数据必须先完全生产并写入存储（如文件）后，才能开始消费。
// 这与流处理中的管道化（Pipelined）传输模型（边生产边消费）形成鲜明对比。
// 有界性 (Bounded)： 专用于处理有界数据集（即批处理）或流处理中需要阻塞 Shuffle 的阶段。
// 持久化存储： 利用 BoundedData 抽象，将数据持久化到外部存储（如磁盘文件或内存映射文件），使得数据可以被多次消费（例如，在下游任务失败重试时）。
// 支持多读者： 允许在数据写入完成后，多个下游任务并行创建 ResultSubpartitionView 来读取相同的数据。
// 总结来说，它是 Flink 批处理（或流批一体）模式下，用于将数据完整地写入文件存储，并允许多个下游任务读取该文件的核心组件。

final class BoundedBlockingSubpartition extends ResultSubpartition {

    /** This lock guards the creation of readers and disposal of the memory mapped file. */
    private final Object lock = new Object();

    /** The current buffer, may be filled further over time. */
    // 当前缓冲区。
    // 存储上游写入但尚未通过 flush() 或 finish() 写入到底层 BoundedData 的 BufferConsumer。
    @Nullable private BufferConsumer currentBuffer;

    /** The bounded data store that we store the data in. */
    // 有界数据存储。
    // 负责将数据实际持久化。实现可以是写入磁盘文件 (FileChannelBoundedData) 或内存映射文件 (MemoryMappedBoundedData)。
    private final BoundedData data;

    /** All created and not yet released readers. */
    // 读者集合。
    // 存储所有已创建但尚未释放的 ResultSubpartitionView 实例。
    // 用于追踪读者的生命周期，以便在最后一个读者释放时清理底层数据存储。
    @GuardedBy("lock")
    private final Set<ResultSubpartitionView> readers;

    /**
     * Flag to transfer file via FileRegion way in network stack if partition type is file without
     * SSL enabled.
     */
    // 直接文件传输标志。
    // 指示是否可以使用 Netty 的 FileRegion 机制进行数据传输（通常适用于数据存储在文件且未启用 SSL 的情况）。
    // 使用这种方式可以绕过 JVM 堆内存，提高传输效率。
    private final boolean useDirectFileTransfer;

    /** Counter for the number of data buffers (not events!) written. */
    // 数据 Buffer 计数器。
    // 记录已写入到底层存储的数据 Buffer 的数量（不包括 Event 事件）。
    // 用于统计和下游读取时的 Backlog 信息。
    private int numDataBuffersWritten;

    /** The counter for the number of data buffers and events. */
    // 总 Buffer 和 Event 计数器。
    // 记录已写入到底层存储的所有 Buffer 和 Event 的总数量。用于统计。
    private int numBuffersAndEventsWritten;

    /** Flag indicating whether the writing has finished and this is now available for read. */
    // 完成标志。
    // 标志数据写入阶段是否已完成。
    // 只有当此标志为 true 时，才允许创建新的读者。
    private boolean isFinished;

    /** Flag indicating whether the subpartition has been released. */
    // 释放标志。
    // 标志子分区是否已被释放（销毁）。一旦释放，写入和读取操作都将被禁止。
    private boolean isReleased;

    public BoundedBlockingSubpartition(
            int index, ResultPartition parent, BoundedData data, boolean useDirectFileTransfer) {

        super(index, parent);

        this.data = checkNotNull(data);
        this.useDirectFileTransfer = useDirectFileTransfer;
        this.readers = new HashSet<>();
    }

    // ------------------------------------------------------------------------

    /**
     * Checks if writing is finished. Readers cannot be created until writing is finished, and no
     * further writes can happen after that.
     */
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) throws IOException {
        if (isFinished()) {
            bufferConsumer.close();
            return ADD_BUFFER_ERROR_CODE;
        }

        flushCurrentBuffer();
        currentBuffer = bufferConsumer;
        return Integer.MAX_VALUE;
    }

    @Override
    public void flush() {
        // unfortunately, the signature of flush does not allow for any exceptions, so we
        // need to do this discouraged pattern of runtime exception wrapping
        try {
            flushCurrentBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    private void flushCurrentBuffer() throws IOException {
        if (currentBuffer != null) {
            writeAndCloseBufferConsumer(currentBuffer);
            currentBuffer = null;
        }
    }

    private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer) throws IOException {
        try {
            final Buffer buffer = bufferConsumer.build();
            try {
                if (parent.canBeCompressed(buffer)) {
                    final Buffer compressedBuffer =
                            parent.bufferCompressor.compressToIntermediateBuffer(buffer);
                    data.writeBuffer(compressedBuffer);
                    if (compressedBuffer != buffer) {
                        compressedBuffer.recycleBuffer();
                    }
                } else {
                    data.writeBuffer(buffer);
                }

                numBuffersAndEventsWritten++;
                if (buffer.isBuffer()) {
                    numDataBuffersWritten++;
                }
            } finally {
                buffer.recycleBuffer();
            }
        } finally {
            bufferConsumer.close();
        }
    }

    @Override
    public int finish() throws IOException {
        checkState(!isReleased, "data partition already released");
        checkState(!isFinished, "data partition already finished");

        isFinished = true;
        flushCurrentBuffer();
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        writeAndCloseBufferConsumer(eventBufferConsumer);
        data.finishWrite();
        return eventBufferConsumer.getWrittenBytes();
    }

    @Override
    public void release() throws IOException {
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            isFinished = true; // for fail fast writes

            if (currentBuffer != null) {
                currentBuffer.close();
                currentBuffer = null;
            }
            checkReaderReferencesAndDispose();
        }
    }

    @Override
    public ResultSubpartitionView createReadView(BufferAvailabilityListener availability)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "data partition already released");
            checkState(isFinished, "writing of blocking partition not yet finished");

            if (!Files.isReadable(data.getFilePath())) {
                throw new PartitionNotFoundException(parent.getPartitionId());
            }

            final ResultSubpartitionView reader;
            if (useDirectFileTransfer) {
                reader =
                        new BoundedBlockingSubpartitionDirectTransferReader(
                                this,
                                data.getFilePath(),
                                numDataBuffersWritten,
                                numBuffersAndEventsWritten);
            } else {
                reader =
                        new BoundedBlockingSubpartitionReader(
                                this, data, numDataBuffersWritten, availability);
            }
            readers.add(reader);
            return reader;
        }
    }

    void releaseReaderReference(ResultSubpartitionView reader) throws IOException {
        onConsumedSubpartition();

        synchronized (lock) {
            if (readers.remove(reader) && isReleased) {
                checkReaderReferencesAndDispose();
            }
        }
    }

    @GuardedBy("lock")
    private void checkReaderReferencesAndDispose() throws IOException {
        assert Thread.holdsLock(lock);

        // To avoid lingering memory mapped files (large resource footprint), we don't
        // wait for GC to unmap the files, but use a Netty utility to directly unmap the file.
        // To avoid segmentation faults, we need to wait until all readers have been released.

        if (readers.isEmpty()) {
            data.close();
        }
    }

    @VisibleForTesting
    public BufferConsumer getCurrentBuffer() {
        return currentBuffer;
    }

    // ---------------------------- statistics --------------------------------

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        // not supported.
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return numBuffersAndEventsWritten;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return data.getSize();
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    int getBuffersInBacklogUnsafe() {
        return numDataBuffersWritten;
    }

    // ---------------------------- factories --------------------------------

    /**
     * Creates a BoundedBlockingSubpartition that simply stores the partition data in a file. Data
     * is eagerly spilled (written to disk) and readers directly read from the file.
     */
    public static BoundedBlockingSubpartition createWithFileChannel(
            int index,
            ResultPartition parent,
            File tempFile,
            int readBufferSize,
            boolean sslEnabled)
            throws IOException {

        final FileChannelBoundedData bd =
                FileChannelBoundedData.create(tempFile.toPath(), readBufferSize);
        return new BoundedBlockingSubpartition(index, parent, bd, !sslEnabled);
    }

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in memory mapped file.
     * Data is written to and read from the mapped memory region. Disk spilling happens lazily, when
     * the OS swaps out the pages from the memory mapped file.
     */
    public static BoundedBlockingSubpartition createWithMemoryMappedFile(
            int index, ResultPartition parent, File tempFile) throws IOException {

        final MemoryMappedBoundedData bd = MemoryMappedBoundedData.create(tempFile.toPath());
        return new BoundedBlockingSubpartition(index, parent, bd, false);
    }

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in a file and memory
     * maps that file for reading. Data is eagerly spilled (written to disk) and then mapped into
     * memory. The main difference to the {@link #createWithMemoryMappedFile(int, ResultPartition,
     * File)} variant is that no I/O is necessary when pages from the memory mapped file are
     * evicted.
     */
    public static BoundedBlockingSubpartition createWithFileAndMemoryMappedReader(
            int index, ResultPartition parent, File tempFile) throws IOException {

        final FileChannelMemoryMappedBoundedData bd =
                FileChannelMemoryMappedBoundedData.create(tempFile.toPath());
        return new BoundedBlockingSubpartition(index, parent, bd, false);
    }
}
