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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder.PositionMarker;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for producing {@link Buffer}.
 *
 * <p>It reads data written by {@link BufferBuilder}. Although it is not thread safe and can be used
 * only by one single thread, this thread can be different than the thread using/writing to {@link
 * BufferBuilder}. Pattern here is simple: one thread writes data to {@link BufferBuilder} and there
 * can be a different thread reading from it using {@link BufferConsumer}.
 */
// BufferConsumer 是一个非常核心的组件。如果说 BufferBuilder 是负责“写”的生产者，那么 BufferConsumer 就是负责“读”的视图。
// BufferConsumer 的主要作用是从 BufferBuilder 写入的内存段中读取数据。它的设计精髓在于：
// 生产者-消费者解耦：允许一个线程（Task 线程）向 BufferBuilder 写入数据，而另一个线程（Netty 网络线程）通过 BufferConsumer 读取数据。
// 多视图共享：一个 BufferBuilder 可以对应多个 BufferConsumer（例如在广播模式下）。它们共享同一块物理内存（MemorySegment），但各自维护独立的读取进度（currentReaderPosition）
//


@NotThreadSafe
public class BufferConsumer implements Closeable {
    // 指向底层的物理缓冲区。它是对 MemorySegment 的包装，负责实际的引用计数管理。
    private final Buffer buffer;
    // 用来同步生产者（BufferBuilder）的写入进度。
    // 它会缓存写入位置，减少对 volatile 变量的频繁访问。
    private final CachedPositionMarker writerPosition;
    // 记录当前 BufferConsumer 已经读到了哪个位置。
    private int currentReaderPosition;

    /** Constructs {@link BufferConsumer} instance with static content of a certain size. */
    // 用于创建一个包含静态数据的消费者。它假定数据已经写死，构造后立即处于 Finished 状态。
    public BufferConsumer(Buffer buffer, int size) {
        this(buffer, () -> -size, 0);
        checkState(
                isFinished(),
                "BufferConsumer with static size must be finished after construction!");
    }

    public BufferConsumer(
            Buffer buffer,
            BufferBuilder.PositionMarker currentWriterPosition,
            int currentReaderPosition) {
        this.buffer = checkNotNull(buffer);
        this.writerPosition = new CachedPositionMarker(checkNotNull(currentWriterPosition));
        checkArgument(
                currentReaderPosition <= writerPosition.getCached(),
                "Reader position larger than writer position");
        this.currentReaderPosition = currentReaderPosition;
    }

    /**
     * Checks whether the {@link BufferBuilder} has already been finished.
     *
     * <p>BEWARE: this method accesses the cached value of the position marker which is only updated
     * after calls to {@link #build()} and {@link #skip(int)}!
     *
     * @return <tt>true</tt> if the buffer was finished, <tt>false</tt> otherwise
     */
    // 检查对应的 BufferBuilder 是否已经调用了 finish()。
    // 如果是，说明这块 Buffer 不会再有新数据进入。
    public boolean isFinished() {
        return writerPosition.isFinished();
    }

    /**
     * @return sliced {@link Buffer} containing the not yet consumed data. Returned {@link Buffer}
     *     shares the reference counter with the parent {@link BufferConsumer} - in order to recycle
     *     memory both of them must be recycled/closed.
     */
    // 更新并获取最新的写入位置，创建一个从 currentReaderPosition 到 writerPosition 的只读切片（Slice）
    public Buffer build() {
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        Buffer slice =
                buffer.readOnlySlice(
                        currentReaderPosition, cachedWriterPosition - currentReaderPosition);
        currentReaderPosition = cachedWriterPosition;
        return slice.retainBuffer();
    }

    /** @param bytesToSkip number of bytes to skip from currentReaderPosition */
    // 跳过指定字节。
    // 仅移动读取指针，不产生数据对象

    void skip(int bytesToSkip) {
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        int bytesReadable = cachedWriterPosition - currentReaderPosition;
        checkState(bytesToSkip <= bytesReadable, "bytes to skip beyond readable range");
        currentReaderPosition += bytesToSkip;
    }

    /**
     * Returns a retained copy with separate indexes. This allows to read from the same {@link
     * MemorySegment} twice.
     *
     * <p>WARNING: the newly returned {@link BufferConsumer} will have its reader index copied from
     * the original buffer. In other words, data already consumed before copying will not be visible
     * to the returned copies.
     *
     * @return a retained copy of self with separate indexes
     */
    // 创建一个当前消费者的副本。
    // 副本共享同一个物理 Buffer，但拥有独立的 currentReaderPosition
    public BufferConsumer copy() {
        return new BufferConsumer(
                buffer.retainBuffer(), writerPosition.positionMarker, currentReaderPosition);
    }

    /**
     * Returns a retained copy with separate indexes and sets the reader position to the given
     * value. This allows to read from the same {@link MemorySegment} twice starting from the
     * supplied position.
     *
     * @param readerPosition the new reader position. Can be less than the {@link
     *     #currentReaderPosition}, but may not exceed the current writer's position.
     * @return a retained copy of self with separate indexes
     */
    // 创建副本并手动指定读取起始点。
    // 常用于从头开始重读数据（如重传场景）。
    public BufferConsumer copyWithReaderPosition(int readerPosition) {
        return new BufferConsumer(
                buffer.retainBuffer(), writerPosition.positionMarker, readerPosition);
    }

    public boolean isBuffer() {
        return buffer.isBuffer();
    }

    // 获取 Buffer 存储的数据类型（是普通数据还是 Event 事件）。
    public Buffer.DataType getDataType() {
        return buffer.getDataType();
    }
    // 释放对物理 Buffer 的引用。
    // 如果引用计数归零，底层内存会被回收。
    @Override
    public void close() {
        if (!buffer.isRecycled()) {
            buffer.recycleBuffer();
        }
    }
    // 检查底层的内存块是否已被回收
    public boolean isRecycled() {
        return buffer.isRecycled();
    }
    // 获取当前写入者一共写了多少字节
    public int getWrittenBytes() {
        return writerPosition.getCached();
    }

    int getCurrentReaderPosition() {
        return currentReaderPosition;
    }

    boolean isStartOfDataBuffer() {
        return buffer.getDataType() == DATA_BUFFER && currentReaderPosition == 0;
    }

    int getBufferSize() {
        return buffer.getMaxCapacity();
    }

    /** Returns true if there is new data available for reading. */
    // 检查当前读位置是否小于最新的写位置。如果是，说明有新数据可读。
    public boolean isDataAvailable() {
        return currentReaderPosition < writerPosition.getLatest();
    }
    // 生成调试信息，方便排查内存数据问题。
    public String toDebugString(boolean includeHash) {
        Buffer buffer = null;
        try (BufferConsumer copiedBufferConsumer = copy()) {
            buffer = copiedBufferConsumer.build();
            checkState(copiedBufferConsumer.isFinished());
            return buffer.toDebugString(includeHash);
        } finally {
            if (buffer != null) {
                buffer.recycleBuffer();
            }
        }
    }

    /**
     * Cached reading wrapper around {@link PositionMarker}.
     *
     * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be
     * implemented independently of one another - so that the cached values can not accidentally
     * leak from one to another.
     */
    private static class CachedPositionMarker {
        private final PositionMarker positionMarker;

        /**
         * Locally cached value of {@link PositionMarker} to avoid unnecessary volatile accesses.
         */
        private int cachedPosition;

        CachedPositionMarker(PositionMarker positionMarker) {
            this.positionMarker = checkNotNull(positionMarker);
            update();
        }

        public boolean isFinished() {
            return PositionMarker.isFinished(cachedPosition);
        }

        public int getCached() {
            return PositionMarker.getAbsolute(cachedPosition);
        }

        private int getLatest() {
            return PositionMarker.getAbsolute(positionMarker.get());
        }

        private void update() {
            this.cachedPosition = positionMarker.get();
        }
    }
}
