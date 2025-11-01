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

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** View over a pipelined in-memory only subpartition. */
// PipelinedSubpartitionView 是 Flink 网络栈中一个关键的组件，
// 它充当了下游消费者（例如 Netty I/O 线程）与数据源（PipelinedSubpartition）之间的桥梁或句柄
// 数据读取接口： 它是下游任务拉取数据（Buffer 和 Event）的唯一接口。下游消费者通过调用其 getNextBuffer() 方法来获取数据。
// 封装和解耦： 它将 ResultSubpartition 的复杂内部实现（如同步、队列管理、Checkpoint 处理）对消费者隐藏起来。消费者只需要通过 View 接口进行操作。
public class PipelinedSubpartitionView implements ResultSubpartitionView {

    /** The subpartition this view belongs to. */
    // 父分区引用。
    // 持有对它所代表的 PipelinedSubpartition 实例的引用。所有实际的数据拉取和状态查询操作都会委托给这个父分区。
    private final PipelinedSubpartition parent;
    // 可用性监听器。 当父分区有新数据可用时，用来通知消费者的回调接口。
    // 通常由 PartitionRequestClient 或 Netty I/O 线程持有。
    private final BufferAvailabilityListener availabilityListener;

    /** Flag indicating whether this view has been released. */
    final AtomicBoolean isReleased;

    public PipelinedSubpartitionView(
            PipelinedSubpartition parent, BufferAvailabilityListener listener) {
        this.parent = checkNotNull(parent);
        this.availabilityListener = checkNotNull(listener);
        this.isReleased = new AtomicBoolean();
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        return parent.pollBuffer();
    }

    @Override
    public void notifyDataAvailable() {
        availabilityListener.notifyDataAvailable(this);
    }

    @Override
    public void notifyPriorityEvent(int priorityBufferNumber) {
        availabilityListener.notifyPriorityEvent(priorityBufferNumber);
    }

    @Override
    public void releaseAllResources() {
        if (isReleased.compareAndSet(false, true)) {
            // The view doesn't hold any resources and the parent cannot be restarted. Therefore,
            // it's OK to notify about consumption as well.
            parent.onConsumedSubpartition();
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased.get() || parent.isReleased();
    }

    @Override
    public void resumeConsumption() {
        parent.resumeConsumption();
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        parent.acknowledgeAllDataProcessed();
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
        return parent.getAvailabilityAndBacklog(isCreditAvailable);
    }

    @Override
    public Throwable getFailureCause() {
        Throwable cause = parent.getFailureCause();
        if (cause != null) {
            return new ProducerFailedException(cause);
        }
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return parent.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return parent.getNumberOfQueuedBuffers();
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        parent.bufferSize(newBufferSize);
    }

    @Override
    public int peekNextBufferSubpartitionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format(
                "%s(index: %d) of ResultPartition %s",
                this.getClass().getSimpleName(),
                parent.getSubPartitionIndex(),
                parent.parent.getPartitionId());
    }
}
