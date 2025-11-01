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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which allows to reconnect after failure. Only one view
 * is allowed at a time to read teh subpartition.
 */
// Flink 中用于实现近似恢复（Approximate Recovery）或称为弹性恢复（Resilience/Eager Scheduling）场景的特殊子分区
// 在支持流式数据传输的同时，允许下游消费者在失败后重新连接到该子分区，并从头开始消费数据，以支持弹性恢复策略。
// 可重连（Reconnectable）： 允许新的读取视图在旧视图被释放后创建，而标准管道化子分区一旦被消费就不能再创建新的视图。
// 部分记录清理（Partial Record Cleanup）： 专为处理重连时队列中可能残留的不完整记录而设计。由于消费者失败，最后一个 BufferConsumer 中的记录可能只写入了一部分，重连后需要清理这部分不完整数据，以避免向下游发送错误或截断的记录
// 不支持通道状态恢复： 明确表示不参与精确一次 (Exactly-Once) Checkpoint 机制中的飞行中数据 (In-flight Data) 捕获和恢复（通过 isSupportChannelStateRecover() 返回 false 体现）。
public class PipelinedApproximateSubpartition extends PipelinedSubpartition {

    private static final Logger LOG =
            LoggerFactory.getLogger(PipelinedApproximateSubpartition.class);

    @GuardedBy("buffers")
    private boolean isPartialBufferCleanupRequired = false;

    PipelinedApproximateSubpartition(
            int index, int receiverExclusiveBuffersPerChannel, ResultPartition parent) {
        super(index, receiverExclusiveBuffersPerChannel, parent);
    }

    /**
     * To simply the view releasing threading model, {@link
     * PipelinedApproximateSubpartition#releaseView()} is called only before creating a new view.
     *
     * <p>There is still one corner case when a downstream task fails continuously in a short period
     * of time then multiple netty worker threads can createReadView at the same time. TODO: This
     * problem will be solved in FLINK-19774
     */
    @Override
    public PipelinedSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) {
        synchronized (buffers) {
            checkState(!isReleased);

            releaseView();

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            readView = new PipelinedApproximateSubpartitionView(this, availabilityListener);
        }

        return readView;
    }

    @Override
    Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
        if (isPartialBufferCleanupRequired) {
            isPartialBufferCleanupRequired = !buffer.cleanupPartialRecord();
        }

        return buffer.build();
    }

    private void releaseView() {
        assert Thread.holdsLock(buffers);
        if (readView != null) {
            // upon reconnecting, two netty threads may require the same view to release
            LOG.debug(
                    "Releasing view of subpartition {} of {}.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            readView.releaseAllResources();
            readView = null;

            isPartialBufferCleanupRequired = true;
            isBlocked = false;
            sequenceNumber = 0;
        }
    }

    @Override
    public boolean isSupportChannelStateRecover() {
        return false;
    }

    /** for testing only. */
    @VisibleForTesting
    boolean isPartialBufferCleanupRequired() {
        return isPartialBufferCleanupRequired;
    }

    /** for testing only. */
    @VisibleForTesting
    void setIsPartialBufferCleanupRequired() {
        isPartialBufferCleanupRequired = true;
    }
}
