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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link CheckpointBarrierHandler} reacts to checkpoint barrier arriving from the input
 * channels. Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */

// 容错协议抽象： 它为 Flink 的两种主要 Checkpoint 模式（对齐 Checkpoint 和非对齐 Checkpoint）提供了一个统一的接口。
// 管理 Checkpoint 生命周期： 负责追踪 Checkpoint 的状态和计时，包括 Checkpoint 对齐的开始、结束、持续时间以及处理的数据量。
// 处理取消： 接收并处理 CancelCheckpointMarker，通知 Task 放弃正在进行的 Checkpoint。

public abstract class CheckpointBarrierHandler implements Closeable {
    private static final long OUTSIDE_OF_ALIGNMENT = Long.MIN_VALUE;

    /** The listener to be notified on complete checkpoints. */
    // Checkpoin 通知监听者。
    // Flink Task 自身的接口，当 Barrier Handler 确认 Checkpoint 完成或取消时，会调用此对象的 triggerCheckpointOnBarrier 或 abortCheckpointOnBarrier 方法
    private final CheckpointableTask toNotifyOnCheckpoint;
    // 时钟。
    // 用于测量 Checkpoint 相关的各种时间，例如对齐持续时间和 Checkpoint 延迟。
    private final Clock clock;

    /** The time (in nanoseconds) that the latest alignment took. */
    // 一个 Future，用于异步存储最新一次成功 Checkpoint 的对齐时间
    private CompletableFuture<Long> latestAlignmentDurationNanos = new CompletableFuture<>();

    /**
     * The time (in nanoseconds) between creation of the checkpoint's first checkpoint barrier and
     * receiving it by this task.
     */
    // 最新 Checkpoint 启动延迟。
    // 记录 Checkpoint Barrier 从 JobManager 发出到本 Task 接收到第一个 Barrier 之间的时间延迟（单位：纳秒）
    private long latestCheckpointStartDelayNanos;

    /** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
    // 对齐开始时间戳。
    // 记录最新一次对齐过程的起始时间
    private long startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;

    /** ID of checkpoint for which alignment was started last. */
    // 开始对齐的 Checkpoint ID。
    // 记录启动对齐过程的 Checkpoint 的 ID。
    // 如果 Checkpoint 完成，此 ID 用于将对齐指标与 Checkpoint 关联。
    private long startAlignmentCheckpointId = -1;

    /**
     * Cumulative counter of bytes processed during alignment. Once we complete alignment, we will
     * put this value into the {@link #latestBytesProcessedDuringAlignment}.
     */
    // 对齐期间处理的字节数。
    // 累积计数器，记录在 Checkpoint 对齐期间（即等待最后一个 Barrier 期间）Task 处理的数据字节总数。
    private long bytesProcessedDuringAlignment;
    // 最新对齐期间处理的字节数。
    // 一个 Future，用于异步存储最新一次成功 Checkpoint 对齐期间处理的字节数。
    private CompletableFuture<Long> latestBytesProcessedDuringAlignment = new CompletableFuture<>();
    // 任务完成后 Checkpoint 开关。
    // 指示是否允许在所有输入分区的 EndOfPartitionEvent 到达后，仍等待并完成 Checkpoint。
    private final boolean enableCheckpointAfterTasksFinished;

    public CheckpointBarrierHandler(
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            boolean enableCheckpointAfterTasksFinished) {
        this.toNotifyOnCheckpoint = checkNotNull(toNotifyOnCheckpoint);
        this.clock = checkNotNull(clock);
        this.enableCheckpointAfterTasksFinished = enableCheckpointAfterTasksFinished;
    }
    // 获取配置。
    // 返回是否启用了任务完成后继续 Checkpoint 的配置。
    boolean isCheckpointAfterTasksFinishedEnabled() {
        return enableCheckpointAfterTasksFinished;
    }

    @Override
    public void close() throws IOException {}

    // 处理 Checkpoint Barrier。 Task 接收到 Checkpoint Barrier 时调用
    public abstract void processBarrier(
            CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo, boolean isRpcTriggered)
            throws IOException;
    // 处理 Barrier 广播。
    // Task 接收到 Barrier 广播（Checkpoint Coordinator 通知 Barrier 即将到达）时调用。主要用于一些高级的 Checkpoint 优化。
    public abstract void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException;
    // 处理取消屏障。
    // Task 接收到 CancelCheckpointMarker 时调用，通知 Task 放弃正在进行的 Checkpoint。
    public abstract void processCancellationBarrier(
            CancelCheckpointMarker cancelBarrier, InputChannelInfo channelInfo) throws IOException;
    // 处理分区结束事件。
    // Task 接收到 EndOfPartitionEvent（表示上游通道数据已结束）时调用，用于管理 Checkpoint 的结束条件。
    public abstract void processEndOfPartition(InputChannelInfo channelInfo) throws IOException;
    // 获取最新 Checkpoint ID。
    // 返回当前正在进行或最近一次完成的 Checkpoint ID。
    public abstract long getLatestCheckpointId();
    // 获取对齐持续时间。
    // 如果任务正在对齐中，则返回当前已用时间；否则返回最新一次完成的对齐持续时间。
    public long getAlignmentDurationNanos() {
        if (isDuringAlignment()) {
            return clock.relativeTimeNanos() - startOfAlignmentTimestamp;
        } else {
            return FutureUtils.getOrDefault(latestAlignmentDurationNanos, 0L);
        }
    }
    // 获取 Checkpoint 启动延迟。
    // 返回最新 Checkpoint Barrier 从 JobManager 发出到 Task 接收到第一个 Barrier 之间的延迟时间。
    public long getCheckpointStartDelayNanos() {
        return latestCheckpointStartDelayNanos;
    }
    // 获取所有 Barrier 接收 Future。
    // 在基类中返回一个已完成的 Future（通常在具体实现中覆盖，用于等待对齐完成）
    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        return CompletableFuture.completedFuture(null);
    }
    // 通知 Task 触发 Checkpoint
    protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws IOException {
        CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointBarrier.getId(),
                        checkpointBarrier.getTimestamp(),
                        System.currentTimeMillis());

        CheckpointMetricsBuilder checkpointMetrics;
        if (checkpointBarrier.getId() == startAlignmentCheckpointId) {
            checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(latestAlignmentDurationNanos)
                            .setBytesProcessedDuringAlignment(latestBytesProcessedDuringAlignment)
                            .setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);
        } else {
            checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(0);
        }

        toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
                checkpointMetaData, checkpointBarrier.getCheckpointOptions(), checkpointMetrics);
    }
    // 通知 Task 取消 Checkpoint（因 Cancel Marker）
    protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
        notifyAbort(
                checkpointId,
                new CheckpointException(
                        CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
    }
    // 通知 Task 取消 Checkpoint
    protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
        toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
    }
    // 标记对齐开始和结束（非对齐 Checkpoint）
    protected void markAlignmentStartAndEnd(long checkpointId, long checkpointCreationTimestamp) {
        markAlignmentStart(checkpointId, checkpointCreationTimestamp);
        markAlignmentEnd(0);
    }
    // 标记对齐开始
    protected void markAlignmentStart(long checkpointId, long checkpointCreationTimestamp) {
        latestCheckpointStartDelayNanos =
                1_000_000 * Math.max(0, clock.absoluteTimeMillis() - checkpointCreationTimestamp);

        resetAlignment();
        startOfAlignmentTimestamp = clock.relativeTimeNanos();
        startAlignmentCheckpointId = checkpointId;
    }
    // 标记对齐结束
    protected void markAlignmentEnd() {
        markAlignmentEnd(clock.relativeTimeNanos() - startOfAlignmentTimestamp);
    }

    protected void markAlignmentEnd(long alignmentDuration) {
        checkState(
                alignmentDuration >= 0,
                "Alignment time is less than zero({}). Is the time monotonic?",
                alignmentDuration);

        latestAlignmentDurationNanos.complete(alignmentDuration);
        latestBytesProcessedDuringAlignment.complete(bytesProcessedDuringAlignment);

        startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;
        bytesProcessedDuringAlignment = 0;
    }

    protected void resetAlignment() {
        markAlignmentEnd(0);
        latestAlignmentDurationNanos = new CompletableFuture<>();
        latestBytesProcessedDuringAlignment = new CompletableFuture<>();
    }
    // 检查 Checkpoint 是否待处理。
    // 检查当前是否有 Checkpoint 正在等待完成（例如，等待所有 Barrier 到达）
    protected abstract boolean isCheckpointPending();

    public void addProcessedBytes(int bytes) {
        if (isDuringAlignment()) {
            bytesProcessedDuringAlignment += bytes;
        }
    }

    @VisibleForTesting
    boolean isDuringAlignment() {
        return startOfAlignmentTimestamp > OUTSIDE_OF_ALIGNMENT;
    }

    protected final Clock getClock() {
        return clock;
    }
}
