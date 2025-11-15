/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A task that participates in checkpointing.
 *
 * @see TaskInvokable
 * @see AbstractInvokable
 */
// Flink 任务中用于实现容错和状态持久化的核心契约
// 定义了任务如何参与 Flink 的**分布式检查点（Checkpointing）**机制。
// 启动检查点： 能够作为 Source 任务，在收到 Checkpoint Coordinator 的指令后，异步启动一个检查点。
// 屏障对齐： 能够响应收到的检查点屏障 (Checkpoint Barrier)，执行状态快照和数据对齐。
// 生命周期通知： 能够接收并处理来自 Checkpoint Coordinator 的关于检查点完成、中止或被取代的通知。
@Internal
public interface CheckpointableTask {

    /**
     * This method is called to trigger a checkpoint, asynchronously by the checkpoint coordinator.
     *
     * <p>This method is called for tasks that start the checkpoints by injecting the initial
     * barriers, i.e., the source tasks. In contrast, checkpoints on downstream operators, which are
     * the result of receiving checkpoint barriers, invoke the {@link
     * #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetricsBuilder)}
     * method.
     *
     * @param checkpointMetaData Meta data for about this checkpoint
     * @param checkpointOptions Options for performing this checkpoint
     * @return future with value of {@code false} if the checkpoint was not carried out, {@code
     *     true} otherwise
     */
    // 异步触发检查点 (Source 任务)
    // 由 Checkpoint Coordinator 远程调用。此方法通常只在 Source 任务（数据源任务）上调用。
    // Source 任务收到此指令后，将异步启动检查点流程，主要包括：发送检查点屏障 (Checkpoint Barrier) 到其输出流中，并开始本地状态快照。
    CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions);

    /**
     * This method is called when a checkpoint is triggered as a result of receiving checkpoint
     * barriers on all input streams.
     *
     * @param checkpointMetaData Meta data for about this checkpoint
     * @param checkpointOptions Options for performing this checkpoint
     * @param checkpointMetrics Metrics about this checkpoint 当接收到所有输入流的检查点屏障 (checkpoint barrier) 时触发检查点
     * @throws IOException Exceptions thrown as the result of triggering a checkpoint are forwarded.
     */
    // 屏障触发检查点 (Downstream 任务)
    // 由任务自身调用。当一个非 Source 任务接收到所有输入流的检查点屏障时，会触发此方法。
    // 任务会执行屏障对齐（等待所有输入流的屏障）、执行本地状态快照，并将屏障转发到其输出流。
    void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException;

    /**
     * Invoked when a checkpoint has been completed, i.e., when the checkpoint coordinator has
     * received the notification from all participating tasks.
     *
     * @param checkpointId The ID of the checkpoint that is complete.
     * @return future that completes when the notification has been processed by the task.
     */
    // 通知检查点完成
    // 当 Checkpoint Coordinator 收到所有任务的确认，确认检查点成功完成时调用。
    // 任务可以利用此通知来执行清理操作，例如释放不再需要的旧检查点句柄。
    Future<Void> notifyCheckpointCompleteAsync(long checkpointId);

    /**
     * Invoked when a checkpoint has been aborted, i.e., when the checkpoint coordinator has
     * received a decline message from one task and try to abort the targeted checkpoint by
     * notification.
     *
     * @param checkpointId The ID of the checkpoint that is aborted.
     * @param latestCompletedCheckpointId The ID of the latest completed checkpoint.
     * @return future that completes when the notification has been processed by the task.
     */
    // 通知检查点中止
    // 当 Checkpoint Coordinator 发现检查点失败或被拒绝时调用。任务应处理此通知，例如清理与该失败检查点相关的临时资源。
    Future<Void> notifyCheckpointAbortAsync(long checkpointId, long latestCompletedCheckpointId);

    /**
     * Invoked when a checkpoint has been subsumed, i.e., when the checkpoint coordinator has
     * confirmed one checkpoint has been finished, and try to remove the first previous checkpoint.
     *
     * @param checkpointId The ID of the checkpoint that is subsumed.
     * @return future that completes when the notification has been processed by the task.
     */
    // 通知检查点被取代
    // 当 Checkpoint Coordinator 确定某个旧的已完成检查点已被新的检查点取代，不再需要保留时调用（例如，由于达到了保留检查点的数量限制）
    Future<Void> notifyCheckpointSubsumedAsync(long checkpointId);

    /**
     * Aborts a checkpoint as the result of receiving possibly some checkpoint barriers, but at
     * least one {@link org.apache.flink.runtime.io.network.api.CancelCheckpointMarker}.
     *
     * <p>This requires implementing tasks to forward a {@link
     * org.apache.flink.runtime.io.network.api.CancelCheckpointMarker} to their outputs.
     *
     * @param checkpointId The ID of the checkpoint to be aborted.
     * @param cause The reason why the checkpoint was aborted during alignment
     */
    // 屏障中止检查点
    // 当任务接收到取消检查点标记（CancelCheckpointMarker）时调用。
    // 任务必须停止当前正在进行的检查点对齐，并向其输出流转发 CancelCheckpointMarker，以通知下游任务中止该检查点。
    void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) throws IOException;
}
