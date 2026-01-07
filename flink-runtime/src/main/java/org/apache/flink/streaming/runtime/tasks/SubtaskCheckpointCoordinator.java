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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Coordinates checkpointing-related work for a subtask (i.e. {@link
 * org.apache.flink.runtime.taskmanager.Task Task} and {@link StreamTask}). Responsibilities:
 *
 * <ol>
 *   <li>build a snapshot (invokable)
 *   <li>report snapshot to the JobManager
 *   <li>action upon checkpoint notification
 *   <li>maintain storage locations
 * </ol>
 */
// 定义了任何负责在 Task 级别管理检查点生命周期和状态快照的组件必须实现的功能。
// Flink TaskManager 中负责处理 Task 容错机制的核心组件。
// 它的主要作用是作为 Task 线程（StreamTask）与检查点存储和外部 JobMaster 之间的协调者。
// 快照构建 (Snapshot Building)： 协调 Task 内的算子链（OperatorChain）和输入/输出通道进行状态的同步和异步快照。
// 进度报告 (Reporting)： 将子任务完成的检查点快照结果报告给 JobMaster。
// 通道状态管理： 管理输入/输出通道的状态写入（特别是对于非对齐检查点）。
// 检查点通知处理： 接收并处理来自 JobMaster 关于检查点完成、中止或被取代的通知，并触发相应的本地清理或状态操作。
@Internal
public interface SubtaskCheckpointCoordinator extends Closeable {

    /** Initialize new checkpoint. */
    // 初始化新检查点。
    // 在 Task 收到检查点屏障或内部触发检查点后调用。
    // 它的主要作用是初始化通道状态写入器（ChannelStateWriter），准备开始缓冲或写入输入和输出通道的状态。
    void initInputsCheckpoint(long id, CheckpointOptions checkpointOptions)
            throws CheckpointException;
    // 获取通道状态写入器。
    // 返回用于写入输入和输出通道状态（针对非对齐检查点）的实例。
    // StreamTask 使用它来将写入器注入到输入门和结果分区中。
    ChannelStateWriter getChannelStateWriter();
    // 获取检查点存储视图。
    // 返回一个视图接口，用于访问检查点存储，特别是用于创建写入检查点数据的流（CheckpointStorageWorkerView）。
    CheckpointStorageWorkerView getCheckpointStorage();
    // 屏障中止检查点。
    // 当 Task 接收到取消检查点标记（CancelCheckpointMarker）时，或者在屏障对齐期间发生错误时调用。
    // 它通知 Task 放弃当前检查点 ID 的快照尝试，并释放相关的临时资源。
    void abortCheckpointOnBarrier(
            long checkpointId, CheckpointException cause, OperatorChain<?, ?> operatorChain)
            throws IOException;

    /** Must be called after {@link #initInputsCheckpoint(long, CheckpointOptions)}. */
    // 执行状态快照。 这是执行检查点快照的核心方法。
    // 它负责：1. 协调 OperatorChain 中的所有算子进行同步状态快照；
    // 2. 触发异步快照（如将状态写入远程存储）；3. 收集检查点指标；4. 向 JobMaster 报告快照结果。
    void checkpointState(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics,
            OperatorChain<?, ?> operatorChain,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws Exception;

    /**
     * Notified on the task side once a distributed checkpoint has been completed.
     *
     * @param checkpointId The checkpoint id to notify as been completed.
     * @param operatorChain The chain of operators executed by the task.
     * @param isRunning Whether the task is running.
     */
    // 检查点完成通知。
    // 当 JobMaster 通知该 Task，某个特定的检查点 ID 已全局成功完成时调用。
    // Task 可以在这里执行清理工作，例如丢弃旧的检查点数据。
    void notifyCheckpointComplete(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception;

    /**
     * Notified on the task side once a distributed checkpoint has been aborted.
     *
     * @param checkpointId The checkpoint id to notify as been completed.
     * @param operatorChain The chain of operators executed by the task.
     * @param isRunning Whether the task is running.
     */
    // 检查点中止通知。
    // 当 JobMaster 通知该 Task，某个检查点因失败或超时而被中止时调用。
    // Task 可以在这里执行相应的清理和恢复准备。
    void notifyCheckpointAborted(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception;

    /**
     * Notified on the task side once a distributed checkpoint has been subsumed.
     *
     * @param checkpointId The checkpoint id to notify as been subsumed.
     * @param operatorChain The chain of operators executed by the task.
     * @param isRunning Whether the task is running.
     */
    // 检查点取代通知。
    // 当 JobMaster 通知该 Task，某个旧的检查点已被更新、更成功的检查点取代（即旧检查点已过时并被丢弃）时调用。
    // Task 可以在这里清理旧检查点相关的本地资源。
    void notifyCheckpointSubsumed(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception;

    /** Waits for all the pending checkpoints to finish their asynchronous step. */
    // 等待挂起检查点完成。
    // 阻塞当前线程，直到所有已开始的异步检查点操作（如状态写入）都完成。
    // 这通常在 Task 即将关闭之前调用，以确保所有正在进行的快照工作都得到妥善处理。
    void waitForPendingCheckpoints() throws Exception;

    /** Cancel all resources. */
    void cancel() throws IOException;
}
