/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Public;

/**
 * This interface is typically only needed for transactional interaction with the "outside world",
 * like committing external side effects on checkpoints. An example is committing external
 * transactions once a checkpoint completes.
 *
 * <h3>Invocation Guarantees</h3>
 *
 * <p>It is NOT guaranteed that the implementation will receive a notification for each completed or
 * aborted checkpoint. While these notifications come in most cases, notifications might not happen,
 * for example, when a failure/restore happens directly after a checkpoint completed.
 *
 * <p>To handle this correctly, implementation should follow the "Checkpoint Subsuming Contract"
 * described below.
 *
 * <h3>Exceptions</h3>
 *
 * <p>The notifications from this interface come "after the fact", meaning after the checkpoint has
 * been aborted or completed. Throwing an exception will not change the completion/abortion of the
 * checkpoint.
 *
 * <p>Exceptions thrown from this method result in task- or job failure and recovery.
 *
 * <h3>Checkpoint Subsuming Contract</h3>
 *
 * <p>Checkpoint IDs are strictly increasing. A checkpoint with higher ID always subsumes a
 * checkpoint with lower ID. For example, when checkpoint T is confirmed complete, the code can
 * assume that no checkpoints with lower ID (T-1, T-2, etc.) are pending any more. <b>No checkpoint
 * with lower ID will ever be committed after a checkpoint with a higher ID.</b>
 *
 * <p>This does not necessarily mean that all of the previous checkpoints actually completed
 * successfully. It is also possible that some checkpoint timed out or was not fully acknowledged by
 * all tasks. Implementations must then behave as if that checkpoint did not happen. The recommended
 * way to do this is to let the completion of a new checkpoint (higher ID) subsume the completion of
 * all earlier checkpoints (lower ID).
 *
 * <p>This property is easy to achieve for cases where increasing "offsets", "watermarks", or other
 * progress indicators are communicated on checkpoint completion. A newer checkpoint will have a
 * higher "offset" (more progress) than the previous checkpoint, so it automatically subsumes the
 * previous one. Remember the "offset to commit" for a checkpoint ID and commit it when that
 * specific checkpoint (by ID) gets the notification that it is complete.
 *
 * <p>If you need to publish some specific artifacts (like files) or acknowledge some specific IDs
 * after a checkpoint, you can follow a pattern like below.
 *
 * <h3>Implementing Checkpoint Subsuming for Committing Artifacts</h3>
 *
 * <p>The following is a sample pattern how applications can publish specific artifacts on
 * checkpoint. Examples would be operators that acknowledge specific IDs or publish specific files
 * on checkpoint.
 *
 * <ul>
 *   <li>During processing, have two sets of artifacts.
 *       <ol>
 *         <li>A "ready set": Artifacts that are ready to be published as part of the next
 *             checkpoint. Artifacts are added to this set as soon as they are ready to be
 *             committed. This set is "transient", it is not stored in Flink's state persisted
 *             anywhere.
 *         <li>A "pending set": Artifacts being committed with a checkpoint. The actual publishing
 *             happens when the checkpoint is complete. This is a map of "{@code long =>
 *             List<Artifact>}", mapping from the id of the checkpoint when the artifact was ready
 *             to the artifacts. /li>
 *       </ol>
 *   <li>On checkpoint, add that set of artifacts from the "ready set" to the "pending set",
 *       associated with the checkpoint ID. The whole "pending set" gets stored in the checkpoint
 *       state.
 *   <li>On {@code notifyCheckpointComplete()} publish all IDs/artifacts from the "pending set" up
 *       to the checkpoint with that ID. Remove these from the "pending set".
 *   <li/>
 * </ul>
 *
 * <p>That way, even if some checkpoints did not complete, or if the notification that they
 * completed got lost, the artifacts will be published as part of the next checkpoint that
 * completes.
 */
// CheckpointListener 接口是 Flink 状态管理和**端到端一致性（End-to-End Consistency）**机制中非常重要的一环。
// 它使得 Flink 的算子（Operator）能够与外部系统进行事务性交互。
// 允许 Flink 的 状态（State） 或 算子（Operator） 接收关于 分布式检查点（Checkpoint） 完成或中止的通知。
// 核心作用是：
// 外部事务提交： 主要用于实现 Flink 的两阶段提交（Two-Phase Commit, 2PC）模式。
// 在检查点成功完成并持久化后，实现了此接口的算子会收到通知，此时它可以安全地向外部系统（如 Kafka、数据库）提交在检查点期间产生的外部副作用（External Side Effects），从而保证数据从 Flink 到外部系统的**精确一次（Exactly-Once）**语义。
// 清理/资源管理： 允许在检查点完成或中止后，进行必要的清理工作或资源释放（尽管不常用）。
// 关键概念： 任何需要对外部世界进行事务性操作（比如提交 Kafka 偏移量或写入事务文件）的 Flink 组件，都应该实现这个接口。
@Public
public interface CheckpointListener {

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * <p>These notifications are "best effort", meaning they can sometimes be skipped. To behave
     * properly, implementers need to follow the "Checkpoint Subsuming Contract". Please see the
     * {@link CheckpointListener class-level JavaDocs} for details.
     *
     * <p>Please note that checkpoints may generally overlap, so you cannot assume that the {@code
     * notifyCheckpointComplete()} call is always for the latest prior checkpoint (or snapshot) that
     * was taken on the function/operator implementing this interface. It might be for a checkpoint
     * that was triggered earlier. Implementing the "Checkpoint Subsuming Contract" (see above)
     * properly handles this situation correctly as well.
     *
     * <p>Please note that throwing exceptions from this method will not cause the completed
     * checkpoint to be revoked. Throwing exceptions will typically cause task/job failure and
     * trigger recovery.
     *
     * @param checkpointId The ID of the checkpoint that has been completed.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task. Note that this will NOT lead to the checkpoint being revoked.
     */
    // 检查点完成通知
    // 当具有给定 checkpointId 的分布式检查点成功完成并被 Flink 视为持久化后，该方法会被调用。
    // 核心用途： 触发外部事务的提交（第二阶段）。例如，一个 Kafka Sink 收到此通知后，会提交该检查点对应的所有已写入数据的事务，从而使数据对消费者可见。
    // 异常处理： 抛出异常不会撤销已完成的检查点，但会导致任务或作业失败并触发恢复。
    void notifyCheckpointComplete(long checkpointId) throws Exception;

    /**
     * This method is called as a notification once a distributed checkpoint has been aborted.
     *
     * <p><b>Important:</b> The fact that a checkpoint has been aborted does NOT mean that the data
     * and artifacts produced between the previous checkpoint and the aborted checkpoint are to be
     * discarded. The expected behavior is as if this checkpoint was never triggered in the first
     * place, and the next successful checkpoint simply covers a longer time span. See the
     * "Checkpoint Subsuming Contract" in the {@link CheckpointListener class-level JavaDocs} for
     * details.
     *
     * <p>These notifications are "best effort", meaning they can sometimes be skipped.
     *
     * <p>This method is very rarely necessary to implement. The "best effort" guarantee, together
     * with the fact that this method should not result in discarding any data (per the "Checkpoint
     * Subsuming Contract") means it is mainly useful for earlier cleanups of auxiliary resources.
     * One example is to pro-actively clear a local per-checkpoint state cache upon checkpoint
     * failure.
     *
     * @param checkpointId The ID of the checkpoint that has been aborted.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task or job.
     */
    // 检查点中止通知
    // 当具有给定 checkpointId 的分布式检查点被中止或失败时，该方法会被调用。这是一个默认方法，意味着不是必须实现。
    // 核心用途： 主要用于提前清理辅助资源（例如，清理一个本地的、与失败检查点相关的临时状态缓存）
    default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}
