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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Public;

import java.io.IOException;
import java.util.Collection;

/**
 * The {@code Committer} is responsible for committing the data staged by the {@link
 * CommittingSinkWriter} in the second step of a two-phase commit protocol.
 *
 * <p>A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will
 * restart from previous checkpoint and re-attempt to commit all committables. Thus, some or all
 * committables may have already been committed. These {@link CommitRequest}s must not change the
 * external system and implementers are asked to signal {@link
 * CommitRequest#signalAlreadyCommitted()}.
 *
 * @param <CommT> The type of information needed to commit the staged data
 */
// Committer 接口是 Flink SinkV2 API 的核心组件之一，用于执行两阶段提交（Two-Phase Commit, 2PC）协议的第二阶段：最终提交。
// 最终确认数据： 它接收由 CommittingSinkWriter 在检查点期间生成的 Committable 对象，并执行必要的外部操作（如重命名文件、标记事务完成、写入数据库索引），从而将数据从临时状态转为永久可见状态。
// 实现精确一次语义： Committer 通常运行在一个独立的 Flink 协调任务或 JobManager 上。它在 Flink 检查点成功后才被触发执行提交，确保了**精确一次（Exactly-Once）**的一致性。
// 处理幂等性与重试： Flink 在提交阶段可能会失败并重启，因此 commit 方法会被多次调用，并且提交的内容可能包含已经被提交过的 Committables。Committer 必须是幂等的，能够安全地处理重复提交的请求。
// Committer 是 Flink Exactly-Once Sink 的最后一步执行者，负责将预提交的元数据转化为对外部存储的最终、永久性写入。
// CommT	提交类型	由 CommittingSinkWriter 生成的、用于指导 Committer 完成最终提交的元数据类型。例如，如果 Sink 写入文件，CommT 可能包含要提交的文件路径。
@Public
public interface Committer<CommT> extends AutoCloseable {
    /**
     * Commit the given list of {@link CommT}.
     *
     * @param committables A list of commit requests staged by the sink writer.
     * @throws IOException for reasons that may yield a complete restart of the job.
     */
    // 执行最终提交。
    // 这是 Committer 的主要业务逻辑。
    // Flink 将所有并行 SinkWriter 生成的 CommitRequest（包含 CommT 元数据）发送给 Committer。
    // 实现者必须遍历这些请求，尝试对外部系统进行提交。
    // 如果提交失败，实现者应使用 CommitRequest 接口提供的方法（如 retryLater()）来指示 Flink 如何处理该失败。
    void commit(Collection<CommitRequest<CommT>> committables)
            throws IOException, InterruptedException;

    /**
     * A request to commit a specific committable.
     *
     * @param <CommT>
     */
    // 封装了一个需要提交的 Committable (CommT) 及其在 Flink 运行时中的重试和失败处理机制。
    @Public
    interface CommitRequest<CommT> {

        /** Returns the committable. */
        // 获取 Committable。
        // 返回 Committer 需要处理的实际元数据对象 (CommT)。
        CommT getCommittable();

        /**
         * Returns how many times this particular committable has been retried. Starts at 0 for the
         * first attempt.
         */
        // 获取重试次数。
        // 返回 Flink 尝试提交此 Committable 的次数（首次尝试返回 0）。
        // 这有助于 Committer 实现者根据重试次数调整策略或进行日志记录。
        int getNumberOfRetries();

        /**
         * The commit failed for known reason and should not be retried.
         *
         * <p>Currently calling this method only logs the error, discards the comittable and
         * continues. In the future the behaviour might be configurable.
         */
        // 已知原因失败信号。
        // 告知 Flink 提交失败是由于已知、非重试性的原因。
        // 目前 Flink 记录错误并丢弃此 Committable，并继续执行。
        void signalFailedWithKnownReason(Throwable t);

        /**
         * The commit failed for unknown reason and should not be retried.
         *
         * <p>Currently calling this method fails the job. In the future the behaviour might be
         * configurable.
         */
        // 未知原因失败信号。
        // 告知 Flink 提交失败是由于未知、不可恢复的原因。目前 Flink 会使整个 Job 失败，因为它暗示了外部系统或 Flink 内部存在严重问题。
        void signalFailedWithUnknownReason(Throwable t);

        /**
         * The commit failed for a retriable reason. If the sink supports a retry maximum, this may
         * permanently fail after reaching that maximum. Else the committable will be retried as
         * long as this method is invoked after each attempt.
         */
        // 稍后重试。
        // 告知 Flink 提交暂时失败，但可以稍后再次尝试。这通常用于临时的网络或资源问题。
        void retryLater();

        /**
         * Updates the underlying committable and retries later (see {@link #retryLater()} for a
         * description). This method can be used if a committable partially succeeded.
         */
        // 更新并重试。 用于 Committable 仅部分成功提交的场景。
        // Committer 可以更新 CommT（例如，移除已成功提交的部分），然后要求 Flink 使用更新后的 Committable 稍后重试。
        void updateAndRetryLater(CommT committable);

        /**
         * Signals that a committable is skipped as it was committed already in a previous run.
         * Using this method is optional but eases bookkeeping and debugging. It also serves as a
         * code documentation for the branches dealing with recovery.
         */
        // 已提交信号。
        // 明确告知 Flink，此 Committable 在本次提交之前已经成功提交
        void signalAlreadyCommitted();
    }
}
