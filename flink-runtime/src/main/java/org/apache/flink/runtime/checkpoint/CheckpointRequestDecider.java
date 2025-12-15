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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.IntSupplier;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.identityHashCode;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS;

/**
 * Decides whether a {@link CheckpointCoordinator.CheckpointTriggerRequest checkpoint request}
 * should be executed, dropped or postponed. Dropped requests are failed immediately. Postponed
 * requests are enqueued into a queue and can be dequeued later.
 *
 * <p>Decision is made according to:
 *
 * <ul>
 *   <li>checkpoint properties (e.g. isForce, isPeriodic)
 *   <li>checkpointing configuration (e.g. max concurrent checkpoints, min pause)
 *   <li>current state (other queued requests, pending checkpoints, last checkpoint completion time)
 * </ul>
 */
// 作为 检查点协调器 (CheckpointCoordinator) 的内部组件，专门负责接收、排队、排序和决策检查点触发请求 (CheckpointTriggerRequest) 的处理顺序。
// 在 Flink 运行时，可能同时发生多种检查点请求（例如，定时触发、手动 Savepoint 触发等）。该决策器根据以下几个关键因素，对每个新请求做出判断：
// 执行 (Execute): 立即执行该请求（或队列中的下一个最高优先级请求）。
// 推迟 (Postpone): 将请求放入队列中等待条件满足。
// 丢弃 (Drop): 立即拒绝并失败该请求。
// 决策依据主要包括： 最大并发检查点数限制、最小检查点间隔、请求的类型（Savepoint、强制、周期性）以及当前队列状态。

@SuppressWarnings("ConstantConditions")
class CheckpointRequestDecider {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRequestDecider.class);
    // 排队时间日志阈值。
    // 超过这个时间（默认为 100 毫秒）的请求，其排队时间会被记录到 INFO 级别的日志中，用于监控和调试。
    private static final int LOG_TIME_IN_QUEUE_THRESHOLD_MS = 100;
    // 默认最大排队请求数。
    // 队列中允许存在的检查点请求的最大数量，默认值为 1000。
    private static final int DEFAULT_MAX_QUEUED_REQUESTS = 1000;
    // 最大并发检查点尝试数。
    // JobManager 允许同时处于 Pending 状态的检查点数量上限，这是一个核心配置参数。
    private final int maxConcurrentCheckpointAttempts;
    // 周期性触发器重调度函数。
    // 当周期性检查点因不满足最小间隔时间限制而被拒绝时，调用此函数来重置定时器，等待指定的延迟时间 (Long, Long 参数分别代表当前相对时间和延迟时间)
    private final BiConsumer<Long, Long> rescheduleTrigger;
    // 时间提供者。
    // 用于获取当前时间（通常是相对时间），以便计算检查点之间的时间间隔。
    private final Clock clock;
    // 最小检查点间隔时间（毫秒）。
    // 强制要求上一个检查点完成后，必须等待这段时间后才能触发下一个周期性检查点。
    private final long minPauseBetweenCheckpoints;
    // Pending 检查点数量提供者。
    // 一个函数式接口，用于动态获取当前正在进行中（Pending）的检查点数量。
    private final IntSupplier pendingCheckpointsSizeSupplier;
    // 正在清理的检查点数量提供者。
    // 一个函数式接口，用于获取当前正在进行清理（如删除旧文件）的检查点数量。这个值在某些逻辑中也会计入对并发数的限制。
    private final IntSupplier numberOfCleaningCheckpointsSupplier;
    // 排队请求集合。
    // 存储所有等待调度的检查点请求的队列。
    // 它使用 TreeSet 实现了 优先级队列 的功能，保证请求总是按照 checkpointTriggerRequestsComparator 定义的优先级顺序排列。
    private final NavigableSet<CheckpointTriggerRequest> queuedRequests =
            new TreeSet<>(checkpointTriggerRequestsComparator());
    // 队列中最大的请求数
    private final int maxQueuedRequests;

    CheckpointRequestDecider(
            int maxConcurrentCheckpointAttempts,
            BiConsumer<Long, Long> rescheduleTrigger,
            Clock clock,
            long minPauseBetweenCheckpoints,
            IntSupplier pendingCheckpointsSizeSupplier,
            IntSupplier numberOfCleaningCheckpointsSupplier) {
        this(
                maxConcurrentCheckpointAttempts,
                rescheduleTrigger,
                clock,
                minPauseBetweenCheckpoints,
                pendingCheckpointsSizeSupplier,
                numberOfCleaningCheckpointsSupplier,
                DEFAULT_MAX_QUEUED_REQUESTS);
    }

    CheckpointRequestDecider(
            int maxConcurrentCheckpointAttempts,
            BiConsumer<Long, Long> rescheduleTrigger,
            Clock clock,
            long minPauseBetweenCheckpoints,
            IntSupplier pendingCheckpointsSizeSupplier,
            IntSupplier numberOfCleaningCheckpointsSupplier,
            int maxQueuedRequests) {
        Preconditions.checkArgument(maxConcurrentCheckpointAttempts > 0);
        Preconditions.checkArgument(maxQueuedRequests > 0);
        this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
        this.rescheduleTrigger = rescheduleTrigger;
        this.clock = clock;
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.pendingCheckpointsSizeSupplier = pendingCheckpointsSizeSupplier;
        this.numberOfCleaningCheckpointsSupplier = numberOfCleaningCheckpointsSupplier;
        this.maxQueuedRequests = maxQueuedRequests;
    }

    /**
     * Submit a new checkpoint request and decide whether it or some other request can be executed.
     *
     * @return request that should be executed
     */
    // 处理新检查点请求的入口点。
    // 它负责执行队列容量管理（丢弃优先级最低的请求或新请求）和调用核心决策逻辑来选择下一个要执行的请求。
    // 接收新的请求 (newRequest)、当前是否正在触发检查点 (isTriggering) 的布尔值，以及上一个检查点完成的时间 (lastCompletionMs)，返回一个可能被立即执行的请求。
    Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            CheckpointTriggerRequest newRequest, boolean isTriggering, long lastCompletionMs) {
        // 检查当前队列是否已达到最大容量 (maxQueuedRequests) 并且队列中优先级最低的请求（即 last()，因为 TreeSet 是升序排列，优先级最低在末尾）不是周期性请求 (!isPeriodic)。
        // 这种情况队列中都是用户提交的请求，应保留它们并丢弃新来的请求。
        if (queuedRequests.size() >= maxQueuedRequests && !queuedRequests.last().isPeriodic) {
            // there are only non-periodic (ie user-submitted) requests enqueued - retain them and
            // drop the new one
            newRequest.completeExceptionally(new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
            return Optional.empty();
        } else {
            queuedRequests.add(newRequest);
            if (queuedRequests.size() > maxQueuedRequests) {
                // 移除优先级最低的请求
                queuedRequests
                        .pollLast()
                        .completeExceptionally(
                                new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
            }
            // 调用核心决策方法
            Optional<CheckpointTriggerRequest> request =
                    chooseRequestToExecute(isTriggering, lastCompletionMs);
            request.ifPresent(CheckpointRequestDecider::logInQueueTime);
            return request;
        }
    }

    /**
     * Choose one of the queued requests to execute, if any.
     *
     * @return request that should be executed
     */
    Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute(
            boolean isTriggering, long lastCompletionMs) {
        Optional<CheckpointTriggerRequest> request =
                chooseRequestToExecute(isTriggering, lastCompletionMs);
        request.ifPresent(CheckpointRequestDecider::logInQueueTime);
        return request;
    }

    /**
     * Choose the next {@link CheckpointTriggerRequest request} to execute based on the provided
     * candidate and the current state. Acquires a lock and may update the state.
     *
     * @return request that should be executed
     */
    // 用于决定下一个要执行的检查点请求的核心逻辑方法。
    // 在请求队列非空的情况下，它根据当前的并发限制、最小间隔配置以及请求的优先级（Savepoint > 强制 > 非周期性 > 周期性），从队列中选择一个满足条件的最高优先级请求并将其移除，准备执行。
    // 接收当前是否有检查点正在触发 (isTriggering) 和上一个检查点完成的时间戳 (lastCompletionMs)。
    private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            boolean isTriggering, long lastCompletionMs) {
        // 正在触发中。 检查 JobManager 是否已经开始触发一个检查点（防止重复触发）。
        if (isTriggering
                || queuedRequests.isEmpty()
                || numberOfCleaningCheckpointsSupplier.getAsInt()
                        > maxConcurrentCheckpointAttempts) {
            return Optional.empty();
        }
        // 并发检查： 检查当前正在进行中（Pending）的检查点数量是否已达到最大并发限制 (maxConcurrentCheckpointAttempts)
        if (pendingCheckpointsSizeSupplier.getAsInt() >= maxConcurrentCheckpointAttempts) {
            // 强制规则： 只有强制请求才能突破并发限制。
            return Optional.of(queuedRequests.first())
                    .filter(CheckpointTriggerRequest::isForce)
                    .map(unused -> queuedRequests.pollFirst());
        }
        // 获取最高优先级请求： 获取当前队列中优先级最高的请求，但不将其移除（等待后续检查）。
        CheckpointTriggerRequest first = queuedRequests.first();
        // 最小间隔检查条件： 检查该请求是否既不是强制请求 (!isForce)，又是周期性请求 (isPeriodic)。只有周期性且非强制的请求才受最小间隔限制。
        if (!first.isForce() && first.isPeriodic) {
            long currentRelativeTime = clock.relativeTimeMillis();
            long nextTriggerDelayMillis =
                    lastCompletionMs - currentRelativeTime + minPauseBetweenCheckpoints;
            if (nextTriggerDelayMillis > 0) {
                queuedRequests
                        .pollFirst()
                        .completeExceptionally(
                                new CheckpointException(MINIMUM_TIME_BETWEEN_CHECKPOINTS));
                rescheduleTrigger.accept(currentRelativeTime, nextTriggerDelayMillis);
                return Optional.empty();
            }
        }

        return Optional.of(queuedRequests.pollFirst());
    }

    @VisibleForTesting
    @Deprecated
    PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
        return new PriorityQueue<>(queuedRequests);
    }

    void abortAll(CheckpointException exception) {
        while (!queuedRequests.isEmpty()) {
            queuedRequests.pollFirst().completeExceptionally(exception);
        }
    }

    int getNumQueuedRequests() {
        return queuedRequests.size();
    }

    private static Comparator<CheckpointTriggerRequest> checkpointTriggerRequestsComparator() {
        return (r1, r2) -> {
            if (r1.props.isSavepoint() != r2.props.isSavepoint()) {
                return r1.props.isSavepoint() ? -1 : 1;
            } else if (r1.isForce() != r2.isForce()) {
                return r1.isForce() ? -1 : 1;
            } else if (r1.isPeriodic != r2.isPeriodic) {
                return r1.isPeriodic ? 1 : -1;
            } else if (r1.timestamp != r2.timestamp) {
                return Long.compare(r1.timestamp, r2.timestamp);
            } else {
                return Integer.compare(identityHashCode(r1), identityHashCode(r2));
            }
        };
    }

    private static void logInQueueTime(CheckpointTriggerRequest request) {
        if (LOG.isInfoEnabled()) {
            long timeInQueue = currentTimeMillis() - request.timestamp;
            if (timeInQueue > LOG_TIME_IN_QUEUE_THRESHOLD_MS) {
                LOG.info("checkpoint request time in queue: {}", timeInQueue);
            }
        }
    }
}
