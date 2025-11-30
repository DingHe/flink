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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Delegate class responsible for checkpoints cleaning and counting the number of checkpoints yet to
 * clean.
 */
// CheckpointsCleaner 是 Flink 检查点协调器 (Checkpoint Coordinator) 的委托类，专门负责管理和执行检查点 (Checkpoint) 的清理工作。
// 主要职责是异步地释放不再需要的检查点所占用的资源，包括元数据和实际的状态文件。这些不再需要的检查点通常是：
// 被新检查点取代 (Subsumed) 的旧检查点。
// 在存储过程中失败的检查点。
// 作业关闭时需要最终清理的所有检查点。
@ThreadSafe
public class CheckpointsCleaner implements Serializable, AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointsCleaner.class);
    private static final long serialVersionUID = 2545865801947537790L;
    // 并行清理模式开关。
    // 决定是否应使用异步/并行方式来执行检查点的丢弃操作。
    // 可以通过 CheckpointingOptions.CLEANER_PARALLEL_MODE 配置。
    private final boolean parallelMode;
    private final Object lock = new Object();
    // 待清理检查点计数器。
    // 记录当前正在进行异步清理（即已触发清理但尚未完成）的检查点数量。
    @GuardedBy("lock")
    private int numberOfCheckpointsToClean;
    // 异步关闭的 Future。
    // 在调用 closeAsync() 时初始化。
    // 当 numberOfCheckpointsToClean 降为 0 时，该 Future 会被完成 (complete)，表示所有待清理工作已完成，CheckpointsCleaner 已安全关闭。
    @GuardedBy("lock")
    @Nullable
    private CompletableFuture<Void> cleanUpFuture;
    // 存储那些已被 CompletedCheckpointStore 取代（因容量限制而被移除），但尚未开始清理的旧检查点。
    /** All subsumed checkpoints. */
    @GuardedBy("lock")
    private final List<CompletedCheckpoint> subsumedCheckpoints = new ArrayList<>();

    public CheckpointsCleaner() {
        this.parallelMode = CheckpointingOptions.CLEANER_PARALLEL_MODE.defaultValue();
    }

    public CheckpointsCleaner(boolean parallelMode) {
        this.parallelMode = parallelMode;
    }

    int getNumberOfCheckpointsToClean() {
        synchronized (lock) {
            return numberOfCheckpointsToClean;
        }
    }
    // 启动单个检查点的异步清理过程。
    // 参数 checkpoint： 要清理的检查点对象（可以是 CompletedCheckpoint 或 Checkpoint 抽象类型）。
    // 参数 shouldDiscard： 布尔值，指示是否应该真正执行丢弃操作（即释放资源）。如果为 false，则只执行 postCleanAction。
    // 参数 postCleanAction： 检查点清理完成后需要执行的回调逻辑。
    public void cleanCheckpoint(
            Checkpoint checkpoint,
            boolean shouldDiscard,
            Runnable postCleanAction,
            Executor executor) {
        LOG.debug(
                "Clean checkpoint {} parallel-mode={} shouldDiscard={}",
                checkpoint.getCheckpointID(),
                parallelMode,
                shouldDiscard);
        if (shouldDiscard) {
            incrementNumberOfCheckpointsToClean();

            Checkpoint.DiscardObject discardObject = checkpoint.markAsDiscarded();
            CompletableFuture<Void> discardFuture =
                    parallelMode
                            ? discardObject.discardAsync(executor)
                            : FutureUtils.runAsync(discardObject::discard, executor);
            discardFuture.handle(
                    (Object outerIgnored, Throwable outerThrowable) -> {
                        if (outerThrowable != null) {
                            LOG.warn(
                                    "Could not properly discard completed checkpoint {}.",
                                    checkpoint.getCheckpointID(),
                                    outerThrowable);
                        }

                        decrementNumberOfCheckpointsToClean();
                        postCleanAction.run();
                        return null;
                    });
        } else {
            executor.execute(postCleanAction);
        }
    }

    /**
     * Add one subsumed checkpoint to CheckpointsCleaner, the subsumed checkpoint would be discarded
     * at {@link #cleanSubsumedCheckpoints(long, Set, Runnable, Executor)}.
     *
     * @param completedCheckpoint which is subsumed.
     */
    public void addSubsumedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        synchronized (lock) {
            subsumedCheckpoints.add(completedCheckpoint);
        }
    }

    /**
     * Clean checkpoint that is not in the given {@param stillInUse}.
     *
     * @param upTo lowest CheckpointID which is still valid.
     * @param stillInUse the state of those checkpoints are still referenced.
     * @param postCleanAction post action after cleaning.
     * @param executor is used to perform the cleanup logic.
     */
    public void cleanSubsumedCheckpoints(
            long upTo, Set<Long> stillInUse, Runnable postCleanAction, Executor executor) {
        synchronized (lock) {
            Iterator<CompletedCheckpoint> iterator = subsumedCheckpoints.iterator();
            while (iterator.hasNext()) {
                CompletedCheckpoint checkpoint = iterator.next();
                if (checkpoint.getCheckpointID() < upTo
                        && !stillInUse.contains(checkpoint.getCheckpointID())) {
                    try {
                        LOG.debug("Try to discard checkpoint {}.", checkpoint.getCheckpointID());
                        cleanCheckpoint(
                                checkpoint,
                                checkpoint.shouldBeDiscardedOnSubsume(),
                                postCleanAction,
                                executor);
                        iterator.remove();
                    } catch (Exception e) {
                        LOG.warn("Fail to discard the old checkpoint {}.", checkpoint);
                    }
                }
            }
        }
    }

    public void cleanCheckpointOnFailedStoring(
            CompletedCheckpoint completedCheckpoint, Executor executor) {
        cleanCheckpoint(completedCheckpoint, true, () -> {}, executor);
    }

    private void incrementNumberOfCheckpointsToClean() {
        synchronized (lock) {
            checkState(cleanUpFuture == null, "CheckpointsCleaner has already been closed");
            numberOfCheckpointsToClean++;
        }
    }

    private void decrementNumberOfCheckpointsToClean() {
        synchronized (lock) {
            numberOfCheckpointsToClean--;
            maybeCompleteCloseUnsafe();
        }
    }

    private void maybeCompleteCloseUnsafe() {
        if (numberOfCheckpointsToClean == 0 && cleanUpFuture != null) {
            cleanUpFuture.complete(null);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (cleanUpFuture == null) {
                cleanUpFuture = new CompletableFuture<>();
            }
            maybeCompleteCloseUnsafe();
            subsumedCheckpoints.clear();
            return cleanUpFuture;
        }
    }
}
