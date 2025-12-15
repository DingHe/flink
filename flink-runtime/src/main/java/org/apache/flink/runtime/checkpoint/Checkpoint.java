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

import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** A checkpoint, pending or completed. */
// Checkpoint 接口是 Flink 中所有检查点实体的顶级抽象
// 定义了任何 Flink 检查点（无论是正在进行中的 PendingCheckpoint 还是已完成的 CompletedCheckpoint）都必须具备的基本信息和生命周期管理能力，尤其是关于资源的清理和丢弃操作。
// 提供了一个通用的标识符 (CheckpointID) 和一套安全的机制，用于在检查点不再需要时（例如失败、超时或被清理）释放其关联的所有资源。
public interface Checkpoint {
    // 空操作丢弃对象。
    // 这是一个用于占位的常量实例，它的 discard() 方法不执行任何操作 (NOOP - No Operation)
    DiscardObject NOOP_DISCARD_OBJECT = () -> {};
    // 获取检查点 ID。
    // 返回此检查点实例的唯一标识符（一个递增的 Long 整数）。
    long getCheckpointID();

    /**
     * This method precede the {@link DiscardObject#discard()} method and should be called from the
     * {@link CheckpointCoordinator}(under the lock) while {@link DiscardObject#discard()} can be
     * called from any thread/place.
     */
    // 标记为已丢弃。
    // 这个方法用于在 CheckpointCoordinator 的锁保护下（即在主线程中）对检查点进行状态标记，表示该检查点已不再有用（无论是失败还是被取代）。
    DiscardObject markAsDiscarded();

    /** Extra interface for discarding the checkpoint. */
    interface DiscardObject {
        // 执行丢弃操作。
        // 包含实际清理检查点资源（如释放内存状态、清理持久化存储中的元数据文件或数据文件）的逻辑。这个方法可以从任何线程调用。
        void discard() throws Exception;
        // 异步丢弃。
        default CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            return FutureUtils.runAsync(this::discard, ioExecutor);
        }
    }
}
