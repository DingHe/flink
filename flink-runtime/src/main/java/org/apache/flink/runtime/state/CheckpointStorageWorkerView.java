/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.Environment;

import java.io.IOException;

/**
 * This interface implements the durable storage of checkpoint data and metadata streams. An
 * individual checkpoint or savepoint is stored to a {@link CheckpointStorageLocation} which created
 * by {@link CheckpointStorageCoordinatorView}.
 *
 * <p>Methods of this interface act as a worker role in task manager.
 */
// 定义了 Flink **任务管理器（TaskManager）上的工作者（Worker）**角色在处理检查点状态存储时所需的全部功能。
// 抽象了任务在执行检查点过程中，与底层存储系统交互，用于写入检查点数据流的操作。
// 定位存储： 负责将检查点协调器发送的存储引用解析成实际可写的流工厂。
// 数据流操作： 提供了打开数据流的方法，用于任务将它的状态数据持久化到检查点存储位置。
@Internal
public interface CheckpointStorageWorkerView {

    /**
     * Resolves a storage location reference into a CheckpointStreamFactory.
     *
     * <p>The reference may be the {@link CheckpointStorageLocationReference#isDefaultReference()
     * default reference}, in which case the method should return the default location, taking
     * existing configuration and checkpoint ID into account.
     *
     * @param checkpointId The ID of the checkpoint that the location is initialized for.
     * @param reference The checkpoint location reference.
     * @return A checkpoint storage location reflecting the reference and checkpoint ID.
     * @throws IOException Thrown, if the storage location cannot be initialized from the reference.
     */
    // 解析检查点存储位置。
    // 将检查点协调器 (CheckpointCoordinator) 提供的存储位置引用 (reference) 解析为一个检查点流工厂 (CheckpointStreamFactory)。
    // 任务通过这个工厂来创建实际的输出流，写入它的状态
    // checkpointId: 当前检查点的 ID
    // reference: 存储位置的抽象引用（可能是一个路径或 ID）。如果它是默认引用，方法应返回配置的默认位置
    CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException;

    /**
     * Opens a stream to persist checkpoint state data that is owned strictly by tasks and not
     * attached to the life cycle of a specific checkpoint.
     *
     * <p>This method should be used when the persisted data cannot be immediately dropped once the
     * checkpoint that created it is dropped. Examples are write-ahead-logs. For those, the state
     * can only be dropped once the data has been moved to the target system, which may sometimes
     * take longer than one checkpoint (if the target system is temporarily unable to keep up).
     *
     * <p>The fact that the job manager does not own the life cycle of this type of state means also
     * that it is strictly the responsibility of the tasks to handle the cleanup of this data.
     *
     * <p>Developer note: In the future, we may be able to make this a special case of "shared
     * state", where the task re-emits the shared state reference as long as it needs to hold onto
     * the persisted state data.
     *
     * @return A checkpoint state stream to the location for state owned by tasks.
     * @throws IOException Thrown, if the stream cannot be opened.
     */
    // 创建任务自有状态流
    // 用于持久化生命周期不严格绑定到特定检查点的状态数据，即任务自有状态（Task-Owned State）
    // 种状态的清理由任务自身负责，而不是由 JobManager 统一管理。常见的例子是 **RocksDB 的写前日志（WAL）数据。
    CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException;

    /**
     * A complementary method to {@link #createTaskOwnedStateStream()}. Creates a toolset that gives
     * access to additional operations that can be performed in the task owned state location.
     *
     * @return A toolset for additional operations for state owned by tasks.
     */
    // 创建任务自有状态工具集。
    // 返回一个工具集 (CheckpointStateToolset)，用于访问和执行与任务自有状态存储位置**相关的额外操作。
    // 例如，可能包括清理旧的、不再需要的任务自有状态的工具。
    CheckpointStateToolset createTaskOwnedCheckpointStateToolset();

    /**
     * Return {@link org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageAccess} if
     * file merging is enabled. Otherwise, return itself. File merging is supported by subclasses of
     * {@link org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess}.
     */
    // 文件合并存储转换（默认方法）。
    // 这是一个默认方法，用于在启用了文件合并优化功能时，返回一个支持文件合并的存储访问实例
    default CheckpointStorageWorkerView toFileMergingStorage(
            FileMergingSnapshotManager mergingSnapshotManager, Environment environment)
            throws IOException {
        return this;
    }
}
