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

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This interface creates a {@link CheckpointStorageLocation} to which an individual checkpoint or
 * savepoint is stored.
 *
 * <p>Methods of this interface act as an administration role in checkpoint coordinator.
 */
// CheckpointStorageCoordinatorView 接口定义了 检查点协调器 (CheckpointCoordinator) 与检查点存储后端 (CheckpointStorage) 之间进行交互的视图或契约。
// 抽象了所有与存储位置初始化、定位和解析相关的操作。
// 它允许 CheckpointCoordinator 在不关心底层存储实现细节的情况下（例如是文件系统、RocksDB 还是其他服务），完成以下关键管理任务：
// 初始化检查点或 Savepoint 的存储位置。
// 解析外部存储指针（例如一个文件路径或 ID）到具体的存储句柄。
// 查询存储能力（例如是否支持高可用）
public interface CheckpointStorageCoordinatorView {

    /**
     * Checks whether this backend supports highly available storage of data.
     *
     * <p>Some state backends may not support highly-available durable storage, with default
     * settings, which makes them suitable for zero-config prototyping, but not for actual
     * production setups.
     */
    // 支持高可用存储检查。
    // 检查当前存储后端是否支持高可用 (HA) 的持久化存储。
    boolean supportsHighlyAvailableStorage();

    /** Checks whether the storage has a default savepoint location configured. */
    // 默认 Savepoint 位置检查。
    // 检查存储后端是否已配置一个默认的 Savepoint 存储位置
    boolean hasDefaultSavepointLocation();

    /**
     * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
     * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
     *
     * <p>If the state backend cannot understand the format of the pointer (for example because it
     * was created by a different state backend) this method should throw an {@code IOException}.
     *
     * @param externalPointer The external checkpoint pointer to resolve.
     * @return The checkpoint location handle.
     * @throws IOException Thrown, if the state backend does not understand the pointer, or if the
     *     pointer could not be resolved due to an I/O error.
     */
    // 解析外部指针。
    // 将一个外部检查点/Savepoint 的指针（例如，一个路径或 ID）解析为一个具体的、已完成的检查点存储位置句柄 (CompletedCheckpointStorageLocation)
    CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

    /**
     * Initializes the necessary prerequisites for storage locations of checkpoints.
     *
     * <p>For file-based checkpoint storage, this method would initialize essential base checkpoint
     * directories on checkpoint coordinator side and should be executed before calling {@link
     * #initializeLocationForCheckpoint(long)}.
     *
     * @throws IOException Thrown, if these base storage locations cannot be initialized due to an
     *     I/O exception.
     */
    // 初始化基础存储位置。
    // 在创建检查点之前，初始化存储后端所需的所有基础目录或资源。
    // 对于基于文件的存储，这通常是创建 Job 的顶级检查点目录。
    void initializeBaseLocationsForCheckpoint() throws IOException;

    /**
     * Initializes a storage location for new checkpoint with the given ID.
     *
     * <p>The returned storage location can be used to write the checkpoint data and metadata to and
     * to obtain the pointers for the location(s) where the actual checkpoint data should be stored.
     *
     * @param checkpointId The ID (logical timestamp) of the checkpoint that should be persisted.
     * @return A storage location for the data and metadata of the given checkpoint.
     * @throws IOException Thrown if the storage location cannot be initialized due to an I/O
     *     exception.
     */
    // 初始化检查点存储位置。
    // 为一个新的检查点（由 checkpointId 标识）初始化一个专门的存储位置 (CheckpointStorageLocation)
    CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException;

    /**
     * Initializes a storage location for new savepoint with the given ID.
     *
     * <p>If an external location pointer is passed, the savepoint storage location will be
     * initialized at the location of that pointer. If the external location pointer is null, the
     * default savepoint location will be used. If no default savepoint location is configured, this
     * will throw an exception. Whether a default savepoint location is configured can be checked
     * via {@link #hasDefaultSavepointLocation()}.
     *
     * @param checkpointId The ID (logical timestamp) of the savepoint's checkpoint.
     * @param externalLocationPointer Optionally, a pointer to the location where the savepoint
     *     should be stored. May be null.
     * @return A storage location for the data and metadata of the savepoint.
     * @throws IOException Thrown if the storage location cannot be initialized due to an I/O
     *     exception.
     */
    // 初始化 Savepoint 存储位置。 为一个新的 Savepoint 初始化存储位置
    // checkpointId: Savepoint 所基于的检查点 ID
    // externalLocationPointer: 可选的外部路径，指定 Savepoint 的存储位置。
    CheckpointStorageLocation initializeLocationForSavepoint(
            long checkpointId, @Nullable String externalLocationPointer) throws IOException;
}
