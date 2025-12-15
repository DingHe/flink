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

package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nonnull;

import java.util.Optional;

/**
 * This state handle represents a directory, usually used to be registered to {@link
 * org.apache.flink.runtime.state.SharedStateRegistry} to track the life cycle of the directory.
 */
// DirectoryStreamStateHandle 是 StreamStateHandle 接口的一个特殊实现，它代表一个 HDFS 或 S3 等远程文件系统上的目录，而不是单个文件或数据流。
// 目录引用与生命周期管理： 它的主要目的不是为了读取流数据，而是作为一个 Job 级别的目录引用，通常用于注册到 SharedStateRegistry (共享状态注册表) 中。
// 共享状态追踪： 通过注册，CheckpointCoordinator 可以追踪这个目录的生命周期和引用计数。当该目录被所有使用它的检查点废弃后，DirectoryStreamStateHandle 负责执行目录的删除操作。
// 应用场景（例如文件合并）： 它在 Flink 尝试进行文件合并优化时（如 RocksDB 增量状态管理或文件合并状态后端）尤其重要。它作为一个顶级句柄，用于管理那些包含许多小型状态文件（如 SST 文件或块文件）的整个目录。

public class DirectoryStreamStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;
    // 远程目录路径。
    // 存储该句柄引用的远程文件系统（如 HDFS/S3）上的目录路径。
    // 这是该句柄的核心信息。
    private final Path directory;

    public DirectoryStreamStateHandle(@Nonnull Path directory) {
        this.directory = directory;
    }

    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return new PhysicalStateHandleID(directory.toString());
    }

    public SharedStateRegistryKey createStateRegistryKey() {
        return new SharedStateRegistryKey(directory.toString());
    }

    @Override
    public int hashCode() {
        return directory.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DirectoryStreamStateHandle that = (DirectoryStreamStateHandle) o;

        return directory.equals(that.directory);
    }

    @Override
    public String toString() {
        return "DirectoryStreamStateHandle{" + "directory=" + getDirectory() + '}';
    }

    public Path getDirectory() {
        return directory;
    }

    @Override
    public void discardState() throws Exception {
        FileSystem fs = directory.getFileSystem();
        fs.delete(directory, true);
    }

    /**
     * This handle usually used to track the life cycle of the directory, therefore a fake size is
     * provided.
     */
    @Override
    public long getStateSize() {
        return 0;
    }

    public static DirectoryStreamStateHandle of(@Nonnull Path directory) {
        return new DirectoryStreamStateHandle(directory);
    }
}
