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

import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This state handle represents a directory. This class is, for example, used to represent the
 * directory of RocksDB's native checkpoint directories for local recovery.
 */
// DirectoryStateHandle 是 Flink 状态句柄 (StateObject) 接口的一个具体实现。它的核心作用是引用和管理一个完整的目录作为状态的一部分。
// 目录封装： 它将一个文件系统中的物理目录（通过其路径字符串 directoryString）封装起来，作为一个状态对象。
// 本地状态恢复： 最典型的应用场景是 RocksDB 状态后端的本地恢复 (Local Recovery)。RocksDB 会将其状态以目录形式存储在 TaskManager 的本地磁盘上。DirectoryStateHandle 就被用来引用这个本地目录，使得 TaskManager 在失败重启后，可以快速从本地磁盘恢复 RocksDB 状态，而无需从远程检查点存储下载数据。
// 生命周期管理： 它实现了 StateObject 接口，因此具备状态对象的基本行为，如报告大小、并在状态被废弃时删除整个目录。
// 可序列化性： 它的路径信息以字符串形式存储，确保该对象可以被序列化（例如，作为检查点元数据的一部分）并在 Flink 集群中传输。

public class DirectoryStateHandle implements StateObject {

    /** Serial version. */
    private static final long serialVersionUID = 1L;

    /** The path that describes the directory, as a string, to be serializable. */
    // 目录路径字符串。
    // 存储实际状态目录的路径，以字符串形式存在，这是该对象可序列化的关键。在对象被序列化和反序列化时，只有这个字符串会被传输。
    private final String directoryString;

    /** (Optional) Size of the directory, used for metrics. Can be 0 if unknown or empty. */
    // 目录大小。
    // 存储该目录及其所有文件占用的总字节数。主要用于 Flink 的检查点指标 (metrics) 和状态大小统计。如果大小未知或目录为空，则为 0。
    private final long directorySize;

    /** Transient path cache, to avoid re-parsing the string. */
    // 瞬态路径对象缓存。
    // 存储 directoryString 对应的 Java Path 对象。它是 transient（瞬态）的，意味着在序列化时不会被保存。
    private transient Path directory;

    public DirectoryStateHandle(@Nonnull Path directory, long directorySize) {
        this.directory = directory;
        this.directoryString = directory.toString();
        this.directorySize = directorySize;
    }

    public static DirectoryStateHandle forPathWithSize(@Nonnull Path directory) {
        long size;
        try {
            size = FileUtils.getDirectoryFilesSize(directory);
        } catch (IOException e) {
            size = 0L;
        }
        return new DirectoryStateHandle(directory, size);
    }

    // 删除目录
    @Override
    public void discardState() throws IOException {
        ensurePath();
        FileUtils.deleteDirectory(directory.toFile());
    }

    @Override
    public long getStateSize() {
        return directorySize;
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        collector.add(StateObjectLocation.LOCAL_DISK, directorySize);
    }

    @Nonnull
    public Path getDirectory() {
        ensurePath();
        return directory;
    }

    private void ensurePath() {
        if (directory == null) {
            directory = Paths.get(directoryString);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DirectoryStateHandle that = (DirectoryStateHandle) o;

        return directoryString.equals(that.directoryString);
    }

    @Override
    public int hashCode() {
        return directoryString.hashCode();
    }

    @Override
    public String toString() {
        return "DirectoryStateHandle{"
                + "directory='"
                + directoryString
                + '\''
                + ", directorySize="
                + directorySize
                + '}';
    }
}
