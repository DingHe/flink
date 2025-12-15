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

package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.LogicalFile;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsSegmentDataInputStream;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link FileStateHandle} for state that was written to a file segment. A {@link
 * SegmentFileStateHandle} represents a {@link LogicalFile}, which has already been written to a
 * segment in a physical file.
 */
// SegmentFileStateHandle 是 Flink 状态后端中用于支持文件合并 (File Merging) 功能的一个关键状态句柄。
// 它的主要作用是引用存储在一个物理文件中的**一个连续片段（Segment）**所代表的状态数据。
// 在 Flink 检查点中，为了优化远程文件系统（如 S3、HDFS）上的 I/O 性能，特别是当产生大量小状态文件时，Flink 会将多个小的状态数据块（逻辑文件，LogicalFile）写入到同一个大的物理文件中。这被称为文件合并。
// 片段引用： 它不引用整个文件，而是引用物理文件中的一个特定字节范围（从 startPos 开始，长度为 stateSize）。
// 精确读取： 它实现了 openInputStream() 方法，通过创建一个特殊的 FsSegmentDataInputStream，确保在恢复状态时，Task 只读取其所需状态片段的数据，避免读取整个合并文件。
// 逻辑文件关联： 它通过 LogicalFile.LogicalFileId 保持对逻辑文件的引用，这有助于在 TaskManager 端进行状态恢复和缓存管理。
// 生命周期脱钩： 它的 discardState() 方法为空，表明该句柄不对底层物理文件负责。物理文件的生命周期由管理整个目录或合并文件的上层句柄（如 DirectoryStreamStateHandle 或专门的合并文件句柄）负责。



public class SegmentFileStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;

    /** The path to the file in the filesystem, fully describing the file system. */
    // 物理文件路径。
    // 存储了包含该状态片段的远程文件系统的完整路径。
    private final Path filePath;

    /** The size of the state in the file. */
    // 片段大小。
    // 该状态片段在物理文件中所占的字节数。
    protected final long stateSize;

    /** The starting position of the segment in the file. */
    // 起始位置。
    // 该状态片段在物理文件中的起始字节偏移量。
    private final long startPos;

    /** The scope of the state. */
    // 状态范围。 标记该状态片段是独占的 (EXCLUSIVE) 还是共享的 (SHARED)，影响其生命周期和清理策略。
    private final CheckpointedStateScope scope;

    /** The id for corresponding logical file. Used to retrieve LogicalFile in TM. */
    // 逻辑文件 ID。
    // 该片段对应的逻辑状态块的唯一标识符。
    // 用于 TaskManager 在恢复时检索和管理状态块。
    private final LogicalFile.LogicalFileId logicalFileId;

    /**
     * Creates a new segment file state for the given file path.
     *
     * @param filePath The path to the file that stores the state.
     * @param startPos Start position of the segment in the physical file.
     * @param stateSize Size of the segment.
     * @param scope The state's scope, whether it is exclusive or shared.
     * @param fileId The corresponding logical file id.
     */
    public SegmentFileStateHandle(
            Path filePath,
            long startPos,
            long stateSize,
            CheckpointedStateScope scope,
            LogicalFile.LogicalFileId fileId) {
        this.filePath = filePath;
        this.stateSize = stateSize;
        this.startPos = startPos;
        this.scope = scope;
        this.logicalFileId = fileId;
    }

    /**
     * This method should be empty, so that JM is not in charge of the lifecycle of files in a
     * file-merging checkpoint.
     */
    @Override
    public void discardState() {}

    /**
     * Gets the path where this handle's state is stored.
     *
     * @return The path where this handle's state is stored.
     */
    public Path getFilePath() {
        return filePath;
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        FSDataInputStream inputStream = getFileSystem().open(filePath);
        return new FsSegmentDataInputStream(inputStream, startPos, stateSize);
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return new PhysicalStateHandleID(logicalFileId.getKeyString());
    }

    public long getStartPos() {
        return startPos;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        collector.add(StateObjectLocation.REMOTE, getStateSize());
    }

    public CheckpointedStateScope getScope() {
        return scope;
    }

    public LogicalFile.LogicalFileId getLogicalFileId() {
        return logicalFileId;
    }

    /**
     * Gets the file system that stores the file state.
     *
     * @return The file system that stores the file state.
     * @throws IOException Thrown if the file system cannot be accessed.
     */
    private FileSystem getFileSystem() throws IOException {
        return FileSystem.get(filePath.toUri());
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof SegmentFileStateHandle)) {
            return false;
        }

        SegmentFileStateHandle that = (SegmentFileStateHandle) o;

        return logicalFileId.equals(that.logicalFileId)
                && filePath.equals(that.filePath)
                && startPos == that.startPos
                && stateSize == that.stateSize
                && scope.equals(that.scope);
    }

    @Override
    public int hashCode() {
        int result = logicalFileId.hashCode();
        result = 31 * result + Objects.hashCode(getFilePath());
        result = 31 * result + Objects.hashCode(startPos);
        result = 31 * result + Objects.hashCode(stateSize);
        result = 31 * result + Objects.hashCode(scope);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "Segment File State: %s [Starting Position: %d, %d bytes]",
                getFilePath(), startPos, stateSize);
    }
}
