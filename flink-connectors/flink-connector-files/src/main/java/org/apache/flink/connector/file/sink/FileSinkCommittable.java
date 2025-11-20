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

package org.apache.flink.connector.file.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper class for both type of committables in {@link FileSink}. One committable might be either
 * one pending files to commit, or one in-progress file to cleanup.
 */
// FileSinkCommittable 是 Flink 文件 Sink (FileSink) 专用的 Committable 类型。它封装了在两阶段提交（2PC）过程中，文件写入器 (SinkWriter) 需要传递给提交器 (Committer) 的元数据。
// 统一 Committable 类型： 文件 Sink 需要处理多种与文件相关的操作，包括文件提交、清理未完成文件和清理压缩文件。FileSinkCommittable 作为这些不同操作的通用封装器，确保 Committer 只需处理一种类型即可完成所有文件操作。
// 文件提交元数据： 它包含了一个待提交文件的关键信息，这些信息足以让 Committer 在检查点成功后，将文件从临时位置移动到最终可见位置。
// 失败清理元数据： 它还包含了在恢复或失败清理时，需要删除未完成或临时文件的元数据，以保证文件系统状态的干净和正确性。
// 它是 Flink FileSink 用于实现精确一次语义的关键数据结构，将需要被 Committer 最终确认（提交或清理）的文件操作元数据打包在一起。
@Internal
public class FileSinkCommittable implements Serializable {
    // 存储桶 ID。
    // 标识该 Committable 所属的文件存储桶的 ID。文件 Sink 通常根据时间或键将数据写入不同的目录/桶中。
    private final String bucketId;
    // 待提交文件元数据。
    // 包含一个已完成写入但尚未提交的文件所需的所有恢复信息。
    // Committer 将使用此信息将文件最终提交（重命名到最终路径）。
    @Nullable private final InProgressFileWriter.PendingFileRecoverable pendingFile;
    // 待清理的进行中文件元数据。
    // 包含一个未完成（in-progress）文件的恢复信息。
    // 该 Committable 用于在作业恢复或失败时，指示 Committer 清理这些临时文件。
    @Nullable private final InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup;
    // 待清理的压缩文件路径。
    // 仅在 Sink 启用了文件压缩/合并功能时使用。它包含需要清理的旧压缩文件的路径。
    @Nullable private final Path compactedFileToCleanup;

    public FileSinkCommittable(
            String bucketId, InProgressFileWriter.PendingFileRecoverable pendingFile) {
        this.bucketId = bucketId;
        this.pendingFile = checkNotNull(pendingFile);
        this.inProgressFileToCleanup = null;
        this.compactedFileToCleanup = null;
    }

    public FileSinkCommittable(
            String bucketId,
            InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.bucketId = bucketId;
        this.pendingFile = null;
        this.inProgressFileToCleanup = checkNotNull(inProgressFileToCleanup);
        this.compactedFileToCleanup = null;
    }

    public FileSinkCommittable(String bucketId, Path compactedFileToCleanup) {
        this.bucketId = bucketId;
        this.pendingFile = null;
        this.inProgressFileToCleanup = null;
        this.compactedFileToCleanup = checkNotNull(compactedFileToCleanup);
    }

    FileSinkCommittable(
            String bucketId,
            @Nullable InProgressFileWriter.PendingFileRecoverable pendingFile,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup,
            @Nullable Path compactedFileToCleanup) {
        this.bucketId = bucketId;
        this.pendingFile = pendingFile;
        this.inProgressFileToCleanup = inProgressFileToCleanup;
        this.compactedFileToCleanup = compactedFileToCleanup;
    }

    public String getBucketId() {
        return bucketId;
    }

    public boolean hasPendingFile() {
        return pendingFile != null;
    }

    @Nullable
    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

    public boolean hasInProgressFileToCleanup() {
        return inProgressFileToCleanup != null;
    }

    @Nullable
    public InProgressFileWriter.InProgressFileRecoverable getInProgressFileToCleanup() {
        return inProgressFileToCleanup;
    }

    public boolean hasCompactedFileToCleanup() {
        return compactedFileToCleanup != null;
    }

    @Nullable
    public Path getCompactedFileToCleanup() {
        return compactedFileToCleanup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSinkCommittable that = (FileSinkCommittable) o;
        return Objects.equals(bucketId, that.bucketId)
                && Objects.equals(pendingFile, that.pendingFile)
                && Objects.equals(inProgressFileToCleanup, that.inProgressFileToCleanup)
                && Objects.equals(compactedFileToCleanup, that.compactedFileToCleanup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, pendingFile, inProgressFileToCleanup, compactedFileToCleanup);
    }

    @Override
    public String toString() {
        return "FileSinkCommittable{"
                + "bucketId='"
                + bucketId
                + ", pendingFile="
                + pendingFile
                + ", inProgressFileToCleanup="
                + inProgressFileToCleanup
                + ", compactedFileToCleanup="
                + compactedFileToCleanup
                + '}';
    }
}
