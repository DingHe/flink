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

package org.apache.flink.connector.file.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link FileSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link org.apache.flink.connector.file.sink.writer.FileWriter FileWriter}
 * and commit them, or put them in "finished" state and ready to be consumed by downstream
 * applications or systems.
 */
// FileCommitter 是 Flink 文件 Sink (FileSink) 专用的 提交器 (Committer) 实现。它实现了两阶段提交（2PC）协议的第二阶段：最终提交，是保障文件写入**精确一次（Exactly-Once）**语义的关键组件。
// 文件提交： 接收来自 FileSinkWriter 的 FileSinkCommittable 集合，并将处于 "pending"（待定） 状态的文件正式提交，即将其从临时目录移动或重命名到最终可见的目标目录。
// 文件清理： 负责清理那些因任务失败或恢复而遗留下来的未完成（in-progress）的临时文件，以及旧的已合并/压缩的文件，确保目标文件系统的状态是干净和一致的。
// 幂等性处理： 由于 Committer 在 Job 恢复时可能会被要求重新提交已经成功的 Committable，FileCommitter 的底层 BucketWriter 机制确保了提交操作是幂等的，即使重复执行也不会导致数据重复。
// 它是一个文件系统操作协调者，将 Flink 检查点中的文件元数据转化为对文件系统的最终、持久化和容错的操作。
@Internal
public class FileCommitter implements Committer<FileSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileCommitter.class);
    // 存储桶写入器。
    // 核心文件操作工具类。
    // 它封装了与文件系统交互的具体逻辑（如文件重命名、清理文件）。
    // FileCommitter 所有的文件操作都通过它来完成。
    private final BucketWriter<?, ?> bucketWriter;

    public FileCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }
    // 执行提交和清理操作。
    // 实现了 2PC 的最终提交阶段。
    @Override
    public void commit(Collection<CommitRequest<FileSinkCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<FileSinkCommittable> request : requests) {
            FileSinkCommittable committable = request.getCommittable();
            if (committable.hasPendingFile()) {
                // 处理待提交文件： 如果 committable.hasPendingFile() 为真
                // We should always use commitAfterRecovery which contains additional checks.
                bucketWriter.recoverPendingFile(committable.getPendingFile()).commitAfterRecovery();
            }

            if (committable.hasInProgressFileToCleanup()) {
                bucketWriter.cleanupInProgressFileRecoverable(
                        committable.getInProgressFileToCleanup());
            }

            if (committable.hasCompactedFileToCleanup()) {
                Path committedFileToCleanup = committable.getCompactedFileToCleanup();
                try {
                    committedFileToCleanup.getFileSystem().delete(committedFileToCleanup, false);
                } catch (Exception e) {
                    // Try best to cleanup compacting files, skip if failed.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Failed to cleanup a compacted file, the file will be remained and should not be visible: {}",
                                committedFileToCleanup,
                                e);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
