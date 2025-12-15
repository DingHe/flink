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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.NotDuplicatingCheckpointStateToolset;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of durable checkpoint storage to file systems. */
// FsCheckpointStorageAccess 是 Flink 文件系统状态后端（FsStateBackend）在 JobManager（作为 CheckpointStorageCoordinatorView）
// 和 TaskManager（作为 CheckpointStorageWorkerView）两端实现检查点存储访问的具体类。
// 管理文件系统连接和目录结构。 它实例化了实际的 FileSystem 对象，并根据 Flink 的规范（继承自 AbstractFsCheckpointStorageAccess）计算出作业（Job）特有的检查点、共享状态和任务自有状态的目录路径。
// 实现 I/O 抽象。 它实现了所有抽象的存储访问方法，特别是提供了基于文件系统的检查点流工厂 (CheckpointStreamFactory)，允许任务管理器将状态数据写入配置的存储路径中。
// 支持高可用。 它明确声明支持高可用存储，因为底层依赖于 HDFS、S3 等分布式文件系统。
// Flink 文件系统检查点存储逻辑的最终执行者，负责将抽象的存储操作（如初始化位置、解析引用、创建输出流）具体落实到文件系统 I/O 操作上。

public class FsCheckpointStorageAccess extends AbstractFsCheckpointStorageAccess {
    // 文件系统实例。
    // 实际用于执行所有文件操作（如 mkdirs、创建流）的文件系统对象（如 HDFS、本地文件系统）。
    protected final FileSystem fileSystem;
    // Job 检查点根目录。
    // 当前 Job 存储其所有检查点和 Savepoint 的顶层目录路径。
    protected final Path checkpointsDirectory;
    // 共享状态目录。
    // 位于 checkpointsDirectory 下，用于存储可在多个检查点之间共享的状态数据。
    protected final Path sharedStateDirectory;
    // 任务自有状态目录。
    // 位于 checkpointsDirectory 下，用于存储生命周期由任务自身管理的状态（如 RocksDB 的 WAL）。
    protected final Path taskOwnedStateDirectory;
    // 文件大小阈值（字节）。
    // 当任务写入状态数据时，如果单个状态片段小于此阈值，它可能会被嵌入到检查点元数据中，而不是写入一个单独的文件。
    protected final int fileSizeThreshold;
    // 写入缓冲区大小（字节）。
    // 创建文件输出流 (CheckpointStateOutputStream) 时使用的缓冲区大小。
    protected final int writeBufferSize;
    // 基础位置初始化标志。
    // 一个内部标志，用于跟踪 Job 检查点所需的基础目录（共享状态目录和任务自有状态目录）是否已经创建。
    private boolean baseLocationsInitialized = false;

    public FsCheckpointStorageAccess(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        this(
                checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                true,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
    }

    public FsCheckpointStorageAccess(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            boolean createCheckpointSubDirs,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        this(
                checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                createCheckpointSubDirs,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
    }

    public FsCheckpointStorageAccess(
            FileSystem fs,
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            boolean createCheckpointSubDirs,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        super(jobId, defaultSavepointDirectory);

        checkArgument(fileSizeThreshold >= 0);
        checkArgument(writeBufferSize >= 0);

        this.fileSystem = checkNotNull(fs);
        this.checkpointsDirectory =
                createCheckpointSubDirs
                        ? getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId)
                        : checkpointBaseDirectory;
        this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
        this.taskOwnedStateDirectory =
                new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
        this.fileSizeThreshold = fileSizeThreshold;
        this.writeBufferSize = writeBufferSize;
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    Path getCheckpointsDirectory() {
        return checkpointsDirectory;
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorage implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsHighlyAvailableStorage() {
        return true;
    }
    // 初始化基础目录。
    // 使用 fileSystem.mkdirs() 创建共享状态目录 (sharedStateDirectory) 和任务自有状态目录 (taskOwnedStateDirectory)。
    // 这是在 Job 首次启动检查点流程时执行的。
    @Override
    public void initializeBaseLocationsForCheckpoint() throws IOException {
        if (!fileSystem.mkdirs(sharedStateDirectory)) {
            throw new IOException(
                    "Failed to create directory for shared state: " + sharedStateDirectory);
        }
        if (!fileSystem.mkdirs(taskOwnedStateDirectory)) {
            throw new IOException(
                    "Failed to create directory for task owned state: " + taskOwnedStateDirectory);
        }
    }
    // 初始化检查点位置。
    // 为给定的 checkpointId 创建专有检查点目录（例如 chk-N），并返回一个 FsCheckpointStorageLocation 实例。
    // 这个实例包含了所有必要的目录路径、文件系统句柄和配置参数，供 TaskManager 写入状态。
    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
            throws IOException {
        checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %s.", checkpointId);

        // prepare all the paths needed for the checkpoints
        final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

        // create the checkpoint exclusive directory
        fileSystem.mkdirs(checkpointDir);

        return new FsCheckpointStorageLocation(
                fileSystem,
                checkpointDir,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                CheckpointStorageLocationReference.getDefault(),
                fileSizeThreshold,
                writeBufferSize);
    }
    // 解析存储位置到流工厂。
    // TaskManager 调用此方法来获取一个流工厂 (CheckpointStreamFactory) 以开始写入状态数据。
    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException {
        // 如果引用是默认引用：
        // 基于 checkpointId 和 Job 的根目录，
        // 构造标准的 FsCheckpointStorageLocation（即 JobManager 在 initializeLocationForCheckpoint 中使用的目录）。
        if (reference.isDefaultReference()) {
            // default reference, construct the default location for that particular checkpoint
            final Path checkpointDir =
                    createCheckpointDirectory(checkpointsDirectory, checkpointId);

            return new FsCheckpointStorageLocation(
                    fileSystem,
                    checkpointDir,
                    sharedStateDirectory,
                    taskOwnedStateDirectory,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize);
        } else {
            // location encoded in the reference
            final Path path = decodePathFromReference(reference);

            return new FsCheckpointStorageLocation(
                    path.getFileSystem(),
                    path,
                    path,
                    path,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize);
        }
    }
    // 创建任务自有状态输出流。
    // 返回一个 FsCheckpointStateOutputStream 实例。这个流专门用于将任务自有状态（如 RocksDB 的日志）写入到 taskOwnedStateDirectory 中。
    @Override
    public CheckpointStateOutputStream createTaskOwnedStateStream() {
        // as the comment of CheckpointStorageWorkerView#createTaskOwnedStateStream said we may
        // change into shared state,
        // so we use CheckpointedStateScope.SHARED here.
        return new FsCheckpointStateOutputStream(
                taskOwnedStateDirectory, fileSystem, writeBufferSize, fileSizeThreshold);
    }
    // 创建任务自有状态工具集。
    // 返回一个工具集，用于执行对任务自有状态的额外操作，如清理。
    // 如果底层 fileSystem 支持路径复制 (PathsCopyingFileSystem)，则返回 FsCheckpointStateToolset；否则返回 NotDuplicatingCheckpointStateToolset（表示无法优化路径复制）。
    @Override
    public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
        if (fileSystem instanceof PathsCopyingFileSystem) {
            return new FsCheckpointStateToolset(
                    taskOwnedStateDirectory, (PathsCopyingFileSystem) fileSystem);
        } else {
            return new NotDuplicatingCheckpointStateToolset();
        }
    }
    // 创建 Savepoint 位置（抽象方法的实现）。
    // 继承自父类的抽象方法。
    // 它将确定的 Savepoint 目录路径编码成一个引用 (encodePathAsReference)，并返回一个配置了 Savepoint 路径的 FsCheckpointStorageLocation 实例。
    @Override
    protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
        final CheckpointStorageLocationReference reference = encodePathAsReference(location);
        return new FsCheckpointStorageLocation(
                fs, location, location, location, reference, fileSizeThreshold, writeBufferSize);
    }
    // 文件合并存储转换。 这是一个用于支持文件合并（File Merging）优化的方法。
    // 如果启用文件合并，它将基于当前配置参数和提供的管理器，创建一个 FsMergingCheckpointStorageAccess 实例并返回，该实例具有额外的合并逻辑。
    public FsMergingCheckpointStorageAccess toFileMergingStorage(
            FileMergingSnapshotManager mergingSnapshotManager, Environment environment)
            throws IOException {
        return new FsMergingCheckpointStorageAccess(
                checkpointsDirectory,
                getDefaultSavepointDirectory(),
                environment.getJobID(),
                fileSizeThreshold,
                writeBufferSize,
                mergingSnapshotManager,
                environment);
    }
}
