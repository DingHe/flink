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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.util.FileUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of durable checkpoint storage to file systems.
 *
 * <h1>Checkpoint Layout</h1>
 *
 * <p>The checkpoint storage is configured with a base directory and persists the checkpoint data of
 * specific checkpoints in specific subdirectories. For example, if the base directory was set to
 * {@code hdfs://namenode:port/flink-checkpoints/}, the state backend will create a subdirectory
 * with the job's ID that will contain the actual checkpoints: ({@code
 * hdfs://namenode:port/flink-checkpoints/1b080b6e710aabbef8993ab18c6de98b})
 *
 * <p>Each checkpoint individually will store all its files in a subdirectory that includes the
 * checkpoint number, such as {@code
 * hdfs://namenode:port/flink-checkpoints/1b080b6e710aabbef8993ab18c6de98b/chk-17/}.
 *
 * <h1>Savepoint Layout</h1>
 *
 * <p>A savepoint that is set to be stored in path {@code hdfs://namenode:port/flink-savepoints/},
 * will create a subdirectory {@code savepoint-jobId(0, 6)-randomDigits} in which it stores all
 * savepoint data. The random digits are added as "entropy" to avoid directory collisions.
 *
 * <h1>Metadata File</h1>
 *
 * <p>A completed checkpoint writes its metadata into a file '{@value
 * AbstractFsCheckpointStorageAccess#METADATA_FILE_NAME}'.
 */
// Flink 基于文件系统 (File System, Fs) 的检查点存储访问的抽象基类。
// 核心职责： 它为所有基于文件系统的存储后端（如 FsCheckpointStorageAccess，通常用于 FsStateBackend 或 ChangelogStateBackend 配合持久化存储时）提供了一套标准的目录布局、Savepoint 路径生成、以及检查点指针解析的通用实现。
// 它主要实现了 CheckpointStorageAccess 接口中与路径管理、解析和 Savepoint 初始化相关的逻辑，而将底层的 I/O 流操作留给子类实现。
// 定义了 Flink 检查点和 Savepoint 在文件系统上的标准目录结构：
// Job 根目录: base_path/flink-checkpoints/job_id/
// 单个 Checkpoint 目录: .../job_id/chk-N/ (其中 N 是检查点 ID)
// 单个 Savepoint 目录: savepoint_base_path/savepoint-jobId(0, 6)-randomDigits/
// 元数据文件: 在每个检查点/Savepoint 目录中，元数据文件固定命名为 _metadata。


public abstract class AbstractFsCheckpointStorageAccess implements CheckpointStorageAccess {

    // ------------------------------------------------------------------------
    //  Constants
    // ------------------------------------------------------------------------

    /** The prefix of the directory containing the data exclusive to a checkpoint. */
    // 检查点目录前缀。 用于生成单个检查点目录，例如 chk-17。
    public static final String CHECKPOINT_DIR_PREFIX = "chk-";

    /** The name of the directory for shared checkpoint state. */
    // 共享状态目录名。
    // 检查点中共享状态数据存储的目录名。
    public static final String CHECKPOINT_SHARED_STATE_DIR = "shared";

    /**
     * The name of the directory for state not owned/released by the master, but by the
     * TaskManagers.
     */
    // 任务自有状态目录名。
    // 由 TaskManager 拥有生命周期（非 JobManager 释放）的状态存储目录名。
    public static final String CHECKPOINT_TASK_OWNED_STATE_DIR = "taskowned";

    /** The name of the metadata files in checkpoints / savepoints. */
    // 元数据文件名。
    // 存储检查点/Savepoint 元数据的固定文件名。
    public static final String METADATA_FILE_NAME = "_metadata";

    /** The magic number that is put in front of any reference. */
    // 引用魔术数字。
    // 用于编码检查点存储位置引用时，作为路径字节流的前缀，以快速验证引用的有效性。
    private static final byte[] REFERENCE_MAGIC_NUMBER = new byte[] {0x05, 0x5F, 0x3F, 0x18};

    // ------------------------------------------------------------------------
    //  Fields and properties
    // ------------------------------------------------------------------------

    /** The jobId, written into the generated savepoint directories. */
    // 作业 ID。
    // 当前 Job 的唯一标识符。它用于创建 Job 特定的检查点目录和生成 Savepoint 目录前缀。
    private final JobID jobId;

    /** The default location for savepoints. Null, if none is configured. */
    // 默认 Savepoint 目录。
    // 可选配置，如果用户未指定 Savepoint 目标路径，则使用此路径作为默认位置。如果未配置，则为 null。
    @Nullable private final Path defaultSavepointDirectory;

    /**
     * Creates a new checkpoint storage.
     *
     * @param jobId The ID of the job that writes the checkpoints.
     * @param defaultSavepointDirectory The default location for savepoints, or null, if none is
     *     set.
     */
    protected AbstractFsCheckpointStorageAccess(
            JobID jobId, @Nullable Path defaultSavepointDirectory) {

        this.jobId = checkNotNull(jobId);
        this.defaultSavepointDirectory = defaultSavepointDirectory;
    }

    /**
     * Gets the default directory for savepoints. Returns null, if no default savepoint directory is
     * configured.
     */
    // 获取默认 Savepoint 目录。
    // 返回配置的默认 Savepoint 目录路径，如果未配置则返回 null。
    @Nullable
    public Path getDefaultSavepointDirectory() {
        return defaultSavepointDirectory;
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorage implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean hasDefaultSavepointLocation() {
        return defaultSavepointDirectory != null;
    }

    // 解析检查点指针。
    // 调用静态方法 resolveCheckpointPointer 来将外部指针（路径字符串）解析为已完成检查点存储位置 (CompletedCheckpointStorageLocation) 的句柄。
    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String checkpointPointer)
            throws IOException {
        return resolveCheckpointPointer(checkpointPointer);
    }

    /**
     * Creates a file system based storage location for a savepoint.
     *
     * <p>This methods implements the logic that decides which location to use (given optional
     * parameters for a configured location and a location passed for this specific savepoint) and
     * how to name and initialize the savepoint directory.
     *
     * @param externalLocationPointer The target location pointer for the savepoint. Must be a valid
     *     URI. Null, if not supplied.
     * @param checkpointId The checkpoint ID of the savepoint.
     * @return The checkpoint storage location for the savepoint.
     * @throws IOException Thrown if the target directory could not be created.
     */
    // 初始化 Savepoint 存储位置。
    // 负责决定 Savepoint 的最终写入路径并创建目录。
    @Override
    public CheckpointStorageLocation initializeLocationForSavepoint(
            @SuppressWarnings("unused") long checkpointId, @Nullable String externalLocationPointer)
            throws IOException {

        // determine where to write the savepoint to
        // 1. 确定基础路径： 优先使用传入的 externalLocationPointer；
        // 如果没有，则使用 defaultSavepointDirectory；如果两者都没有，则抛出异常。
        final Path savepointBasePath;
        if (externalLocationPointer != null) {
            savepointBasePath = new Path(externalLocationPointer);
        } else if (defaultSavepointDirectory != null) {
            savepointBasePath = defaultSavepointDirectory;
        } else {
            throw new IllegalArgumentException(
                    "No savepoint location given and no default location configured.");
        }

        // generate the savepoint directory
        // 2. 生成目录名： 目录名格式为 savepoint-jobId(前6位)-randomDigits，使用随机后缀避免冲突。
        final FileSystem fs = savepointBasePath.getFileSystem();
        final String prefix = "savepoint-" + jobId.toString().substring(0, 6) + '-';

        Exception latestException = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            final Path path = new Path(savepointBasePath, FileUtils.getRandomFilename(prefix));

            try {
                // 创建目录： 尝试创建目录（最多 10 次），成功后将路径合格化 (makeQualified)，
                // 最后调用抽象方法 createSavepointLocation 创建最终的存储位置对象。
                if (fs.mkdirs(path)) {
                    // we make the path qualified, to make it independent of default schemes and
                    // authorities
                    final Path qp = path.makeQualified(fs);

                    return createSavepointLocation(fs, qp);
                }
            } catch (Exception e) {
                latestException = e;
            }
        }

        throw new IOException(
                "Failed to create savepoint directory at " + savepointBasePath, latestException);
    }
    // 创建 Savepoint 位置对象。
    // 这是一个抽象方法，由子类实现。
    protected abstract CheckpointStorageLocation createSavepointLocation(
            FileSystem fs, Path location) throws IOException;

    // ------------------------------------------------------------------------
    //  Creating and resolving paths
    // ------------------------------------------------------------------------

    /**
     * Builds directory into which a specific job checkpoints, meaning the directory inside which it
     * creates the checkpoint-specific subdirectories.
     *
     * <p>This method only succeeds if a base checkpoint directory has been set; otherwise the
     * method fails with an exception.
     *
     * @param jobId The ID of the job
     * @return The job's checkpoint directory, re
     * @throws UnsupportedOperationException Thrown, if no base checkpoint directory has been set.
     */
    // 获取 Job 检查点根目录。
    // 拼接 Job 的检查点基础路径和 Job ID，生成 Job 特有的检查点根目录路径。
    protected static Path getCheckpointDirectoryForJob(Path baseCheckpointPath, JobID jobId) {
        return new Path(baseCheckpointPath, jobId.toString());
    }

    /**
     * Creates the directory path for the data exclusive to a specific checkpoint.
     *
     * @param baseDirectory The base directory into which the job checkpoints.
     * @param checkpointId The ID (logical timestamp) of the checkpoint.
     */

    // 创建单个检查点目录路径。
    // 拼接 Job 检查点根目录和检查点 ID，生成单个检查点的数据存储目录路径，使用 CHECKPOINT_DIR_PREFIX。
    protected static Path createCheckpointDirectory(Path baseDirectory, long checkpointId) {
        return new Path(baseDirectory, CHECKPOINT_DIR_PREFIX + checkpointId);
    }

    /**
     * Takes the given string (representing a pointer to a checkpoint) and resolves it to a file
     * status for the checkpoint's metadata file.
     *
     * @param checkpointPointer The pointer to resolve.
     * @return A state handle to checkpoint/savepoint's metadata.
     * @throws IOException Thrown, if the pointer cannot be resolved, the file system not accessed,
     *     or the pointer points to a location that does not seem to be a checkpoint/savepoint.
     */
    // 解析检查点指针。
    // 将外部字符串指针解析为文件系统中的路径，并验证其有效性。
    @Internal
    public static FsCompletedCheckpointStorageLocation resolveCheckpointPointer(
            String checkpointPointer) throws IOException {
        checkNotNull(checkpointPointer, "checkpointPointer");
        checkArgument(!checkpointPointer.isEmpty(), "empty checkpoint pointer");

        // check if the pointer is in fact a valid file path
        final Path path;
        try {
            path = new Path(checkpointPointer);
        } catch (Exception e) {
            throw new IOException(
                    "Checkpoint/savepoint path '"
                            + checkpointPointer
                            + "' is not a valid file URI. "
                            + "Either the pointer path is invalid, or the checkpoint was created by a different state backend.");
        }

        // check if the file system can be accessed
        final FileSystem fs;
        try {
            fs = path.getFileSystem();
        } catch (IOException e) {
            throw new IOException(
                    "Cannot access file system for checkpoint/savepoint path '"
                            + checkpointPointer
                            + "'.",
                    e);
        }

        final FileStatus status;
        try {
            status = fs.getFileStatus(path);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(
                    "Cannot find checkpoint or savepoint "
                            + "file/directory '"
                            + checkpointPointer
                            + "' on file system '"
                            + fs.getUri().getScheme()
                            + "'.");
        }

        // if we are here, the file / directory exists
        final Path checkpointDir;
        final FileStatus metadataFileStatus;

        // If this is a directory, we need to find the meta data file
        if (status.isDir()) {
            checkpointDir = status.getPath();
            final Path metadataFilePath = new Path(path, METADATA_FILE_NAME);
            try {
                metadataFileStatus = fs.getFileStatus(metadataFilePath);
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException(
                        "Cannot find meta data file '"
                                + METADATA_FILE_NAME
                                + "' in directory '"
                                + path
                                + "'. Please try to load the checkpoint/savepoint "
                                + "directly from the metadata file instead of the directory.");
            }
        } else {
            // this points to a file and we either do no name validation, or
            // the name is actually correct, so we can return the path
            metadataFileStatus = status;
            checkpointDir = status.getPath().getParent();
        }

        final FileStateHandle metaDataFileHandle =
                new FileStateHandle(metadataFileStatus.getPath(), metadataFileStatus.getLen());

        final String pointer = checkpointDir.makeQualified(fs).toString();

        return new FsCompletedCheckpointStorageLocation(
                fs, checkpointDir, metaDataFileHandle, pointer);
    }

    // ------------------------------------------------------------------------
    //  Encoding / Decoding of References
    // ------------------------------------------------------------------------

    /**
     * Encodes the given path as a reference in bytes. The path is encoded as a UTF-8 string and
     * prepended as a magic number.
     *
     * @param path The path to encode.
     * @return The location reference.
     */
    // 路径编码为引用。
    // 将文件路径 (Path) 编码为字节数组形式的 CheckpointStorageLocationReference。
    // 编码时会以 REFERENCE_MAGIC_NUMBER 为前缀，路径本身以 UTF-8 编码。
    public static CheckpointStorageLocationReference encodePathAsReference(Path path) {
        byte[] refBytes = path.toString().getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[REFERENCE_MAGIC_NUMBER.length + refBytes.length];

        System.arraycopy(REFERENCE_MAGIC_NUMBER, 0, bytes, 0, REFERENCE_MAGIC_NUMBER.length);
        System.arraycopy(refBytes, 0, bytes, REFERENCE_MAGIC_NUMBER.length, refBytes.length);

        return new CheckpointStorageLocationReference(bytes);
    }

    /**
     * Decodes the given reference into a path. This method validates that the reference bytes start
     * with the correct magic number (as written by {@link #encodePathAsReference(Path)}) and
     * converts the remaining bytes back to a proper path.
     *
     * @param reference The bytes representing the reference.
     * @return The path decoded from the reference.
     * @throws IllegalArgumentException Thrown, if the bytes do not represent a proper reference.
     */
    // 引用解码为路径。
    // 将字节数组形式的引用解码回文件路径 (Path)。
    // 它会先检查引用是否以正确的 REFERENCE_MAGIC_NUMBER 开头以验证格式，然后将剩余的字节解码为 UTF-8 字符串路径。
    public static Path decodePathFromReference(CheckpointStorageLocationReference reference) {
        if (reference.isDefaultReference()) {
            throw new IllegalArgumentException("Cannot decode default reference");
        }

        final byte[] bytes = reference.getReferenceBytes();
        final int headerLen = REFERENCE_MAGIC_NUMBER.length;

        if (bytes.length > headerLen) {
            // compare magic number
            for (int i = 0; i < headerLen; i++) {
                if (bytes[i] != REFERENCE_MAGIC_NUMBER[i]) {
                    throw new IllegalArgumentException(
                            "Reference starts with the wrong magic number");
                }
            }

            // covert to string and path
            try {
                return new Path(
                        new String(
                                bytes,
                                headerLen,
                                bytes.length - headerLen,
                                StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new IllegalArgumentException("Reference cannot be decoded to a path", e);
            }
        } else {
            throw new IllegalArgumentException("Reference too short.");
        }
    }
}
