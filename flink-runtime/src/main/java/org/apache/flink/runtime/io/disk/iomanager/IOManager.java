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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.Enumerator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/** The facade for the provided I/O manager services. */
// IOManager（输入/输出管理器）是 Flink 运行时的一个门面（Facade）类，专门负责管理和执行 Flink 算子在处理大量数据时涉及到的磁盘 I/O 操作。
// 当 TaskManager 中的内存（Managed Memory）不足以容纳中间数据（例如在执行大型排序、哈希连接或 GroupBy 操作时），FLink 需要将部分数据**溢写（Spill）**到本地磁盘。IOManager 就是实现这一机制的核心服务。
// 临时文件管理： 管理用于溢写数据的临时文件目录，并负责在这些目录中创建、销毁和循环使用文件通道（File Channels）。
// 高性能 I/O 抽象： 提供一套抽象接口来创建高性能的**块（Block）**读写器（Reader/Writer），这些读写器直接操作 MemorySegment，通常会利用 NIO 或异步 I/O 来最小化对任务执行的阻塞。
// 异步 I/O 执行： 维护一个专用的线程池（ExecutorService）来执行实际的磁盘读写操作，从而实现异步 I/O，防止 I/O 延迟阻塞 Flink 的计算线程。
// IOManager 是 Flink 的本地磁盘溢写（Spilling）服务，确保 Flink 即使在处理超大数据集时，也能稳定且高效地运行，并将磁盘 I/O 隔离到后台线程。
public abstract class IOManager implements AutoCloseable {
    protected static final Logger LOG = LoggerFactory.getLogger(IOManager.class);
    // 临时文件目录名称的前缀，默认为 "io"，用于在配置的临时目录中创建 Flink I/O 专用的子目录。
    private static final String DIR_NAME_PREFIX = "io";
    // 负责管理 Flink 可以用于溢写数据的本地磁盘目录，并以循环（Round-Robin）的方式在这些目录中创建和分配文件通道 ID (FileIOChannel.ID)
    private final FileChannelManager fileChannelManager;
    // 用于执行所有异步磁盘读写请求。
    protected final ExecutorService executorService;

    // -------------------------------------------------------------------------
    //               Constructors / Destructors
    // -------------------------------------------------------------------------

    /**
     * Constructs a new IOManager.
     *
     * @param tempDirs The basic directories for files underlying anonymous channels.
     */
    protected IOManager(String[] tempDirs, ExecutorService executorService) {
        this.fileChannelManager =
                new FileChannelManagerImpl(Preconditions.checkNotNull(tempDirs), DIR_NAME_PREFIX);
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Created a new {} for spilling of task related data to disk (joins, sorting, ...). Used directories:\n\t{}",
                    FileChannelManager.class.getSimpleName(),
                    Arrays.stream(fileChannelManager.getPaths())
                            .map(File::getAbsolutePath)
                            .collect(Collectors.joining("\n\t")));
        }
        this.executorService = executorService;
    }

    /** Removes all temporary files. */
    @Override
    public void close() throws Exception {
        fileChannelManager.close();
    }

    // ------------------------------------------------------------------------
    //                          Channel Instantiations
    // ------------------------------------------------------------------------

    /**
     * Creates a new {@link ID} in one of the temp directories. Multiple invocations of this method
     * spread the channels evenly across the different directories.
     *
     * @return A channel to a temporary directory.
     */
    public ID createChannel() {
        return fileChannelManager.createChannel();
    }

    /**
     * Creates a new {@link Enumerator}, spreading the channels in a round-robin fashion across the
     * temporary file directories.
     *
     * @return An enumerator for channels.
     */
    // 返回一个用于创建一系列通道 ID 的枚举器，这些通道 ID 同样会循环分配到不同的临时文件目录。
    public Enumerator createChannelEnumerator() {
        return fileChannelManager.createChannelEnumerator();
    }

    /**
     * Deletes the file underlying the given channel. If the channel is still open, this call may
     * fail.
     *
     * @param channel The channel to be deleted.
     */
    public static void deleteChannel(ID channel) {
        if (channel != null) {
            if (channel.getPathFile().exists() && !channel.getPathFile().delete()) {
                LOG.warn("IOManager failed to delete temporary file {}", channel.getPath());
            }
        }
    }

    /**
     * Gets the directories that the I/O manager spills to.
     *
     * @return The directories that the I/O manager spills to.
     */
    // 获取溢写目录。
    // 返回 FileChannelManager 管理的所有用于磁盘溢写的本地目录数组。
    public File[] getSpillingDirectories() {
        return fileChannelManager.getPaths();
    }

    /**
     * Gets the directories that the I/O manager spills to, as path strings.
     *
     * @return The directories that the I/O manager spills to, as path strings.
     */
    // 获取溢写目录的绝对路径字符串数组。
    public String[] getSpillingDirectoriesPaths() {
        File[] paths = fileChannelManager.getPaths();
        String[] strings = new String[paths.length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = paths[i].getAbsolutePath();
        }
        return strings;
    }

    // ------------------------------------------------------------------------
    //                        Reader / Writer instantiations
    // ------------------------------------------------------------------------

    /**
     * Creates a block channel writer that writes to the given channel. The writer adds the written
     * segment to its return-queue afterwards (to allow for asynchronous implementations).
     *
     * @param channelID The descriptor for the channel to write to.
     * @return A block channel writer that writes to the given channel.
     * @throws IOException Thrown, if the channel for the writer could not be opened.
     */
    public BlockChannelWriter<MemorySegment> createBlockChannelWriter(ID channelID)
            throws IOException {
        return createBlockChannelWriter(channelID, new LinkedBlockingQueue<>());
    }

    /**
     * Creates a block channel writer that writes to the given channel. The writer adds the written
     * segment to the given queue (to allow for asynchronous implementations).
     *
     * @param channelID The descriptor for the channel to write to.
     * @param returnQueue The queue to put the written buffers into.
     * @return A block channel writer that writes to the given channel.
     * @throws IOException Thrown, if the channel for the writer could not be opened.
     */
    public abstract BlockChannelWriter<MemorySegment> createBlockChannelWriter(
            ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException;

    /**
     * Creates a block channel writer that writes to the given channel. The writer calls the given
     * callback after the I/O operation has been performed (successfully or unsuccessfully), to
     * allow for asynchronous implementations.
     *
     * @param channelID The descriptor for the channel to write to.
     * @param callback The callback to be called for
     * @return A block channel writer that writes to the given channel.
     * @throws IOException Thrown, if the channel for the writer could not be opened.
     */
    public abstract BlockChannelWriterWithCallback<MemorySegment> createBlockChannelWriter(
            ID channelID, RequestDoneCallback<MemorySegment> callback) throws IOException;

    /**
     * Creates a block channel reader that reads blocks from the given channel. The reader pushed
     * full memory segments (with the read data) to its "return queue", to allow for asynchronous
     * read implementations.
     *
     * @param channelID The descriptor for the channel to write to.
     * @return A block channel reader that reads from the given channel.
     * @throws IOException Thrown, if the channel for the reader could not be opened.
     */
    public BlockChannelReader<MemorySegment> createBlockChannelReader(ID channelID)
            throws IOException {
        return createBlockChannelReader(channelID, new LinkedBlockingQueue<>());
    }

    /**
     * Creates a block channel reader that reads blocks from the given channel. The reader pushes
     * the full segments to the given queue, to allow for asynchronous implementations.
     *
     * @param channelID The descriptor for the channel to write to.
     * @param returnQueue The queue to put the full buffers into.
     * @return A block channel reader that reads from the given channel.
     * @throws IOException Thrown, if the channel for the reader could not be opened.
     */
    public abstract BlockChannelReader<MemorySegment> createBlockChannelReader(
            ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException;

    public abstract BufferFileWriter createBufferFileWriter(ID channelID) throws IOException;

    public abstract BufferFileReader createBufferFileReader(
            ID channelID, RequestDoneCallback<Buffer> callback) throws IOException;

    public abstract BufferFileSegmentReader createBufferFileSegmentReader(
            ID channelID, RequestDoneCallback<FileSegment> callback) throws IOException;

    /**
     * Creates a block channel reader that reads all blocks from the given channel directly in one
     * bulk. The reader draws segments to read the blocks into from a supplied list, which must
     * contain as many segments as the channel has blocks. After the reader is done, the list with
     * the full segments can be obtained from the reader.
     *
     * <p>If a channel is not to be read in one bulk, but in multiple smaller batches, a {@link
     * BlockChannelReader} should be used.
     *
     * @param channelID The descriptor for the channel to write to.
     * @param targetSegments The list to take the segments from into which to read the data.
     * @param numBlocks The number of blocks in the channel to read.
     * @return A block channel reader that reads from the given channel.
     * @throws IOException Thrown, if the channel for the reader could not be opened.
     */
    public abstract BulkBlockChannelReader createBulkBlockChannelReader(
            ID channelID, List<MemorySegment> targetSegments, int numBlocks) throws IOException;

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
