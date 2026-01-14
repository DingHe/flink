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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.PortRange;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE;
import static org.apache.flink.configuration.ExecutionOptions.BATCH_SHUFFLE_MODE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Configuration object for the network stack. */
// 主要作用是解析、存储并校验与网络传输相关的所有配置参数。它是 NettyShuffleEnvironment 的“说明书”。
// 当 TaskManager 启动时，它会读取用户的 flink-conf.yaml 设置，通过该类的静态方法 fromConfiguration 将其转化为结构化的 Java 对象。这些配置决定了 Flink 任务在数据交换过程中的内存分配、网络超时、吞吐量优化以及磁盘溢写策略。

public class NettyShuffleEnvironmentConfiguration {
    private static final Logger LOG =
            LoggerFactory.getLogger(NettyShuffleEnvironmentConfiguration.class);

    // 网络缓冲区的总数量。
    // 由网络内存总量除以每个 Buffer 大小得出
    private final int numNetworkBuffers;
    // 单个缓冲区的字节大小（通常为 32KB），对应 MemorySegment 的大小。
    private final int networkBufferSize;
    // 请求分区数据失败时的初始重试等待时间。
    private final int partitionRequestInitialBackoff;
    // 最大重试等待时间。
    private final int partitionRequestMaxBackoff;
    // 分区请求监听器的超时时间
    private final int partitionRequestListenerTimeout;

    /**
     * Number of network buffers to use for each outgoing/incoming channel (subpartition/input
     * channel).
     */
    // 每个输入/输出通道（Subpartition/InputChannel）分配的**独占（Exclusive）**缓冲区数量。
    private final int networkBuffersPerChannel;

    /**
     * Number of extra network buffers to use for each outgoing/incoming gate (result
     * partition/input gate).
     */
    // 每个 Gate（InputGate/ResultPartition）分配的**浮动（Floating）**缓冲区数量，用于处理倾斜。
    private final int floatingNetworkBuffersPerGate;
    // 每个 Gate 正常运行所需的最大缓冲区上限。
    private final Optional<Integer> maxRequiredBuffersPerGate;
    // 排序 Shuffle（Sort-merge Shuffle）触发的最小内存和并行度阈值。
    private final int sortShuffleMinBuffers;

    private final int sortShuffleMinParallelism;

    /** Size of direct memory to be allocated for blocking shuffle data read. */
    // 用于批处理 Shuffle 数据读取的直接内存大小
    private final long batchShuffleReadMemoryBytes;
    // 从缓冲池申请内存段（Segment）的等待超时时间
    private final Duration requestSegmentsTimeout;
    // 是否开启详细的网络指标（开启后会监控每个 Channel 的状态，但有性能开销）。
    private final boolean isNetworkDetailedMetrics;
    // 封装了底层的 Netty 相关配置（如端口、SSL 证书等）
    private final NettyConfig nettyConfig;
    // 存储 Shuffle 溢写数据的本地临时目录列表。
    private final String[] tempDirs;
    // 阻塞式分区类型（如 FILE（文件）、mmap（内存映射）或 AUTO）。
    private final BoundedBlockingSubpartitionType blockingSubpartitionType;
    // 批处理数据传输是否启用压缩以及使用哪种压缩算法（LZ4, ZSTD 等）。
    private final boolean batchShuffleCompressionEnabled;

    private final CompressionCodec compressionCodec;
    // 每个 Channel 最多能占用的缓冲区上限（防止单个 Channel 耗尽全局内存）。
    private final int maxBuffersPerChannel;
    // **缓冲区反膨胀（Buffer Debloating）**配置，用于根据吞吐量动态调整缓冲区大小，降低延迟。
    private final BufferDebloatConfiguration debloatConfiguration;

    /** The maximum number of tpc connections between taskmanagers for data communication. */
    // 不同 TaskManager 之间允许建立的最大 TCP 连接数（用于连接复用和多路复用）。
    private final int maxNumberOfConnections;
    // 是否允许跨作业（Across Jobs）复用 TCP 连接
    private final boolean connectionReuseEnabled;
    // 每个 Gate 允许的**透支（Overdraft）**缓冲区数量，用于应对瞬时流量峰值。
    private final int maxOverdraftBuffersPerGate;
    // **分层存储（Tiered Storage）**配置，Flink 1.18+ 引入的混合存储架构配置。
    private final TieredStorageConfiguration tieredStorageConfiguration;

    public NettyShuffleEnvironmentConfiguration(
            int numNetworkBuffers,
            int networkBufferSize,
            int partitionRequestInitialBackoff,
            int partitionRequestMaxBackoff,
            int partitionRequestListenerTimeout,
            int networkBuffersPerChannel,
            int floatingNetworkBuffersPerGate,
            Optional<Integer> maxRequiredBuffersPerGate,
            Duration requestSegmentsTimeout,
            boolean isNetworkDetailedMetrics,
            @Nullable NettyConfig nettyConfig,
            String[] tempDirs,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            boolean batchShuffleCompressionEnabled,
            CompressionCodec compressionCodec,
            int maxBuffersPerChannel,
            long batchShuffleReadMemoryBytes,
            int sortShuffleMinBuffers,
            int sortShuffleMinParallelism,
            BufferDebloatConfiguration debloatConfiguration,
            int maxNumberOfConnections,
            boolean connectionReuseEnabled,
            int maxOverdraftBuffersPerGate,
            @Nullable TieredStorageConfiguration tieredStorageConfiguration) {

        this.numNetworkBuffers = numNetworkBuffers;
        this.networkBufferSize = networkBufferSize;
        this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
        this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
        this.partitionRequestListenerTimeout = partitionRequestListenerTimeout;
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
        this.maxRequiredBuffersPerGate = maxRequiredBuffersPerGate;
        this.requestSegmentsTimeout = Preconditions.checkNotNull(requestSegmentsTimeout);
        this.isNetworkDetailedMetrics = isNetworkDetailedMetrics;
        this.nettyConfig = nettyConfig;
        this.tempDirs = Preconditions.checkNotNull(tempDirs);
        this.blockingSubpartitionType = Preconditions.checkNotNull(blockingSubpartitionType);
        this.batchShuffleCompressionEnabled = batchShuffleCompressionEnabled;
        this.compressionCodec = Preconditions.checkNotNull(compressionCodec);
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.batchShuffleReadMemoryBytes = batchShuffleReadMemoryBytes;
        this.sortShuffleMinBuffers = sortShuffleMinBuffers;
        this.sortShuffleMinParallelism = sortShuffleMinParallelism;
        this.debloatConfiguration = debloatConfiguration;
        this.maxNumberOfConnections = maxNumberOfConnections;
        this.connectionReuseEnabled = connectionReuseEnabled;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
        this.tieredStorageConfiguration = tieredStorageConfiguration;
    }

    // ------------------------------------------------------------------------

    public int numNetworkBuffers() {
        return numNetworkBuffers;
    }

    public int networkBufferSize() {
        return networkBufferSize;
    }

    public int partitionRequestInitialBackoff() {
        return partitionRequestInitialBackoff;
    }

    public int partitionRequestMaxBackoff() {
        return partitionRequestMaxBackoff;
    }

    public int getPartitionRequestListenerTimeout() {
        return partitionRequestListenerTimeout;
    }

    public int networkBuffersPerChannel() {
        return networkBuffersPerChannel;
    }

    public int floatingNetworkBuffersPerGate() {
        return floatingNetworkBuffersPerGate;
    }

    public Optional<Integer> maxRequiredBuffersPerGate() {
        return maxRequiredBuffersPerGate;
    }

    public long batchShuffleReadMemoryBytes() {
        return batchShuffleReadMemoryBytes;
    }

    public int sortShuffleMinBuffers() {
        return sortShuffleMinBuffers;
    }

    public int sortShuffleMinParallelism() {
        return sortShuffleMinParallelism;
    }

    public Duration getRequestSegmentsTimeout() {
        return requestSegmentsTimeout;
    }

    public NettyConfig nettyConfig() {
        return nettyConfig;
    }

    public boolean isNetworkDetailedMetrics() {
        return isNetworkDetailedMetrics;
    }

    public String[] getTempDirs() {
        return tempDirs;
    }

    public boolean isConnectionReuseEnabled() {
        return connectionReuseEnabled;
    }

    public BoundedBlockingSubpartitionType getBlockingSubpartitionType() {
        return blockingSubpartitionType;
    }

    public boolean isBatchShuffleCompressionEnabled() {
        return batchShuffleCompressionEnabled;
    }

    public BufferDebloatConfiguration getDebloatConfiguration() {
        return debloatConfiguration;
    }

    public boolean isSSLEnabled() {
        return nettyConfig != null && nettyConfig.getSSLEnabled();
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public int getMaxBuffersPerChannel() {
        return maxBuffersPerChannel;
    }

    public int getMaxNumberOfConnections() {
        return maxNumberOfConnections;
    }

    public int getMaxOverdraftBuffersPerGate() {
        return maxOverdraftBuffersPerGate;
    }

    public TieredStorageConfiguration getTieredStorageConfiguration() {
        return tieredStorageConfiguration;
    }

    // ------------------------------------------------------------------------

    /**
     * Utility method to extract network related parameters from the configuration and to sanity
     * check them.
     *
     * @param configuration configuration object
     * @param networkMemorySize the size of memory reserved for shuffle environment
     * @param localTaskManagerCommunication true, to skip initializing the network stack
     * @param taskManagerAddress identifying the IP address under which the TaskManager will be
     *     accessible
     * @return NettyShuffleEnvironmentConfiguration
     */
    // Flink 网络环境配置的核心工厂方法
    // 负责将用户在 flink-conf.yaml 中定义的原始配置转化为可直接驱动 NettyShuffleEnvironment 的强类型对象。
    public static NettyShuffleEnvironmentConfiguration fromConfiguration(
            Configuration configuration,
            MemorySize networkMemorySize,
            boolean localTaskManagerCommunication,
            InetAddress taskManagerAddress) {

        // 获取 TaskManager 用于数据交换监听的端口范围。
        // 如果配置了 taskmanager.data.bind-port 则优先使用，否则使用 taskmanager.data.port
        final PortRange dataBindPortRange = getDataBindPortRange(configuration);
        // 获取内存页大小（默认 32KB）。
        // 这是 Flink 内存管理的基本单位，决定了单个 Buffer 的大小。
        final int pageSize = ConfigurationParserUtils.getPageSize(configuration);
        // 创建底层的 Netty 配置对象。
        // 如果 TaskManager 之间需要跨节点通信，这里会配置线程池、低水位、高水位以及 SSL 安全参数。
        final NettyConfig nettyConfig =
                createNettyConfig(
                        configuration,
                        localTaskManagerCommunication,
                        taskManagerAddress,
                        dataBindPortRange);
        // 计算总的缓冲区数量。逻辑很简单：可用网络内存总量 / pageSize
        final int numberOfNetworkBuffers =
                calculateNumberOfNetworkBuffers(configuration, networkMemorySize, pageSize);

        // 设置分区请求失败时的“退避（Backoff）”重试时间。
        // 当下游请求上游数据还没准备好时，会按指数增长的时间间隔进行重试，避免无效的轮询。
        int initialRequestBackoff =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
        int maxRequestBackoff =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);
        // 设置分区请求监听器的超时时间。如果长时间拿不到数据，会触发超时异常。
        int listenerTimeout =
                (int)
                        configuration
                                .get(
                                        NettyShuffleEnvironmentOptions
                                                .NETWORK_PARTITION_REQUEST_TIMEOUT)
                                .toMillis();
        // 每个通道预分配的硬信用
        int buffersPerChannel =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        // Gate 级别的共享信用（Floating Buffers），用于应对数据倾斜。
        int extraBuffersPerGate =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

        // 设置读取和发送的上限控制
        Optional<Integer> maxRequiredBuffersPerGate =
                configuration.getOptional(
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE);

        int maxBuffersPerChannel =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_MAX_BUFFERS_PER_CHANNEL);

        int maxOverdraftBuffersPerGate =
                configuration.get(
                        NettyShuffleEnvironmentOptions.NETWORK_MAX_OVERDRAFT_BUFFERS_PER_GATE);
        // 批处理模式下，从磁盘读取 Shuffle 数据所需的堆外内存大小。
        long batchShuffleReadMemoryBytes =
                configuration.get(TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY).getBytes();
        // 决定何时启动 Sort-Merge Shuffle。当并行度较大时，传统的每个分区一个文件的方式会产生过多小文件，此时会切换为基于排序的单文件模式。
        int sortShuffleMinBuffers =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        int sortShuffleMinParallelism =
                configuration.get(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM);

        boolean isNetworkDetailedMetrics =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS);
        // 获取本地临时目录并打乱顺序。打乱顺序是为了让不同的 TaskManager 均匀地使用不同的磁盘，防止所有 Task 都挤在第一块磁盘上。
        String[] tempDirs = ConfigurationUtils.parseTempDirectories(configuration);
        // Shuffle the data directories to make it fairer for directory selection between different
        // TaskManagers, which is good for load balance especially when there are multiple disks.
        List<String> shuffleDirs = Arrays.asList(tempDirs);
        Collections.shuffle(shuffleDirs);

        // 定义申请内存段的超时时间，以及阻塞分区的实现方式（如 mmap 或普通文件 IO）。
        Duration requestSegmentsTimeout =
                configuration.get(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_REQUEST_TIMEOUT);

        BoundedBlockingSubpartitionType blockingSubpartitionType =
                getBlockingSubpartitionType(configuration);
        // 决定是否对 Shuffle 数据进行压缩（LZ4, ZSTD 等）。
        // 这段逻辑中包含了一个警告：如果旧配置关闭了压缩但新配置设了算法，会提示用户。
        CompressionCodec compressionCodec =
                configuration.get(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);

        boolean batchShuffleCompressionEnabled;
        if (compressionCodec == CompressionCodec.NONE) {
            batchShuffleCompressionEnabled = false;
        } else {
            batchShuffleCompressionEnabled =
                    configuration.get(
                            NettyShuffleEnvironmentOptions.BATCH_SHUFFLE_COMPRESSION_ENABLED);

            if (!batchShuffleCompressionEnabled) {
                LOG.warn(
                        "Deprecated configuration key {} is used to disable the compression. "
                                + "Please set the {} to \"None\" to disable the compression.",
                        NettyShuffleEnvironmentOptions.BATCH_SHUFFLE_COMPRESSION_ENABLED.key(),
                        NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC.key());
            }
        }
        // 配置 TM 之间的 TCP 连接数。
        // 增加连接数可以减少多路复用时的竞争。connectionReuseEnabled 允许跨 Job 复用连接，提高短作业的启动效率。
        int maxNumConnections =
                Math.max(
                        1,
                        configuration.get(NettyShuffleEnvironmentOptions.MAX_NUM_TCP_CONNECTIONS));

        boolean connectionReuseEnabled =
                configuration.get(
                        NettyShuffleEnvironmentOptions.TCP_CONNECTION_REUSE_ACROSS_JOBS_ENABLED);
        // 确保用户填写的参数是合法的（比如缓冲区数量不能是负数，Gate 至少需要一个 Buffer）。
        checkArgument(buffersPerChannel >= 0, "Must be non-negative.");
        checkArgument(
                !maxRequiredBuffersPerGate.isPresent() || maxRequiredBuffersPerGate.get() >= 1,
                String.format(
                        "At least one buffer is required for each gate, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE
                                .key()));
        checkArgument(
                extraBuffersPerGate >= 1,
                String.format(
                        "The configured floating buffer should be at least 1, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.key()));

        // 支持 Flink 的新特性——分层存储。
        // 如果是 Hybrid Shuffle 模式，则加载分层存储配置，将数据根据冷热分散在内存和远程存储中。
        TieredStorageConfiguration tieredStorageConfiguration = null;
        if ((configuration.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_FULL
                || configuration.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_SELECTIVE)) {
            tieredStorageConfiguration =
                    TieredStorageConfiguration.fromConfiguration(configuration);
        }
        return new NettyShuffleEnvironmentConfiguration(
                numberOfNetworkBuffers,
                pageSize,
                initialRequestBackoff,
                maxRequestBackoff,
                listenerTimeout,
                buffersPerChannel,
                extraBuffersPerGate,
                maxRequiredBuffersPerGate,
                requestSegmentsTimeout,
                isNetworkDetailedMetrics,
                nettyConfig,
                shuffleDirs.toArray(tempDirs),
                blockingSubpartitionType,
                batchShuffleCompressionEnabled,
                compressionCodec,
                maxBuffersPerChannel,
                batchShuffleReadMemoryBytes,
                sortShuffleMinBuffers,
                sortShuffleMinParallelism,
                BufferDebloatConfiguration.fromConfiguration(configuration),
                maxNumConnections,
                connectionReuseEnabled,
                maxOverdraftBuffersPerGate,
                tieredStorageConfiguration);
    }

    /**
     * Parses the hosts / ports for communication and data exchange from configuration.
     *
     * @param configuration configuration object
     * @return the data port
     */
    private static PortRange getDataBindPortRange(Configuration configuration) {
        if (configuration.contains(NettyShuffleEnvironmentOptions.DATA_BIND_PORT)) {
            String dataBindPort = configuration.get(NettyShuffleEnvironmentOptions.DATA_BIND_PORT);

            return new PortRange(dataBindPort);
        }

        int dataBindPort = configuration.get(NettyShuffleEnvironmentOptions.DATA_PORT);
        ConfigurationParserUtils.checkConfigParameter(
                dataBindPort >= 0,
                dataBindPort,
                NettyShuffleEnvironmentOptions.DATA_PORT.key(),
                "Leave config parameter empty or use 0 to let the system choose a port automatically.");

        return new PortRange(dataBindPort);
    }

    /**
     * Calculates the number of network buffers based on configuration and jvm heap size.
     *
     * @param configuration configuration object
     * @param networkMemorySize the size of memory reserved for shuffle environment
     * @param pageSize size of memory segment
     * @return the number of network buffers
     */
    private static int calculateNumberOfNetworkBuffers(
            Configuration configuration, MemorySize networkMemorySize, int pageSize) {

        logIfIgnoringOldConfigs(configuration);

        // tolerate offcuts between intended and allocated memory due to segmentation (will be
        // available to the user-space memory)
        long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
        if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The given number of memory bytes ("
                            + networkMemorySize.getBytes()
                            + ") corresponds to more than MAX_INT pages.");
        }

        return (int) numberOfNetworkBuffersLong;
    }

    @SuppressWarnings("deprecation")
    private static void logIfIgnoringOldConfigs(Configuration configuration) {
        if (configuration.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS)) {
            LOG.info(
                    "Ignoring old (but still present) network buffer configuration via {}.",
                    NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key());
        }
    }

    /**
     * Generates {@link NettyConfig} from Flink {@link Configuration}.
     *
     * @param configuration configuration object
     * @param localTaskManagerCommunication true, to skip initializing the network stack
     * @param taskManagerAddress identifying the IP address under which the TaskManager will be
     *     accessible
     * @param dataPortRange data port range for communication and data exchange
     * @return the netty configuration or {@code null} if communication is in the same task manager
     */
    @Nullable
    private static NettyConfig createNettyConfig(
            Configuration configuration,
            boolean localTaskManagerCommunication,
            InetAddress taskManagerAddress,
            PortRange dataPortRange) {

        final NettyConfig nettyConfig;
        if (!localTaskManagerCommunication) {
            final InetSocketAddress taskManagerInetSocketAddress =
                    new InetSocketAddress(taskManagerAddress, 0);

            nettyConfig =
                    new NettyConfig(
                            taskManagerInetSocketAddress.getAddress(),
                            dataPortRange,
                            ConfigurationParserUtils.getPageSize(configuration),
                            ConfigurationParserUtils.getSlot(configuration),
                            configuration);
        } else {
            nettyConfig = null;
        }

        return nettyConfig;
    }

    private static BoundedBlockingSubpartitionType getBlockingSubpartitionType(
            Configuration config) {
        String transport = config.get(NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE);

        switch (transport) {
            case "mmap":
                return BoundedBlockingSubpartitionType.FILE_MMAP;
            case "file":
                return BoundedBlockingSubpartitionType.FILE;
            default:
                return BoundedBlockingSubpartitionType.AUTO;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + numNetworkBuffers;
        result = 31 * result + networkBufferSize;
        result = 31 * result + partitionRequestInitialBackoff;
        result = 31 * result + partitionRequestMaxBackoff;
        result = 31 * result + partitionRequestListenerTimeout;
        result = 31 * result + networkBuffersPerChannel;
        result = 31 * result + floatingNetworkBuffersPerGate;
        result = 31 * result + requestSegmentsTimeout.hashCode();
        result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(tempDirs);
        result = 31 * result + (batchShuffleCompressionEnabled ? 1 : 0);
        result = 31 * result + Objects.hashCode(compressionCodec);
        result = 31 * result + maxBuffersPerChannel;
        result = 31 * result + Objects.hashCode(batchShuffleReadMemoryBytes);
        result = 31 * result + sortShuffleMinBuffers;
        result = 31 * result + sortShuffleMinParallelism;
        result = 31 * result + maxNumberOfConnections;
        result = 31 * result + (connectionReuseEnabled ? 1 : 0);
        result = 31 * result + maxOverdraftBuffersPerGate;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final NettyShuffleEnvironmentConfiguration that =
                    (NettyShuffleEnvironmentConfiguration) obj;

            return this.numNetworkBuffers == that.numNetworkBuffers
                    && this.networkBufferSize == that.networkBufferSize
                    && this.partitionRequestInitialBackoff == that.partitionRequestInitialBackoff
                    && this.partitionRequestMaxBackoff == that.partitionRequestMaxBackoff
                    && this.networkBuffersPerChannel == that.networkBuffersPerChannel
                    && this.floatingNetworkBuffersPerGate == that.floatingNetworkBuffersPerGate
                    && this.batchShuffleReadMemoryBytes == that.batchShuffleReadMemoryBytes
                    && this.sortShuffleMinBuffers == that.sortShuffleMinBuffers
                    && this.sortShuffleMinParallelism == that.sortShuffleMinParallelism
                    && this.requestSegmentsTimeout.equals(that.requestSegmentsTimeout)
                    && (nettyConfig != null
                            ? nettyConfig.equals(that.nettyConfig)
                            : that.nettyConfig == null)
                    && Arrays.equals(this.tempDirs, that.tempDirs)
                    && this.batchShuffleCompressionEnabled == that.batchShuffleCompressionEnabled
                    && this.maxBuffersPerChannel == that.maxBuffersPerChannel
                    && this.partitionRequestListenerTimeout == that.partitionRequestListenerTimeout
                    && Objects.equals(this.compressionCodec, that.compressionCodec)
                    && this.maxNumberOfConnections == that.maxNumberOfConnections
                    && this.connectionReuseEnabled == that.connectionReuseEnabled
                    && this.maxOverdraftBuffersPerGate == that.maxOverdraftBuffersPerGate;
        }
    }

    @Override
    public String toString() {
        return "NettyShuffleEnvironmentConfiguration{"
                + ", numNetworkBuffers="
                + numNetworkBuffers
                + ", networkBufferSize="
                + networkBufferSize
                + ", partitionRequestInitialBackoff="
                + partitionRequestInitialBackoff
                + ", partitionRequestMaxBackoff="
                + partitionRequestMaxBackoff
                + ", networkBuffersPerChannel="
                + networkBuffersPerChannel
                + ", floatingNetworkBuffersPerGate="
                + floatingNetworkBuffersPerGate
                + ", requestSegmentsTimeout="
                + requestSegmentsTimeout
                + ", nettyConfig="
                + nettyConfig
                + ", tempDirs="
                + Arrays.toString(tempDirs)
                + ", blockingShuffleCompressionEnabled="
                + batchShuffleCompressionEnabled
                + ", compressionCodec="
                + compressionCodec
                + ", maxBuffersPerChannel="
                + maxBuffersPerChannel
                + ", partitionRequestListenerTimeout"
                + partitionRequestListenerTimeout
                + ", batchShuffleReadMemoryBytes="
                + batchShuffleReadMemoryBytes
                + ", sortShuffleMinBuffers="
                + sortShuffleMinBuffers
                + ", sortShuffleMinParallelism="
                + sortShuffleMinParallelism
                + ", maxNumberOfConnections="
                + maxNumberOfConnections
                + ", connectionReuseEnabled="
                + connectionReuseEnabled
                + ", maxOverdraftBuffersPerGate="
                + maxOverdraftBuffersPerGate
                + '}';
    }
}
