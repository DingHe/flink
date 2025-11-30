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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMaster;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE;
import static org.apache.flink.configuration.ExecutionOptions.BATCH_SHUFFLE_MODE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
// Flink 中 ShuffleMaster 接口的一个默认实现，专门用于管理基于 Netty 和本地文件的 Flink 内置数据交换机制。
// 主要负责生成 Netty 连接描述符 (NettyShuffleDescriptor)、计算任务所需的网络内存资源，并集成对高级 Shuffle 功能（如分层 Shuffle/Hybrid Shuffle）的支持。
// NettyShuffleMaster 在 Flink 的数据交换层扮演了配置中心和描述符工厂的角色：
// Shuffle 描述符生成： 接收任务的逻辑和物理信息（PartitionDescriptor 和 ProducerDescriptor），并生成具体的 NettyShuffleDescriptor，其中包含下游消费者通过 Netty 连接获取数据所需的所有地址和 ID 信息。
// 网络资源预估： 基于 Flink 的网络配置和任务的输入输出结构，精确计算一个任务在 TaskManager 上启动和运行所需的网络缓冲区内存大小。
// 支持混合 Shuffle (Tiered Shuffle)： 如果配置开启，它会初始化并委托给 TieredInternalShuffleMaster 来处理更复杂的分层存储和数据访问逻辑。

/** Default {@link ShuffleMaster} for netty and local file based shuffle implementation. */
public class NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {
    // 每个输入通道的缓冲区数。
    // 从配置中读取，定义每个输入通道应分配的最小网络缓冲区数量。
    private final int buffersPerInputChannel;
    // 每个 Gate 的浮动缓冲区数。
    // 从配置中读取，定义每个输入门 (Input Gate) 可额外分配的浮动缓冲区数量。
    private final int floatingBuffersPerGate;
    // 每个 Gate 最大必需缓冲区数。
    // 可选配置，定义读取数据的输入门所需的网络缓冲区的上限。
    private final Optional<Integer> maxRequiredBuffersPerGate;
    // 排序 Shuffle 最小并行度
    // 从配置中读取，用于基于排序的 Shuffle，定义启用特定优化所需的最小任务并行度。
    private final int sortShuffleMinParallelism;
    // 排序 Shuffle 最小缓冲区数。
    // 定义排序 Shuffle 操作所需的最小网络缓冲区数量。
    private final int sortShuffleMinBuffers;
    // 网络缓冲区大小。
    // 从配置中读取，定义 Flink 网络缓冲区（页）的字节大小。
    private final int networkBufferSize;
    // 分层 Shuffle 内部主控。
    // 仅在启用混合/分层 Shuffle 时初始化。 负责处理分层存储相关的 Shuffle 逻辑。
    @Nullable private final TieredInternalShuffleMaster tieredInternalShuffleMaster;
    // 作业 Shuffle 上下文映射。
    // 存储所有已注册 Job 的 Shuffle 上下文，用于 JobMaster 与 ShuffleMaster 之间的通信和回调。
    private final Map<JobID, JobShuffleContext> jobShuffleContexts = new HashMap<>();

    public NettyShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
        Configuration conf = shuffleMasterContext.getConfiguration();
        checkNotNull(conf);
        buffersPerInputChannel =
                conf.get(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        floatingBuffersPerGate =
                conf.get(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
        maxRequiredBuffersPerGate =
                conf.getOptional(
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE);
        sortShuffleMinParallelism =
                conf.get(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM);
        sortShuffleMinBuffers =
                conf.get(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        networkBufferSize = ConfigurationParserUtils.getPageSize(conf);

        if (isHybridShuffleEnabled(conf)) {
            tieredInternalShuffleMaster = new TieredInternalShuffleMaster(shuffleMasterContext);
        } else {
            tieredInternalShuffleMaster = null;
        }

        checkArgument(
                !maxRequiredBuffersPerGate.isPresent() || maxRequiredBuffersPerGate.get() >= 1,
                String.format(
                        "At least one buffer is required for each gate, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE
                                .key()));
        checkArgument(
                floatingBuffersPerGate >= 1,
                String.format(
                        "The configured floating buffer should be at least 1, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.key()));
    }

    @Override
    public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {

        ResultPartitionID resultPartitionID =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());

        List<TierShuffleDescriptor> tierShuffleDescriptors = null;
        if (tieredInternalShuffleMaster != null) {
            tierShuffleDescriptors =
                    tieredInternalShuffleMaster.addPartitionAndGetShuffleDescriptor(
                            jobID, resultPartitionID);
        }

        NettyShuffleDescriptor shuffleDeploymentDescriptor =
                new NettyShuffleDescriptor(
                        producerDescriptor.getProducerLocation(),
                        createConnectionInfo(
                                producerDescriptor, partitionDescriptor.getConnectionIndex()),
                        resultPartitionID,
                        tierShuffleDescriptors);
        return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.releasePartition(shuffleDescriptor);
        }
    }

    private static PartitionConnectionInfo createConnectionInfo(
            ProducerDescriptor producerDescriptor, int connectionIndex) {
        return producerDescriptor.getDataPort() >= 0
                ? NetworkPartitionConnectionInfo.fromProducerDescriptor(
                        producerDescriptor, connectionIndex)
                : LocalExecutionPartitionConnectionInfo.INSTANCE;
    }

    /**
     * JM announces network memory requirement from the calculating result of this method. Please
     * note that the calculating algorithm depends on both I/O details of a vertex and network
     * configuration, e.g. {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and
     * {@link NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}, which means we should
     * always keep the consistency of configurations between JM, RM and TM in fine-grained resource
     * management, thus to guarantee that the processes of memory announcing and allocating respect
     * each other.
     */
    @Override
    public MemorySize computeShuffleMemorySizeForTask(TaskInputsOutputsDescriptor desc) {
        checkNotNull(desc);

        int numRequiredNetworkBuffers =
                NettyShuffleUtils.computeNetworkBuffersForAnnouncing(
                        buffersPerInputChannel,
                        floatingBuffersPerGate,
                        maxRequiredBuffersPerGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        desc.getInputChannelNums(),
                        desc.getPartitionReuseCount(),
                        desc.getSubpartitionNums(),
                        desc.getInputPartitionTypes(),
                        desc.getPartitionTypes());

        return new MemorySize((long) networkBufferSize * numRequiredNetworkBuffers);
    }

    private boolean isHybridShuffleEnabled(Configuration conf) {
        return (conf.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_FULL
                || conf.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_SELECTIVE);
    }

    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        return checkNotNull(jobShuffleContexts.get(jobId))
                .getPartitionWithMetrics(timeout, expectedPartitions);
    }

    @Override
    public void registerJob(JobShuffleContext context) {
        jobShuffleContexts.put(context.getJobId(), context);
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.registerJob(context);
        }
    }

    @Override
    public void unregisterJob(JobID jobId) {
        jobShuffleContexts.remove(jobId);
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.unregisterJob(jobId);
        }
    }

    @Override
    public boolean supportsBatchSnapshot() {
        return true;
    }

    @Override
    public void snapshotState(
            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context) {
        snapshotFuture.complete(EmptyShuffleMasterSnapshot.getInstance());
    }

    @Override
    public void notifyPartitionRecoveryStarted(JobID jobId) {
        checkNotNull(jobShuffleContexts.get(jobId)).notifyPartitionRecoveryStarted();
    }

    @Override
    public void close() throws Exception {
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.close();
        }
    }
}
