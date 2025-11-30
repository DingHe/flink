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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Intermediate result partition registry to use in {@link
 * org.apache.flink.runtime.jobmaster.JobMaster}.
 *
 * @param <T> partition shuffle descriptor used for producer/consumer deployment and their data
 *     exchange.
 */
// ShuffleMaster 接口是 Flink 中负责管理中间结果分区生命周期和访问信息的核心服务组件。
// 它充当 JobMaster 与底层数据交换服务（如 Flink 的 Netty 网络栈或外部 Shuffle 服务）之间的抽象层。
// 主要职责是接收任务生产者（Producer）的注册信息，生成供消费者使用的访问凭证（ShuffleDescriptor），并在适当的时候清理这些资源。
// Shuffle 资源协调： 集中管理 Job 中所有中间结果分区的状态和位置信息。
// 描述符生成： 将逻辑分区信息 (PartitionDescriptor) 和生产者信息 (ProducerDescriptor) 转换为可用于实际数据传输的物理访问描述符 (ShuffleDescriptor)。
// ShuffleMaster 是 JobMaster 用于设置和清理数据交换路径的中央控制点，确保上游任务的数据能被下游任务正确找到并消费。


public interface ShuffleMaster<T extends ShuffleDescriptor> extends AutoCloseable {

    /**
     * Starts this shuffle master as a service. One can do some initialization here, for example
     * getting access and connecting to the external system.
     */
    // 启动 Shuffle Master 服务。
    // 在 JobMaster 启动时调用，用于执行服务初始化，例如连接到外部 Shuffle 系统。
    default void start() throws Exception {}

    /**
     * Closes this shuffle master service which should release all resources. A shuffle master will
     * only be closed when the cluster is shut down.
     */
    // 关闭 Shuffle Master 服务。
    // 在集群关闭时调用，用于释放所有占用的资源。
    @Override
    default void close() throws Exception {}

    /**
     * Registers the target job together with the corresponding {@link JobShuffleContext} to this
     * shuffle master. Through the shuffle context, one can obtain some basic information like job
     * ID, job configuration. It enables ShuffleMaster to notify JobMaster about lost result
     * partitions, so that JobMaster can identify and reproduce unavailable partitions earlier.
     *
     * @param context the corresponding shuffle context of the target job.
     */
    // 注册目标作业。
    // 将一个 Job 及其上下文注册到 Shuffle Master。
    // 这允许 Shuffle Master 获取 Job 的基本配置信息，并能通知 JobMaster 分区丢失事件。
    default void registerJob(JobShuffleContext context) {}

    /**
     * Unregisters the target job from this shuffle master, which means the corresponding job has
     * reached a global termination state and all the allocated resources except for the cluster
     * partitions can be cleared.
     *
     * @param jobID ID of the target job to be unregistered.
     */
    // 注销目标作业。
    // 当 Job 完成或终止时调用，通知 Shuffle Master 可以清理该 Job 占用的所有非集群级别的 Shuffle 资源。
    default void unregisterJob(JobID jobID) {}

    /**
     * Asynchronously register a partition and its producer with the shuffle service.
     *
     * <p>The returned shuffle descriptor is an internal handle which identifies the partition
     * internally within the shuffle service. The descriptor should provide enough information to
     * read from or write data to the partition.
     *
     * @param jobID job ID of the corresponding job which registered the partition
     * @param partitionDescriptor general job graph information about the partition
     * @param producerDescriptor general producer information (location, execution id, connection
     *     info)
     * @return future with the partition shuffle descriptor used for producer/consumer deployment
     *     and their data exchange.
     */
    // 异步注册分区及其生产者。
    // 核心方法。
    // JobMaster 调用此方法注册一个中间结果分区。
    // 它接收 Job ID、分区描述符 (PartitionDescriptor) 和生产者描述符 (ProducerDescriptor)，
    // 并异步返回一个 ShuffleDescriptor (T)，供下游消费者使用。
    CompletableFuture<T> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor);

    /**
     * Release any external resources occupied by the given partition.
     *
     * <p>This call triggers release of any resources which are occupied by the given partition in
     * the external systems outside of the producer executor. This is mostly relevant for the batch
     * jobs and blocking result partitions. The producer local resources are managed by {@link
     * ShuffleDescriptor#storesLocalResourcesOn()} and {@link
     * ShuffleEnvironment#releasePartitionsLocally(Collection)}.
     *
     * @param shuffleDescriptor shuffle descriptor of the result partition to release externally.
     */
    // 外部释放分区资源。
    // 释放分区在生产者 TaskExecutor 外部占用的所有资源（例如，写入外部存储的阻塞型 Shuffle 数据）。
    // 生产者本地资源由 TaskManager 自己管理。
    void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor);

    /**
     * Compute shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
     *
     * @param taskInputsOutputsDescriptor describes task inputs and outputs information for shuffle
     *     memory calculation.
     * @return shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
     */
    // 计算任务的 Shuffle 内存大小。
    // 允许 Shuffle Master 根据任务的输入输出配置 (TaskInputsOutputsDescriptor) 定制化地计算该任务所需的 Shuffle 内存（例如，输入输出缓冲区大小）。
    default MemorySize computeShuffleMemorySizeForTask(
            TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
        return MemorySize.ZERO;
    }

    /**
     * Retrieves specified partitions and their metrics (identified by {@code expectedPartitions}),
     * the metrics include sizes of sub-partitions in a result partition.
     *
     * @param jobId ID of the target job
     * @param timeout The timeout used for retrieve the specified partitions.
     * @param expectedPartitions The set of identifiers for the result partitions whose metrics are
     *     to be fetched.
     * @return A future will contain a collection of the partitions with their metrics that could be
     *     retrieved from the expected partitions within the specified timeout period.
     */
    // 获取指定分区的指标。
    // 异步获取一组指定中间结果分区（通过 ResultPartitionID）的指标数据，例如子分区的大小等。
    default CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    /**
     * Whether the shuffle master supports taking snapshot in batch scenarios if {@link
     * org.apache.flink.configuration.BatchExecutionOptions#JOB_RECOVERY_ENABLED} is true. If it
     * returns true, Flink will call {@link #snapshotState} to take snapshot, and call {@link
     * #restoreState} to restore the state of shuffle master.
     */
    // 检查是否支持批处理快照。
    // 返回 Shuffle Master 是否支持在批处理模式下进行状态快照和恢复。
    default boolean supportsBatchSnapshot() {
        return false;
    }

    /** Triggers a snapshot of the shuffle master's state. */
    // 触发状态快照。
    // 异步触发 Shuffle Master 状态的持久化，将快照结果写入 snapshotFuture
    default void snapshotState(
            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context) {}

    /** Restores the state of the shuffle master from the provided snapshots. */
    // 恢复状态。
    // 从提供的历史快照中恢复 Shuffle Master 的内部状态。
    default void restoreState(List<ShuffleMasterSnapshot> snapshots) {}

    /**
     * Notifies that the recovery process of result partitions has started.
     *
     * @param jobId ID of the target job
     */
    // 通知分区恢复开始。
    // 在 Job 的分区恢复过程开始时通知 ShuffleMaster，以便其可以做相应的准备或状态管理。
    default void notifyPartitionRecoveryStarted(JobID jobId) {}
}
