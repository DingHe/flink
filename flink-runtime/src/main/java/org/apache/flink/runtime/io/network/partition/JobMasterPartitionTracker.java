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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
// JobMasterPartitionTracker 是 PartitionTracker 接口的一个具体实现（扩展）。
// 它专门运行在 JobMaster 上，负责跟踪作业执行过程中产生的所有结果分区（ResultPartition），并管理这些分区的生命周期。
// 跟踪与映射： 记录哪个 TaskExecutor (ResourceID) 产生了哪个结果分区，并将分区与它的部署描述符 (ResultPartitionDeploymentDescriptor) 关联起来。
// 生命周期管理： 提供了发起**分区释放（Release）和分区提升（Promote）**操作的方法。当分区不再需要时（如任务失败、作业完成），JobMaster 通过此接口向 TaskExecutor 和 ShuffleMaster 发送命令清理资源。
// Shuffle 集成： 提供获取集群分区 Shuffle 描述符的方法，支持 Task 部署时建立数据连接。
// K 被具体化为 ResourceID：表示生成该分区的 TaskExecutor 的唯一标识。
// M 被具体化为 ResultPartitionDeploymentDescriptor：表示该分区的所有部署元数据。
public interface JobMasterPartitionTracker
        extends PartitionTracker<ResourceID, ResultPartitionDeploymentDescriptor> {

    /**
     * Starts the tracking of the given partition for the given task executor ID.
     *
     * @param producingTaskExecutorId ID of task executor on which the partition is produced
     * @param resultPartitionDeploymentDescriptor deployment descriptor of the partition
     */
    // 停止跟踪某 TaskExecutor 上的所有分区
    // 当某个 TaskExecutor 失败或离线时，JobMaster 调用此方法，停止跟踪该 TaskExecutor 上所有产出的结果分区。
    void startTrackingPartition(
            ResourceID producingTaskExecutorId,
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor);

    /** Releases the given partitions and stop the tracking of partitions that were released. */
    // 停止跟踪指定的分区。
    // 仅从跟踪器中移除指定的分区，通常作为后续释放或提升操作的一部分。
    default void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        stopTrackingAndReleasePartitions(resultPartitionIds, true);
    }

    /**
     * Releases the given partitions and stop the tracking of partitions that were released. The
     * boolean flag indicates whether we need to notify the ShuffleMaster to release all external
     * resources or not.
     */
    void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster);

    /**
     * Promotes the given partitions, and stops the tracking of partitions that were promoted.
     *
     * @param resultPartitionIds ID of the partition containing both job partitions and cluster
     *     partitions.
     * @return Future that will be completed if the partitions are promoted.
     */
    CompletableFuture<Void> stopTrackingAndPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds);

    /** Gets all the partitions under tracking. */
    Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions();

    /** Gets all the non-cluster partitions under tracking. */
    default Collection<ResultPartitionDeploymentDescriptor> getAllTrackedNonClusterPartitions() {
        return getAllTrackedPartitions().stream()
                .filter(descriptor -> !descriptor.getPartitionType().isPersistent())
                .collect(Collectors.toList());
    }

    /** Gets all the cluster partitions under tracking. */
    default Collection<ResultPartitionDeploymentDescriptor> getAllTrackedClusterPartitions() {
        return getAllTrackedPartitions().stream()
                .filter(descriptor -> descriptor.getPartitionType().isPersistent())
                .collect(Collectors.toList());
    }

    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /** Get the shuffle descriptors of the cluster partitions ordered by partition number. */
    List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID);
}
