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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * Interface for shuffle deployment descriptor of result partition resource.
 *
 * <p>The descriptor is used for the deployment of the partition producer/consumer and their data
 * exchange
 */
// ShuffleDescriptor 接口是 Flink 中用于描述中间结果分区 (Result Partition) 在网络和存储层面的部署和访问信息的通用契约。
// 核心作用是将逻辑结果分区（ResultPartitionID）与其物理存取方式（例如，在哪台 TaskManager 上，通过哪个外部 Shuffle 服务）关联起来，以指导任务的部署和数据交换。
// 数据访问凭证： 它是数据消费者（下游任务）访问生产者（上游任务）产生的中间结果所需的所有配置和地址信息的抽象。
// Shuffle 部署指导： 它被包含在 TaskDeploymentDescriptor 中，用于指导 TaskManager 的 Shuffle Environment 如何设置数据传输通道。
public interface ShuffleDescriptor extends Serializable {
    // 获取结果分区 ID。
    // 返回该描述符所指向的逻辑中间结果分区的唯一标识符。这是建立数据连接的基础。
    ResultPartitionID getResultPartitionID();

    /**
     * Returns whether the partition is known and registered with the {@link ShuffleMaster}
     * implementation.
     *
     * <p>When a partition consumer is being scheduled, it can happen that the producer of the
     * partition (consumer input channel) has not been scheduled and its location and other relevant
     * data is yet to be defined. To proceed with the consumer deployment, currently unknown input
     * channels have to be marked with placeholders. The placeholder is a special implementation of
     * the shuffle descriptor: {@link UnknownShuffleDescriptor}.
     *
     * <p>Note: this method is not supposed to be overridden in concrete shuffle implementation. The
     * only class where it returns {@code true} is {@link UnknownShuffleDescriptor}.
     *
     * @return whether the partition producer has been ever deployed and the corresponding shuffle
     *     descriptor is obtained from the {@link ShuffleMaster} implementation.
     */
    // 检查是否为未知描述符。
    // 默认返回 false。
    // 该方法用于判断 Shuffle Descriptor 是否为占位符 (UnknownShuffleDescriptor)。
    default boolean isUnknown() {
        return false;
    }

    /**
     * Returns the location of the producing task executor if the partition occupies local resources
     * there.
     *
     * <p>Indicates that this partition occupies local resources in the producing task executor.
     * Such partition requires that the task executor is running and being connected to be able to
     * consume the produced data. This is mostly relevant for the batch jobs and blocking result
     * partitions which can outlive the producer lifetime and be released externally. {@link
     * ShuffleEnvironment#releasePartitionsLocally(Collection)} can be used to release such kind of
     * partitions locally.
     *
     * @return the resource id of the producing task executor if the partition occupies local
     *     resources there
     */
    // 本地资源存储位置。
    // 返回该分区是否占用了生产任务 TaskExecutor 上的本地资源（例如本地文件或内存缓冲区）。
    Optional<ResourceID> storesLocalResourcesOn();
}
