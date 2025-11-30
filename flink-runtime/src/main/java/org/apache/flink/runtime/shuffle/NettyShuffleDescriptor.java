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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

/** Default implementation of {@link ShuffleDescriptor} for {@link NettyShuffleMaster}. */
// NettyShuffleDescriptor 是 Flink 基于 Netty 的内部网络堆栈实现 ShuffleDescriptor 接口的默认实现类。
// 核心作用是为 Flink 数据流经 Netty 网络栈的中间结果分区提供完整的物理访问信息和连接详情。
// 当一个任务（消费者）需要读取另一个任务（生产者）的中间结果数据时，它会使用 NettyShuffleDescriptor 来建立连接和获取数据。
// Netty 连接信息封装： 它封装了下游任务通过 Netty 连接上游 TaskManager 提取数据所需的全部信息，包括生产者的地址、端口和连接索引。
// 本地性判断： 提供了快速判断数据生产者和数据消费者是否在同一个 TaskManager 上运行的能力（即数据是否为本地）。
// Tiered Shuffle 支持： 支持分层 Shuffle (Tiered Shuffle)，可以携带不同存储层（Tier）的 Shuffle 描述符。
// NettyShuffleDescriptor 是数据消费者获取数据源的**“通信地址簿和钥匙”**，专门用于指导 Flink 的 Netty 网络模块进行数据传输。
public class NettyShuffleDescriptor implements ShuffleDescriptor {

    private static final long serialVersionUID = 852181945034989215L;
    // 生产者位置 ID。
    // 生产该分区的 TaskExecutor（TaskManager）的唯一资源 ID。用于本地性检查和资源清理。
    private final ResourceID producerLocation;
    // 分区连接信息。
    // 包含用于建立网络连接的网络地址和连接索引。
    // 它是一个接口，有两个具体实现：
    // NetworkPartitionConnectionInfo（用于远程连接）和 LocalExecutionPartitionConnectionInfo（用于本地执行）。
    private final PartitionConnectionInfo partitionConnectionInfo;
    // 结果分区 ID。
    // 该描述符所指向的逻辑中间结果分区的唯一 ID。
    private final ResultPartitionID resultPartitionID;
    // 分层 Shuffle 描述符。
    // 可选属性，用于支持 Tiered Shuffle 架构。
    // 它包含指向数据可能存储在不同存储层（如内存、磁盘、外部存储）的描述符列表。
    @Nullable private final List<TierShuffleDescriptor> tierShuffleDescriptors;

    public NettyShuffleDescriptor(
            ResourceID producerLocation,
            PartitionConnectionInfo partitionConnectionInfo,
            ResultPartitionID resultPartitionID) {
        this(producerLocation, partitionConnectionInfo, resultPartitionID, null);
    }

    public NettyShuffleDescriptor(
            ResourceID producerLocation,
            PartitionConnectionInfo partitionConnectionInfo,
            ResultPartitionID resultPartitionID,
            @Nullable List<TierShuffleDescriptor> tierShuffleDescriptors) {
        this.producerLocation = producerLocation;
        this.partitionConnectionInfo = partitionConnectionInfo;
        this.resultPartitionID = resultPartitionID;
        this.tierShuffleDescriptors = tierShuffleDescriptors;
    }

    public ConnectionID getConnectionId() {
        return new ConnectionID(
                producerLocation,
                partitionConnectionInfo.getAddress(),
                partitionConnectionInfo.getConnectionIndex());
    }

    @Override
    public ResultPartitionID getResultPartitionID() {
        return resultPartitionID;
    }

    @Override
    public Optional<ResourceID> storesLocalResourcesOn() {
        return Optional.of(producerLocation);
    }

    public boolean isLocalTo(ResourceID consumerLocation) {
        return producerLocation.equals(consumerLocation);
    }

    @Nullable
    public List<TierShuffleDescriptor> getTierShuffleDescriptors() {
        return tierShuffleDescriptors;
    }

    /** Information for connection to partition producer for shuffle exchange. */
    public interface PartitionConnectionInfo extends Serializable {
        InetSocketAddress getAddress();

        int getConnectionIndex();
    }

    /**
     * Remote partition connection information with index to query partition.
     *
     * <p>Normal connection information with network address and port for connection in case of
     * distributed execution.
     */
    public static class NetworkPartitionConnectionInfo implements PartitionConnectionInfo {

        private static final long serialVersionUID = 5992534320110743746L;

        private final InetSocketAddress address;

        private final int connectionIndex;

        @VisibleForTesting
        public NetworkPartitionConnectionInfo(InetSocketAddress address, int connectionIndex) {
            this.address = address;
            this.connectionIndex = connectionIndex;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public int getConnectionIndex() {
            return connectionIndex;
        }

        static NetworkPartitionConnectionInfo fromProducerDescriptor(
                ProducerDescriptor producerDescriptor, int connectionIndex) {
            InetSocketAddress address =
                    new InetSocketAddress(
                            producerDescriptor.getAddress(), producerDescriptor.getDataPort());
            return new NetworkPartitionConnectionInfo(address, connectionIndex);
        }
    }

    /**
     * Local partition connection information.
     *
     * <p>Does not have any network connection information in case of local execution.
     */
    public enum LocalExecutionPartitionConnectionInfo implements PartitionConnectionInfo {
        INSTANCE;

        @Override
        public InetSocketAddress getAddress() {
            throw new UnsupportedOperationException(
                    "Local execution does not support shuffle connection.");
        }

        @Override
        public int getConnectionIndex() {
            throw new UnsupportedOperationException(
                    "Local execution does not support shuffle connection.");
        }
    }
}
