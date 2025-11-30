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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partition producer descriptor for {@link ShuffleMaster} to obtain {@link ShuffleDescriptor}.
 *
 * <p>The producer descriptor contains general producer specific information relevant for the
 * shuffle service: the producer location as {@link ResourceID}, {@link ExecutionAttemptID} and the
 * network connection information for shuffle data exchange (address and port).
 */
// ProducerDescriptor 类是 Flink 中用于描述中间结果分区生产者 (Producer) 身份和连接信息的数据结构。
// 在 Flink 的 Shuffle 机制中，当一个上游任务（生产者）准备写入数据分区时，它需要向 ShuffleMaster 注册自己的存在。ProducerDescriptor 就是在注册过程中，将生产者任务的位置、身份和网络连接详情告知 ShuffleMaster 的载体。
// 生产者身份标识： 唯一地标识了负责产生某个中间结果分区的任务实例（即 ExecutionAttempt）。
// 网络连接信息： 提供了下游消费者任务通过网络连接到生产者所在 TaskManager 获取数据所需的所有地址信息（IP 和端口）。
// Shuffle 注册： 作为参数传递给 ShuffleMaster 的 registerPartitionWithProducer 方法，是获取最终 ShuffleDescriptor 的前提条件。
public class ProducerDescriptor {
    /** The resource ID to identify the container where the producer execution is deployed. */
    // 生产者位置 ID。
    // 生产该分区的 TaskExecutor（TaskManager）的唯一资源 ID。用于识别 TaskManager 容器。
    private final ResourceID producerLocation;

    /** The ID of the producer execution attempt. */
    // 生产者执行尝试 ID。
    // 唯一标识该生产者任务的具体执行尝试（ExecutionAttempt）。
    private final ExecutionAttemptID producerExecutionId;

    /** The address to connect to the producer. */
    // 连接地址。
    // 生产任务所在 TaskManager 的 IP 地址。
    private final InetAddress address;

    /**
     * The port to connect to the producer for shuffle exchange.
     *
     * <p>Negative value means local execution.
     */
    // 数据端口。
    // 生产任务所在 TaskManager 上用于 Shuffle 数据交换的端口号。
    // 如果值为负数，表示这是本地执行，通常意味着不需要网络连接。
    private final int dataPort;

    @VisibleForTesting
    public ProducerDescriptor(
            ResourceID producerLocation,
            ExecutionAttemptID producerExecutionId,
            InetAddress address,
            int dataPort) {
        this.producerLocation = checkNotNull(producerLocation);
        this.producerExecutionId = checkNotNull(producerExecutionId);
        this.address = checkNotNull(address);
        this.dataPort = dataPort;
    }

    public ResourceID getProducerLocation() {
        return producerLocation;
    }

    public ExecutionAttemptID getProducerExecutionId() {
        return producerExecutionId;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getDataPort() {
        return dataPort;
    }

    public static ProducerDescriptor create(
            TaskManagerLocation producerLocation, ExecutionAttemptID attemptId) {
        return new ProducerDescriptor(
                producerLocation.getResourceID(),
                attemptId,
                producerLocation.address(),
                producerLocation.dataPort());
    }
}
