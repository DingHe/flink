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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Interface for the implementation of shuffle service local environment.
 *
 * <p>Input/Output interface of local shuffle service environment is based on memory {@link Buffer
 * Buffers}. A producer can write shuffle data into the buffers, obtained from the created {@link
 * ResultPartitionWriter ResultPartitionWriters} and a consumer reads the buffers from the created
 * {@link InputGate InputGates}.
 *
 * <h1>Lifecycle management.</h1>
 *
 * <p>The interface contains method's to manage the lifecycle of the local shuffle service
 * environment:
 *
 * <ol>
 *   <li>{@link ShuffleEnvironment#start} must be called before using the shuffle service
 *       environment.
 *   <li>{@link ShuffleEnvironment#close} is called to release the shuffle service environment.
 * </ol>
 *
 * <h1>Shuffle Input/Output management.</h1>
 *
 * <h2>Result partition management.</h2>
 *
 * <p>The interface implements a factory of result partition writers to produce shuffle data: {@link
 * ShuffleEnvironment#createResultPartitionWriters}. The created writers are grouped per owner. The
 * owner is responsible for the writers' lifecycle from the moment of creation.
 *
 * <p>Partitions are fully released in the following cases:
 *
 * <ol>
 *   <li>{@link ResultPartitionWriter#fail(Throwable)} and {@link ResultPartitionWriter#close()} are
 *       called if the production has failed.
 *   <li>for PIPELINED partitions if there was a detected consumption attempt and it either failed
 *       or finished after the bounded production has been done ({@link
 *       ResultPartitionWriter#finish()} and {@link ResultPartitionWriter#close()} have been
 *       called). Only one consumption attempt is ever expected for the PIPELINED partition at the
 *       moment so it can be released afterwards.
 *   <li>if the following methods are called outside of the producer thread:
 *       <ol>
 *         <li>{@link ShuffleMaster#releasePartitionExternally(ShuffleDescriptor)}
 *         <li>and if it occupies any producer local resources ({@link
 *             ShuffleDescriptor#storesLocalResourcesOn()}) then also {@link
 *             ShuffleEnvironment#releasePartitionsLocally(Collection)}
 *       </ol>
 *       e.g. to manage the lifecycle of BLOCKING result partitions which can outlive their
 *       producers. The BLOCKING partitions can be consumed multiple times.
 * </ol>
 *
 * <p>The partitions, which currently still occupy local resources, can be queried with {@link
 * ShuffleEnvironment#getPartitionsOccupyingLocalResources}.
 *
 * <h2>Input gate management.</h2>
 *
 * <p>The interface implements a factory for the input gates: {@link
 * ShuffleEnvironment#createInputGates}. The created gates are grouped per owner. The owner is
 * responsible for the gates' lifecycle from the moment of creation.
 *
 * <p>When the input gates are created, it can happen that not all consumed partitions are known at
 * that moment e.g. because their producers have not been started yet. Therefore, the {@link
 * ShuffleEnvironment} provides a method {@link ShuffleEnvironment#updatePartitionInfo} to update
 * them externally, when the producer becomes known. The update mechanism has to be threadsafe
 * because the updated gate can be read concurrently from a different thread.
 *
 * @param <P> type of provided result partition writers
 * @param <G> type of provided input gates
 */
// 定义了 TaskManager（任务执行器）本地 Shuffle 服务的运行环境。
// 如果说 JobTable 负责管理作业的逻辑，那么 ShuffleEnvironment 则负责管理作业的 数据传输（IO）。
// 它屏蔽了底层数据传输的具体实现（是基于内存、本地磁盘，还是远程存储）
// ShuffleEnvironment 的主要作用是管理任务之间数据交换的生命周期和资源。
// 数据写出控制：作为工厂类，为生产者任务创建 ResultPartitionWriter，将计算结果写入缓冲区。
// 数据读取控制：为消费者任务创建 InputGate，从其他节点或本地读取数据。
// 生命周期管理：负责 Shuffle 服务的启动、关闭以及中间结果数据（Partitions）的清理。
// 资源抽象：支持插件化 Shuffle 服务。Flink 默认实现是 NettyShuffleEnvironment，但也可以扩展为外部 Shuffle 服务（如 Remote Shuffle Service）。
// P extends ResultPartitionWriter: 代表该环境产生的生产者组件类型。
// G extends IndexedInputGate: 代表该环境产生的消费者组件类型。
public interface ShuffleEnvironment<P extends ResultPartitionWriter, G extends IndexedInputGate>
        extends AutoCloseable {

    /**
     * Start the internal related services before using the shuffle service environment.
     *
     * @return a port to connect for the shuffle data exchange, -1 if only local connection is
     *     possible.
     */
    // 启动 Shuffle 环境内部的相关服务（如 Netty 服务器、内存池等）
    int start() throws IOException;

    /**
     * Create a context of the shuffle input/output owner used to create partitions or gates
     * belonging to the owner.
     *
     * <p>This method has to be called only once to avoid duplicated internal metric group
     * registration.
     *
     * @param ownerName the owner name, used for logs
     * @param executionAttemptID execution attempt id of the producer or consumer
     * @param parentGroup parent of shuffle specific metric group
     * @return context of the shuffle input/output owner used to create partitions or gates
     *     belonging to the owner
     */
    // 为特定的 Task 创建一个“IO 所有者上下文”。
    // 包含所有者名称、执行尝试 ID（ExecutionAttemptID）和度量组。
    // 确保每个 Task 的 Shuffle 资源能够被正确地追踪和度量。
    ShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup);

    /**
     * Factory method for the {@link ResultPartitionWriter ResultPartitionWriters} to produce result
     * partitions.
     *
     * <p>The order of the {@link ResultPartitionWriter ResultPartitionWriters} in the returned
     * collection should be the same as the iteration order of the passed {@code
     * resultPartitionDeploymentDescriptors}.
     *
     * @param ownerContext the owner context relevant for partition creation
     * @param resultPartitionDeploymentDescriptors descriptors of the partition, produced by the
     *     owner
     * @return list of the {@link ResultPartitionWriter ResultPartitionWriters}
     */
    // 生产者工厂方法
    // 根据部署描述符创建一组 ResultPartitionWriter
    List<P> createResultPartitionWriters(
            ShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors);

    /**
     * Release local resources occupied by the given partitions.
     *
     * <p>This is called for partitions which occupy resources locally (can be checked by {@link
     * ShuffleDescriptor#storesLocalResourcesOn()}).
     *
     * @param partitionIds identifying the partitions to be released
     */
    // 手动释放指定的本地分区资源。
    void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds);

    /**
     * Report partitions which still occupy some resources locally.
     *
     * @return collection of partitions which still occupy some resources locally and have not been
     *     released yet.
     */
    // 查询当前哪些分区还在占用本地资源（如内存缓冲区或本地磁盘文件）
    Collection<ResultPartitionID> getPartitionsOccupyingLocalResources();

    /**
     * Get metrics of the partition if it still occupies some resources locally and have not been
     * released yet.
     *
     * @param partitionId the partition id
     * @return An Optional of {@link ShuffleMetrics}, if found, of the given partition
     */
    default Optional<ShuffleMetrics> getMetricsIfPartitionOccupyingLocalResource(
            ResultPartitionID partitionId) {
        return Optional.empty();
    }

    /**
     * Factory method for the {@link InputGate InputGates} to consume result partitions.
     *
     * <p>The order of the {@link InputGate InputGates} in the returned collection should be the
     * same as the iteration order of the passed {@code inputGateDeploymentDescriptors}.
     *
     * @param ownerContext the owner context relevant for gate creation
     * @param partitionProducerStateProvider producer state provider to query whether the producer
     *     is ready for consumption
     * @param inputGateDeploymentDescriptors descriptors of the input gates to consume
     * @return list of the {@link InputGate InputGates}
     */
    // 消费者工厂方法。根据描述符创建 InputGate
    // Task 启动时，通过此方法建立读取数据的“入口”。
    List<G> createInputGates(
            ShuffleIOOwnerContext ownerContext,
            PartitionProducerStateProvider partitionProducerStateProvider,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors);

    /**
     * Update a gate with the newly available partition information, previously unknown.
     *
     * @param consumerID execution id to distinguish gates with the same id from the different
     *     consumer executions
     * @param partitionInfo information needed to consume the updated partition, e.g. network
     *     location
     * @return {@code true} if the partition has been updated or {@code false} if the partition is
     *     not available anymore.
     * @throws IOException IO problem by the update
     * @throws InterruptedException potentially blocking operation was interrupted
     */
    // 动态更新分区信息。
    // 由于 Flink 任务是并行启动的，有时消费者启动时，生产者的位置（IP/端口）还不知道。
    // 当生产者地址确定后，JobManager 会通知消费者，消费者通过此方法更新连接信息。
    boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo)
            throws IOException, InterruptedException;
}
