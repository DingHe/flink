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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a result partition.
 *
 * @see ResultPartition
 */
// Flink 运行时（Runtime）内部使用的部署描述符（Deployment Descriptor）
// 部署数据描述： 在 JobManager 将一个 Task 部署到 TaskManager 时，它携带了关于该 Task **输出数据（Result Partition）**所需的所有配置和标识信息。
// 指导 TaskManager 创建分区： TaskManager 接收到这个描述符后，就知道应该如何配置和创建一个 ResultPartition 实例，包括它的类型（如 PIPELINED 或 BLOCKING）、ID 以及如何与下游 Task 进行数据交换（Shuffle）。


public class ResultPartitionDeploymentDescriptor implements Serializable {

    private static final long serialVersionUID = 6343547936086963705L;
    // 分区逻辑描述符。
    // 包含了分区本身的逻辑属性和标识符，例如中间数据集 ID (IntermediateDataSetID)、分区类型 (ResultPartitionType)、是否为广播分区等。
    private final PartitionDescriptor partitionDescriptor;
    // 数据交换描述符。
    // 包含了分区在网络层或存储层的物理信息和配置，用于指导下游消费者 Task 如何连接到这个分区以获取数据。这通常由 Shuffle Service 负责处理。
    private final ShuffleDescriptor shuffleDescriptor;
    // 最大并行度。表示 Job 中整个 IntermediateDataSet（中间数据集）的最大并行度。
    // 该值主要用于状态管理中的 KeyGroup 范围划分，与当前 Task 的并行度无关，但与 Key 相关的操作有关。
    private final int maxParallelism;

    public ResultPartitionDeploymentDescriptor(
            PartitionDescriptor partitionDescriptor,
            ShuffleDescriptor shuffleDescriptor,
            int maxParallelism) {
        this.partitionDescriptor = checkNotNull(partitionDescriptor);
        this.shuffleDescriptor = checkNotNull(shuffleDescriptor);
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        this.maxParallelism = maxParallelism;
    }

    public IntermediateDataSetID getResultId() {
        return partitionDescriptor.getResultId();
    }

    public IntermediateResultPartitionID getPartitionId() {
        return partitionDescriptor.getPartitionId();
    }

    /** Whether the resultPartition is a broadcast edge. */
    public boolean isBroadcast() {
        return partitionDescriptor.isBroadcast();
    }

    public ResultPartitionType getPartitionType() {
        return partitionDescriptor.getPartitionType();
    }

    public int getTotalNumberOfPartitions() {
        return partitionDescriptor.getTotalNumberOfPartitions();
    }

    public int getNumberOfSubpartitions() {
        return partitionDescriptor.getNumberOfSubpartitions();
    }

    public boolean isNumberOfPartitionConsumerUndefined() {
        return partitionDescriptor.isNumberOfPartitionConsumerUndefined();
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public ShuffleDescriptor getShuffleDescriptor() {
        return shuffleDescriptor;
    }

    @Override
    public String toString() {
        return String.format(
                "ResultPartitionDeploymentDescriptor [PartitionDescriptor: %s, "
                        + "ShuffleDescriptor: %s]",
                partitionDescriptor, shuffleDescriptor);
    }
}
