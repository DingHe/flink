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
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Partition descriptor for {@link ShuffleMaster} to obtain {@link ShuffleDescriptor}. */
// PartitionDescriptor 类是 Flink 中用于描述中间结果分区 (IntermediateResultPartition) 核心元数据的数据结构。
// 它的主要作用是在 Shuffle 过程中，作为任务的生产者 (Producer) 将其产出结果的详细信息提供给 ShuffleMaster。
// Shuffle 元数据传输： 它是 JobMaster 与 ShuffleMaster (或外部 Shuffle 服务) 交互的关键载体。当任务准备部署时，JobMaster 需要向 ShuffleMaster 注册该任务将产生的中间结果分区。
// PartitionDescriptor 就包含了 ShuffleMaster 注册和管理该分区所需的所有信息。
// 结果分区标识： 完整地标识了某个任务的某个输出数据流的一个并行子分区。
// Shuffle 配置说明： 提供了关于该分区的类型 (PIPELINED 或 BLOCKING)、连接索引、广播特性以及分发模式等关键信息，指导 ShuffleMaster 如何处理和存储这些数据。
public class PartitionDescriptor implements Serializable {

    private static final long serialVersionUID = 6343547936086963705L;

    /** The ID of the result this partition belongs to. */
    // 结果数据集 ID。
    // 该分区所属的整个中间结果数据集的唯一 ID。一个数据集包含多个分区。
    private final IntermediateDataSetID resultId;

    /** The total number of partitions for the result. */
    // 总分区数。
    // 该分区所属的整个中间结果数据集的并行度（即总共有多少个 IntermediateResultPartition）。
    private final int totalNumberOfPartitions;

    /** The ID of the partition. */
    // 分区 ID。
    // 该单个中间结果分区的唯一 ID。
    private final IntermediateResultPartitionID partitionId;

    /** The type of the partition. */
    // 分区类型。
    // 中间结果分区的类型，例如 PIPELINED (流式/无界) 或 BLOCKING (阻塞/批处理)。这决定了数据如何被传输和持久化。
    private final ResultPartitionType partitionType;

    /** The number of subpartitions. */
    // 子分区数量。
    // 该分区内部的子分区数量。
    // 在 Shuffle 传输中，子分区通常对应于下游任务的并行度，决定了数据如何路由。
    private final int numberOfSubpartitions;

    /** Connection index to identify this partition of intermediate result. */
    // 连接索引。
    // 标识该中间结果的连接索引，用于在运行时区分同一任务产出的不同中间结果。
    private final int connectionIndex;

    /** Whether the intermediate result is a broadcast result. */
    // 标识该中间结果是否为广播 (Broadcast) 类型，即数据是否发送给下游所有消费者任务。
    private final boolean isBroadcast;

    /**
     * Whether the distribution pattern of the intermediate result is {@link
     * DistributionPattern.ALL_TO_ALL}.
     */
    // 是否为全连接分发模式。
    // 标识下游消费者任务的连接模式是否为 ALL_TO_ALL（全连接），
    // 即每个生产分区的数据都可能被所有消费分区读取。
    private final boolean isAllToAllDistribution;
    // 标识该分区的消费者并行度是否可以在任务部署时确定。如果为 true，通常表示这是一个动态确定的输出。
    private final boolean isNumberOfPartitionConsumerUndefined;

    @VisibleForTesting
    public PartitionDescriptor(
            IntermediateDataSetID resultId,
            int totalNumberOfPartitions,
            IntermediateResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numberOfSubpartitions,
            int connectionIndex,
            boolean isBroadcast,
            boolean isAllToAllDistribution,
            boolean isNumberOfPartitionConsumerUndefined) {
        this.resultId = checkNotNull(resultId);
        checkArgument(totalNumberOfPartitions >= 1);
        this.totalNumberOfPartitions = totalNumberOfPartitions;
        this.partitionId = checkNotNull(partitionId);
        this.partitionType = checkNotNull(partitionType);
        checkArgument(numberOfSubpartitions >= 1);
        this.numberOfSubpartitions = numberOfSubpartitions;
        this.connectionIndex = connectionIndex;
        this.isBroadcast = isBroadcast;
        this.isAllToAllDistribution = isAllToAllDistribution;
        this.isNumberOfPartitionConsumerUndefined = isNumberOfPartitionConsumerUndefined;
    }

    public IntermediateDataSetID getResultId() {
        return resultId;
    }

    public int getTotalNumberOfPartitions() {
        return totalNumberOfPartitions;
    }

    public IntermediateResultPartitionID getPartitionId() {
        return partitionId;
    }

    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    public int getNumberOfSubpartitions() {
        return numberOfSubpartitions;
    }

    public boolean isNumberOfPartitionConsumerUndefined() {
        return isNumberOfPartitionConsumerUndefined;
    }

    int getConnectionIndex() {
        return connectionIndex;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public boolean isAllToAllDistribution() {
        return isAllToAllDistribution;
    }

    @Override
    public String toString() {
        return String.format(
                "PartitionDescriptor [result id: %s, partition id: %s, partition type: %s, "
                        + "subpartitions: %d, connection index: %d, is broadcast: %s, "
                        + "is all-to-all distribution: %s]",
                resultId,
                partitionId,
                partitionType,
                numberOfSubpartitions,
                connectionIndex,
                isBroadcast,
                isAllToAllDistribution);
    }
    // 从 IntermediateResultPartition 创建。
    // 用于从 Flink 的运行时表示 IntermediateResultPartition 对象中提取所有必要信息，创建一个新的 PartitionDescriptor 实例
    public static PartitionDescriptor from(IntermediateResultPartition partition) {
        checkNotNull(partition);

        IntermediateResult result = partition.getIntermediateResult();
        return new PartitionDescriptor(
                result.getId(),
                partition.getIntermediateResult().getNumberOfAssignedPartitions(),
                partition.getPartitionId(),
                result.getResultType(),
                partition.getNumberOfSubpartitions(),
                result.getConnectionIndex(),
                result.isBroadcast(),
                result.getConsumingDistributionPattern() == DistributionPattern.ALL_TO_ALL,
                partition.isNumberOfPartitionConsumersUndefined());
    }
}
