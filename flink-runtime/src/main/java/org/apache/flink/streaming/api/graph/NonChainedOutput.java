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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

/**
 * Used by operator chain and represents a non-chained output of the corresponding stream operator.
 */
// 在 Flink 中，为了提高效率，通常会将多个算子串联（Chaining）起来，在同一个线程中执行，形成一个算子链（Operator Chain）。
// 当数据从算子链中的一个算子流向另一个算子链或未串联的单个算子时，这种连接就形成了非串联输出（Non-Chained Output）
// NonChainedOutput 类的作用就是封装描述这种非串联数据流所需的所有元数据。
// 这些元数据定义了数据如何从当前的算子链离开，通过网络或内存传输到下游任务的输入。
@Internal
public class NonChainedOutput implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Is unaligned checkpoint supported. */
    // 支持非对齐检查点。标记这个输出是否支持非对齐检查点（Unaligned Checkpoints）。
    // 如果支持，数据传输缓冲区中的数据也会作为检查点状态的一部分被保存。
    private final boolean supportsUnalignedCheckpoints;

    /** ID of the producer {@link StreamNode}. */
    // 生产者节点 ID。
    // 该输出所连接的源头 StreamNode 的唯一 ID。
    private final int sourceNodeId;

    /** Parallelism of the consumer vertex. */
    // 消费者并行度。
    // 下游接收数据的任务或算子的并行度。
    private final int consumerParallelism;

    /** Max parallelism of the consumer vertex. */
    // 消费者最大并行度。下游任务允许的最大并行度
    private final int consumerMaxParallelism;

    /** Buffer flush timeout of this output. */
    // 缓冲区刷新超时。
    // 数据在网络缓冲区中等待的最大时间。
    // 这个值影响流任务的延迟和吞吐量。
    private final long bufferTimeout;

    /** ID of the produced intermediate dataset. */
    // 中间数据集 ID。
    // 这个输出所产生的中间数据集（即网络数据流）的唯一标识符。
    // 这是下游任务连接输入时所依赖的关键 ID。
    private final IntermediateDataSetID dataSetId;

    /** Whether this intermediate dataset is a persistent dataset or not. */
    // 是否为持久化数据集。
    // 标记这个数据集是否为持久化的（例如，在批处理或特定场景下，数据可能会被写入存储以供后续作业重用）
    private final boolean isPersistentDataSet;

    /** The side-output tag (if any). */
    // 侧输出标签。
    // 如果这个输出是通过侧输出（Side Output）产生的，则包含该侧输出流的标签。如果不是侧输出，则为 null。
    private final OutputTag<?> outputTag;

    /** The corresponding data partitioner. */
    // 数据分区器。
    // 定义数据记录从当前任务发送到下游任务时，如何分发到下游并行实例（如 KeyGroupStreamPartitioner、RebalancePartitioner 等）
    private StreamPartitioner<?> partitioner;

    /** Target {@link ResultPartitionType}. */
    // 结果分区类型。
    // 定义数据流的底层网络传输类型（如 PIPELINED (流式)、BLOCKING (阻塞式) 等）
    private ResultPartitionType partitionType;

    public NonChainedOutput(
            boolean supportsUnalignedCheckpoints,
            int sourceNodeId,
            int consumerParallelism,
            int consumerMaxParallelism,
            long bufferTimeout,
            boolean isPersistentDataSet,
            IntermediateDataSetID dataSetId,
            OutputTag<?> outputTag,
            StreamPartitioner<?> partitioner,
            ResultPartitionType partitionType) {
        this.supportsUnalignedCheckpoints = supportsUnalignedCheckpoints;
        this.sourceNodeId = sourceNodeId;
        this.consumerParallelism = consumerParallelism;
        this.consumerMaxParallelism = consumerMaxParallelism;
        this.bufferTimeout = bufferTimeout;
        this.isPersistentDataSet = isPersistentDataSet;
        this.dataSetId = dataSetId;
        this.outputTag = outputTag;
        this.partitioner = partitioner;
        this.partitionType = partitionType;
    }

    public boolean supportsUnalignedCheckpoints() {
        return supportsUnalignedCheckpoints;
    }

    public int getSourceNodeId() {
        return sourceNodeId;
    }

    public int getConsumerParallelism() {
        return consumerParallelism;
    }

    public int getConsumerMaxParallelism() {
        return consumerMaxParallelism;
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public IntermediateDataSetID getDataSetId() {
        return dataSetId;
    }

    public IntermediateDataSetID getPersistentDataSetId() {
        return isPersistentDataSet ? dataSetId : null;
    }

    public OutputTag<?> getOutputTag() {
        return outputTag;
    }

    public void setPartitioner(StreamPartitioner<?> partitioner) {
        this.partitioner = partitioner;
    }

    public void setPartitionType(ResultPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public StreamPartitioner<?> getPartitioner() {
        return partitioner;
    }

    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NonChainedOutput output = (NonChainedOutput) o;
        return Objects.equals(dataSetId, output.dataSetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSetId);
    }
}
