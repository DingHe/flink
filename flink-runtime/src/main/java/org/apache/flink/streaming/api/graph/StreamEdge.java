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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An edge in the streaming topology. One edge like this does not necessarily gets converted to a
 * connection between two job vertices (due to chaining/optimization).
 */
// StreamEdge 类是 Flink 流式拓扑图（StreamGraph）中的基本构建块，代表了两个 StreamNode（操作符/算子）之间的数据流连接。
// 逻辑连接表示： 它定义了数据如何从一个操作符（Source Node）流向另一个操作符（Target Node）。
// 数据传输配置： 它封装了数据传输所需的所有关键配置，包括数据分区策略（如 Keyed, Shuffle）、网络交换模式（如 Eager, Batch）以及缓冲区超时。
// 支持复杂拓扑： 它支持多输入（通过 typeNumber）、侧输出（通过 OutputTag）以及未对齐检查点等高级特性。
// 重要说明： 一个 StreamEdge 只是一个逻辑连接。在 Flink 运行时优化（例如操作符链化/Chaining）之后，这个逻辑边不一定会转化为两个最终 Job Vertex 之间的物理网络连接。

@Internal
public class StreamEdge implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long ALWAYS_FLUSH_BUFFER_TIMEOUT = 0L;
    // 边的唯一标识符。
    // 由 Source Node ID、Target Node ID、类型编号、分区器和唯一 ID 共同组成，用于唯一标识这条边。
    private final String edgeId;
    // 数据流出的上游 StreamNode 的 ID。
    private final int sourceId;
    // 数据流入的下游 StreamNode 的 ID。
    private final int targetId;

    /**
     * Note that this field doesn't have to be unique among all {@link StreamEdge}s. It's enough if
     * this field ensures that all logical instances of {@link StreamEdge} are unique, and {@link
     * #hashCode()} are different and {@link #equals(Object)} returns false, for every possible pair
     * of {@link StreamEdge}. Especially among two different {@link StreamEdge}s that are connecting
     * the same pair of nodes.
     */
    // 唯一 ID。
    // 用于区分连接相同 Source 和 Target 节点的不同逻辑边（例如，一个 Co-Process 算子的两个输入边）。
    private final int uniqueId;

    /** The type number of the input for co-tasks. */
    // 输入类型编号。
    // 如果目标节点是多输入算子（如 CoProcessFunction），此编号（通常从 0 开始）标识这条边是哪个输入（例如，输入 0 或输入 1）
    private final int typeNumber;
    /** The side-output tag (if any) of this {@link StreamEdge}. */
    // 侧输出标签。
    // 如果这条边是从 Source 节点的**侧输出（Side Output）**引出的，则包含该侧输出的标签。如果不是侧输出，则为 null。
    private final OutputTag outputTag;

    /** The {@link StreamPartitioner} on this {@link StreamEdge}. */
    // 流分区器。
    // 决定数据从 Source 的并行实例发送到 Target 的并行实例的方式（如 KeyGroupStreamPartitioner, RebalancePartitioner）。
    private StreamPartitioner<?> outputPartitioner;

    /** The name of the operator in the source vertex. */
    // 源操作符名称。
    // 源 StreamNode 所包含的操作符的名称，用于日志和可视化。
    private final String sourceOperatorName;

    /** The name of the operator in the target vertex. */
    // 目标操作符名称。
    // 目标 StreamNode 所包含的操作符的名称，用于日志和可视化。
    private final String targetOperatorName;
    // 流交换模式。
    // 定义数据在网络中交换的方式（如 BATCH、PIPELINED、UNDEFINED），影响数据传输的延迟和吞吐量。
    private final StreamExchangeMode exchangeMode;
    // 缓冲区超时。
    // 设置数据在发送前在缓冲区中等待的最长时间（毫秒）。
    // 设置为 0 表示立即刷新。
    private long bufferTimeout;

    // 标志这条边是否支持未对齐检查点（Unaligned Checkpoints）。
    // 如果为 true，则可以跳过严格的屏障对齐，以减少检查点延迟。
    private boolean supportsUnalignedCheckpoints = true;
    // 中间数据集 ID。
    // 当这条边在运行时形成一个物理数据传输时，它所对应的数据集在 JobGraph 中的唯一标识。
    private final IntermediateDataSetID intermediateDatasetIdToProduce;

    public StreamEdge(
            StreamNode sourceVertex,
            StreamNode targetVertex,
            int typeNumber,
            StreamPartitioner<?> outputPartitioner,
            OutputTag outputTag) {

        this(
                sourceVertex,
                targetVertex,
                typeNumber,
                ALWAYS_FLUSH_BUFFER_TIMEOUT,
                outputPartitioner,
                outputTag,
                StreamExchangeMode.UNDEFINED,
                0,
                null);
    }

    public StreamEdge(
            StreamNode sourceVertex,
            StreamNode targetVertex,
            int typeNumber,
            StreamPartitioner<?> outputPartitioner,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            int uniqueId,
            IntermediateDataSetID intermediateDatasetId) {

        this(
                sourceVertex,
                targetVertex,
                typeNumber,
                sourceVertex.getBufferTimeout(),
                outputPartitioner,
                outputTag,
                exchangeMode,
                uniqueId,
                intermediateDatasetId);
    }

    public StreamEdge(
            StreamNode sourceVertex,
            StreamNode targetVertex,
            int typeNumber,
            long bufferTimeout,
            StreamPartitioner<?> outputPartitioner,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            int uniqueId,
            IntermediateDataSetID intermediateDatasetId) {

        this.sourceId = sourceVertex.getId();
        this.targetId = targetVertex.getId();
        this.uniqueId = uniqueId;
        this.typeNumber = typeNumber;
        this.bufferTimeout = bufferTimeout;
        this.outputPartitioner = outputPartitioner;
        this.outputTag = outputTag;
        this.sourceOperatorName = sourceVertex.getOperatorName();
        this.targetOperatorName = targetVertex.getOperatorName();
        this.exchangeMode = checkNotNull(exchangeMode);
        this.intermediateDatasetIdToProduce = intermediateDatasetId;
        this.edgeId =
                sourceVertex
                        + "_"
                        + targetVertex
                        + "_"
                        + typeNumber
                        + "_"
                        + outputPartitioner
                        + "_"
                        + uniqueId;
    }

    public int getSourceId() {
        return sourceId;
    }

    public int getTargetId() {
        return targetId;
    }

    public int getTypeNumber() {
        return typeNumber;
    }

    public OutputTag getOutputTag() {
        return this.outputTag;
    }

    public StreamPartitioner<?> getPartitioner() {
        return outputPartitioner;
    }

    public StreamExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    public void setPartitioner(StreamPartitioner<?> partitioner) {
        this.outputPartitioner = partitioner;
    }

    public void setBufferTimeout(long bufferTimeout) {
        checkArgument(bufferTimeout >= -1);
        this.bufferTimeout = bufferTimeout;
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public void setSupportsUnalignedCheckpoints(boolean supportsUnalignedCheckpoints) {
        this.supportsUnalignedCheckpoints = supportsUnalignedCheckpoints;
    }

    public boolean supportsUnalignedCheckpoints() {
        return supportsUnalignedCheckpoints;
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, outputTag);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamEdge that = (StreamEdge) o;
        return Objects.equals(edgeId, that.edgeId) && Objects.equals(outputTag, that.outputTag);
    }

    @Override
    public String toString() {
        return "("
                + (sourceOperatorName + "-" + sourceId)
                + " -> "
                + (targetOperatorName + "-" + targetId)
                + ", typeNumber="
                + typeNumber
                + ", outputPartitioner="
                + outputPartitioner
                + ", exchangeMode="
                + exchangeMode
                + ", bufferTimeout="
                + bufferTimeout
                + ", outputTag="
                + outputTag
                + ", uniqueId="
                + uniqueId
                + ')';
    }

    public IntermediateDataSetID getIntermediateDatasetIdToProduce() {
        return intermediateDatasetIdToProduce;
    }
}
