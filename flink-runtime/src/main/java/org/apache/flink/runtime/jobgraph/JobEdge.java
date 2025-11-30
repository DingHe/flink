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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represent edges (communication channels) in a job graph. The edges always go from an
 * intermediate result partition to a job vertex. An edge is parametrized with its {@link
 * DistributionPattern}.
 */
// JobEdge 类是 Flink JobGraph 中的基本连接单元，它代表了物理数据流的通道，连接着上游任务产生的中间数据集 (IntermediateDataSet) 和下游任务的目标顶点 (JobVertex)
// 物理数据通道： 它定义了数据从上游任务的输出（中间结果）流向下游任务的输入所遵循的具体网络通信规则。
// 数据分发模式： 它封装了 DistributionPattern（如 ALL_TO_ALL 或 POINTWISE），决定了上游任务的哪些子任务将数据发送给下游任务的哪些子任务。
// 动态弹性配置： 它包含了 SubtaskStateMapper，专门用于在任务发生扩缩容（Rescaling）时，指导如何重新映射和分配通道状态，确保数据通道的正确恢复。
// JobEdge 是 Flink 调度器和运行时网络栈用来理解和建立任务间数据传输路径的关键配置载体。

public class JobEdge implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /** The vertex connected to this edge.*/
    // 目标顶点。
    // 该边指向的下游任务组 (JobVertex)。
    private final JobVertex target;

    /** The distribution pattern that should be used for this job edge.*/
    // 数据分发模式。
    // 定义上游任务的输出分区如何连接到下游任务的输入实例。常见的模式包括：
    // POINTWISE (或 Forward): 一对一连接。
    // ALL_TO_ALL (或 Rebalance/Hash): 多对多连接。
    private final DistributionPattern distributionPattern;

    /** The channel rescaler that should be used for this job edge on downstream side.*/
    // 下游子任务状态映射器。
    // 在 Job 发生弹性扩缩容时，指导如何将上游持久化的通道状态（如批处理模式下的数据）正确地分配给新的下游子任务。默认是 ROUND_ROBIN。
    private SubtaskStateMapper downstreamSubtaskStateMapper = SubtaskStateMapper.ROUND_ROBIN;

    /** The channel rescaler that should be used for this job edge on upstream side. */
    // 上游子任务状态映射器。
    // 作用与下游映射器类似，但用于指导如何将状态分配给新的上游子任务。默认是 ROUND_ROBIN。
    private SubtaskStateMapper upstreamSubtaskStateMapper = SubtaskStateMapper.ROUND_ROBIN;

    /** The data set at the source of the edge, may be null if the edge is not yet connected.*/
    // 该边连接的上游任务产生的中间结果数据集。
    private final IntermediateDataSet source;

    /**
     * Optional name for the data shipping strategy (forward, partition hash, rebalance, ...), to be
     * displayed in the JSON plan.
     */
    // 数据传输策略名称。
    // 可选名称，用于描述数据传输的具体策略（如 "forward"、"partition hash"、"rebalance"、"broadcast"），
    // 主要用于 JSON 计划显示。
    private String shipStrategyName;
    // 是否为广播边。
    // 标识该边是否为广播连接，即上游的每个子任务都将数据发送给下游的所有子任务。
    private final boolean isBroadcast;
    // 是否为前传边 (Forward)。
    // 标识该边是否为点对点无重新分区的连接，通常用于链化后的内部数据流。
    private boolean isForward;

    /**
     * Optional name for the pre-processing operation (sort, combining sort, ...), to be displayed
     * in the JSON plan.
     */
    // 预处理操作名称。
    // 可选名称，描述在数据传输前可能发生的预处理操作（如 "sort"、"combining sort"），主要用于 JSON 计划显示。
    private String preProcessingOperationName;

    /** Optional description of the caching inside an operator, to be displayed in the JSON plan. */
    // 操作符级别缓存描述。
    // 可选描述，指明在该输入上可能存在的操作符级别缓存策略。
    private String operatorLevelCachingDescription;

    /**
     * Constructs a new job edge, that connects an intermediate result to a consumer task.
     *
     * @param source The data set that is at the source of this edge.
     * @param target The operation that is at the target of this edge.
     * @param distributionPattern The pattern that defines how the connection behaves in parallel.
     * @param isBroadcast Whether the source broadcasts data to the target.
     */
    public JobEdge(
            IntermediateDataSet source,
            JobVertex target,
            DistributionPattern distributionPattern,
            boolean isBroadcast) {
        if (source == null || target == null || distributionPattern == null) {
            throw new NullPointerException();
        }
        this.target = target;
        this.distributionPattern = distributionPattern;
        this.source = source;
        this.isBroadcast = isBroadcast;
    }

    /**
     * Returns the data set at the source of the edge. May be null, if the edge refers to the source
     * via an ID and has not been connected.
     *
     * @return The data set at the source of the edge
     */
    public IntermediateDataSet getSource() {
        return source;
    }

    /**
     * Returns the vertex connected to this edge.
     *
     * @return The vertex connected to this edge.
     */
    public JobVertex getTarget() {
        return target;
    }

    /**
     * Returns the distribution pattern used for this edge.
     *
     * @return The distribution pattern used for this edge.
     */
    public DistributionPattern getDistributionPattern() {
        return this.distributionPattern;
    }

    /**
     * Gets the ID of the consumed data set.
     *
     * @return The ID of the consumed data set.
     */
    public IntermediateDataSetID getSourceId() {
        return source.getId();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the name of the ship strategy for the represented input, like "forward", "partition
     * hash", "rebalance", "broadcast", ...
     *
     * @return The name of the ship strategy for the represented input, or null, if none was set.
     */
    public String getShipStrategyName() {
        return shipStrategyName;
    }

    /**
     * Sets the name of the ship strategy for the represented input.
     *
     * @param shipStrategyName The name of the ship strategy.
     */
    public void setShipStrategyName(String shipStrategyName) {
        this.shipStrategyName = shipStrategyName;
    }

    /** Gets whether the edge is broadcast edge. */
    public boolean isBroadcast() {
        return isBroadcast;
    }

    /** Gets whether the edge is forward edge. */
    public boolean isForward() {
        return isForward;
    }

    /** Sets whether the edge is forward edge. */
    public void setForward(boolean forward) {
        isForward = forward;
    }

    /**
     * Gets the channel state rescaler used for rescaling persisted data on downstream side of this
     * JobEdge.
     *
     * @return The channel state rescaler to use, or null, if none was set.
     */
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return downstreamSubtaskStateMapper;
    }

    /**
     * Sets the channel state rescaler used for rescaling persisted data on downstream side of this
     * JobEdge.
     *
     * @param downstreamSubtaskStateMapper The channel state rescaler selector to use.
     */
    public void setDownstreamSubtaskStateMapper(SubtaskStateMapper downstreamSubtaskStateMapper) {
        this.downstreamSubtaskStateMapper = checkNotNull(downstreamSubtaskStateMapper);
    }

    /**
     * Gets the channel state rescaler used for rescaling persisted data on upstream side of this
     * JobEdge.
     *
     * @return The channel state rescaler to use, or null, if none was set.
     */
    public SubtaskStateMapper getUpstreamSubtaskStateMapper() {
        return upstreamSubtaskStateMapper;
    }

    /**
     * Sets the channel state rescaler used for rescaling persisted data on upstream side of this
     * JobEdge.
     *
     * @param upstreamSubtaskStateMapper The channel state rescaler selector to use.
     */
    public void setUpstreamSubtaskStateMapper(SubtaskStateMapper upstreamSubtaskStateMapper) {
        this.upstreamSubtaskStateMapper = checkNotNull(upstreamSubtaskStateMapper);
    }

    /**
     * Gets the name of the pro-processing operation for this input.
     *
     * @return The name of the pro-processing operation, or null, if none was set.
     */
    public String getPreProcessingOperationName() {
        return preProcessingOperationName;
    }

    /**
     * Sets the name of the pre-processing operation for this input.
     *
     * @param preProcessingOperationName The name of the pre-processing operation.
     */
    public void setPreProcessingOperationName(String preProcessingOperationName) {
        this.preProcessingOperationName = preProcessingOperationName;
    }

    /**
     * Gets the operator-level caching description for this input.
     *
     * @return The description of operator-level caching, or null, is none was set.
     */
    public String getOperatorLevelCachingDescription() {
        return operatorLevelCachingDescription;
    }

    /**
     * Sets the operator-level caching description for this input.
     *
     * @param operatorLevelCachingDescription The description of operator-level caching.
     */
    public void setOperatorLevelCachingDescription(String operatorLevelCachingDescription) {
        this.operatorLevelCachingDescription = operatorLevelCachingDescription;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("%s --> %s [%s]", source.getId(), target, distributionPattern.name());
    }
}
