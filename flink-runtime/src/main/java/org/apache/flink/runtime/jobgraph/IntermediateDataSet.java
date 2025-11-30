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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An intermediate data set is the data set produced by an operator - either a source or any
 * intermediate operation.
 * <p>Intermediate data sets may be read by other operators, materialized, or discarded.
 */
// IntermediateDataSet 类是 Flink JobGraph 中的一个重要概念，它代表了一个 JobVertex 的全部输出结果。
// 它本身并不包含实际的数据，而是扮演着元数据描述符的角色。
// 数据结果抽象： 它是上游任务 (JobVertex) 完成计算后所产生数据的逻辑表示。
// 生产者-消费者链接： 它明确地定义了数据的生产者（producer 属性）以及所有消费者（通过 consumers 列表中的 JobEdge）。
// 传输属性封装： 它封装了数据在网络传输层的关键属性，如分区类型 (ResultPartitionType) 和数据分发模式 (DistributionPattern)，这些信息对 Flink 运行时建立数据通道至关重要。
// IntermediateDataSet 是 JobGraph 中一个数据流的起点（输出端），它描述了数据来自哪个任务、数据的物理存储/传输特性，以及数据流向了哪些下游任务。

public class IntermediateDataSet implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    // 数据集唯一 ID。
    // 用于在整个 Flink Job 中唯一标识该中间数据集。
    private final IntermediateDataSetID id; // the identifier
    // 生产者任务。
    // 指向生成该数据集的 JobVertex（上游操作）。它定义了数据的来源。
    private final JobVertex producer; // the operation that produced this data set

    // All consumers must have the same partitioner and parallelism
    // 消费者边列表。
    // 包含连接到该数据集的所有 JobEdge 列表。
    // 每一条 JobEdge 都指向一个下游的消费者 JobVertex。
    private final List<JobEdge> consumers = new ArrayList<>();

    // The type of partition to use at runtime
    // 结果分区类型。
    // 定义了运行时数据的分区类型，例如：
    // BLOCKING: 阻塞式，数据完全写完后才能被读取（用于批处理或流批一体的场景）。
    // PIPELINED: 流水线式，数据生成后立即被发送（用于流处理）。
    private final ResultPartitionType resultType;

    // 决定数据在消费者中的分布模式，
    //  例如 ALL_TO_ALL 或 POINTWISE
    private DistributionPattern distributionPattern;
    // 是否为广播。
    // 标识数据是否以广播方式传输给下游所有消费者子任务。该值由第一个连接的 JobEdge 决定。
    private boolean isBroadcast;

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSet(
            IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
        this.id = checkNotNull(id);
        this.producer = checkNotNull(producer);
        this.resultType = checkNotNull(resultType);
    }

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSetID getId() {
        return id;
    }

    public JobVertex getProducer() {
        return producer;
    }

    public List<JobEdge> getConsumers() {
        return this.consumers;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public DistributionPattern getDistributionPattern() {
        return distributionPattern;
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    // --------------------------------------------------------------------------------------------
    // 添加消费者边。
    // 将一条新的 JobEdge 添加到消费者列表中。该方法包含重要的完整性检查：
    public void addConsumer(JobEdge edge) {
        // sanity check
        checkState(id.equals(edge.getSourceId()), "Incompatible dataset id.");

        if (consumers.isEmpty()) {
            distributionPattern = edge.getDistributionPattern();
            isBroadcast = edge.isBroadcast();
        } else {
            checkState(
                    distributionPattern == edge.getDistributionPattern(),
                    "Incompatible distribution pattern.");
            checkState(isBroadcast == edge.isBroadcast(), "Incompatible broadcast type.");
        }
        consumers.add(edge);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Intermediate Data Set (" + id + ")";
    }
}
