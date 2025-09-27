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
 * IntermediateDataset 就表示一个 JobVertex 的输出结果，本身并不存储数据，它只是描述了数据的流动关系，尤其是数据的生产者和消费者之间的依赖关系
 * <p>Intermediate data sets may be read by other operators, materialized, or discarded.
 */
public class IntermediateDataSet implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final IntermediateDataSetID id; // the identifier
    //指向生成该数据集的操作（也称为生产者）。它描述了数据集的来源
    private final JobVertex producer; // the operation that produced this data set
    //表示此数据集的所有消费者（下游操作）。每个消费者通过 JobEdge 表示，所有消费者必须具有相同的分区器和并行度（通过 addConsumer 方法进行检查）
    // All consumers must have the same partitioner and parallelism
    private final List<JobEdge> consumers = new ArrayList<>();
    //决定运行时此数据集的分区类型，如分区是块化还是流式。常见的值包括 BLOCKING 和 PIPELINED
    // The type of partition to use at runtime
    private final ResultPartitionType resultType;
    //决定数据在消费者中的分布模式，例如 ALL_TO_ALL 或 POINTWISE
    private DistributionPattern distributionPattern;

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
