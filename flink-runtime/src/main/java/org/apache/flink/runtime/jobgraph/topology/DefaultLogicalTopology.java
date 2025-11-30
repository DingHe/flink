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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.executiongraph.failover.LogicalPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LogicalTopology}. It is an adapter of {@link JobGraph}. */
// DefaultLogicalTopology 类是 Flink **逻辑拓扑（Logical Topology）**接口 LogicalTopology 的默认实现。
// JobGraph 的适配器（Adapter）：它将用户提交的原始逻辑作业图 (JobGraph) 结构转换成一个更适合内部调度和分析使用的逻辑拓扑视图。
// 构建逻辑组件：它负责封装 JobVertex（作业顶点）和 IntermediateDataSet（中间数据集）为内部的逻辑对应物：DefaultLogicalVertex 和 DefaultLogicalResult。
// 计算流水线区域（Pipelined Region）：它集成了计算作业中所有逻辑流水线区域（即可以一起以流式方式执行的顶点集合）的功能，这是 Flink 容错和调度优化的重要基础。
public class DefaultLogicalTopology implements LogicalTopology {
    // 按拓扑排序的逻辑顶点列表。
    // 存储了所有 DefaultLogicalVertex 实例，并且它们是按照数据流方向从 Source（源）到 Sink（汇）进行拓扑排序的。这对于调度器按顺序处理和部署任务至关重要。
    private final List<DefaultLogicalVertex> verticesSorted;
    // ID 到逻辑顶点的映射。
    // 提供了一个 O(1) 复杂度的查找表，允许通过 JobVertexID 快速获取对应的 DefaultLogicalVertex 实例。
    private final Map<JobVertexID, DefaultLogicalVertex> idToVertexMap;
    // ID 到逻辑结果的映射。
    // 提供了一个查找表，允许通过 IntermediateDataSetID 快速获取对应的 DefaultLogicalResult 实例。
    private final Map<IntermediateDataSetID, DefaultLogicalResult> idToResultMap;

    private DefaultLogicalTopology(final List<JobVertex> jobVertices) {
        checkNotNull(jobVertices);

        this.verticesSorted = new ArrayList<>(jobVertices.size());
        this.idToVertexMap = new HashMap<>();
        this.idToResultMap = new HashMap<>();

        buildVerticesAndResults(jobVertices);
    }
    // 从完整的 JobGraph 实例创建 DefaultLogicalTopology。
    // 首先从 JobGraph 中获取按拓扑排序的 JobVertex 列表，然后调用 fromTopologicallySortedJobVertices
    public static DefaultLogicalTopology fromJobGraph(final JobGraph jobGraph) {
        checkNotNull(jobGraph);

        return fromTopologicallySortedJobVertices(
                jobGraph.getVerticesSortedTopologicallyFromSources());
    }
    // 从已拓扑排序的 JobVertex 列表创建 DefaultLogicalTopology。
    // 实际的创建入口，调用私有构造方法。
    public static DefaultLogicalTopology fromTopologicallySortedJobVertices(
            final List<JobVertex> jobVertices) {
        return new DefaultLogicalTopology(jobVertices);
    }
    // 遍历原始 JobVertex 列表，构建并填充 DefaultLogicalVertex 和 DefaultLogicalResult 实例。
    private void buildVerticesAndResults(final Iterable<JobVertex> topologicallySortedJobVertices) {
        final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever = this::getVertex;
        final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever =
                this::getResult;

        for (JobVertex jobVertex : topologicallySortedJobVertices) {
            // 遍历每一个 JobVertex，创建对应的 DefaultLogicalVertex 并存储到 verticesSorted 和 idToVertexMap 中。
            final DefaultLogicalVertex logicalVertex =
                    new DefaultLogicalVertex(jobVertex, resultRetriever);
            this.verticesSorted.add(logicalVertex);
            this.idToVertexMap.put(logicalVertex.getId(), logicalVertex);
            // 遍历该 JobVertex 产生的所有 IntermediateDataSet，
            // 创建对应的 DefaultLogicalResult 并存储到 idToResultMap 中。
            for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
                final DefaultLogicalResult logicalResult =
                        new DefaultLogicalResult(intermediateDataSet, vertexRetriever);
                idToResultMap.put(logicalResult.getId(), logicalResult);
            }
        }
    }

    @Override
    public Iterable<DefaultLogicalVertex> getVertices() {
        return verticesSorted;
    }

    public DefaultLogicalVertex getVertex(final JobVertexID vertexId) {
        return Optional.ofNullable(idToVertexMap.get(vertexId))
                .orElseThrow(
                        () -> new IllegalArgumentException("can not find vertex: " + vertexId));
    }

    private DefaultLogicalResult getResult(final IntermediateDataSetID resultId) {
        return Optional.ofNullable(idToResultMap.get(resultId))
                .orElseThrow(
                        () -> new IllegalArgumentException("can not find result: " + resultId));
    }
    // 计算并返回所有逻辑流水线区域。
    @Override
    public Iterable<DefaultLogicalPipelinedRegion> getAllPipelinedRegions() {
        // 用于根据数据传输类型（Blocking/Pipelined）划分顶点集合。
        final Set<Set<LogicalVertex>> regionsRaw =
                LogicalPipelinedRegionComputeUtil.computePipelinedRegions(verticesSorted);

        final Set<DefaultLogicalPipelinedRegion> regions = new HashSet<>();
        // 将计算得到的原始区域顶点集合（Set<Set<LogicalVertex>>）封装为 DefaultLogicalPipelinedRegion 实例集合后返回。
        for (Set<LogicalVertex> regionVertices : regionsRaw) {
            regions.add(new DefaultLogicalPipelinedRegion(regionVertices));
        }
        return regions;
    }
}
