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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.failover.SchedulingPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalEdge;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
// DefaultExecutionTopology 是接口 SchedulingTopology 的具体实现类，它的核心职能是作为 适配器 (Adapter)：
// 构建调度视图： 它将 Flink 运行时底层的、复杂且动态变化的 ExecutionGraph（执行图，包含了所有物理执行细节）转换和映射成高层次的、面向调度策略的 SchedulingTopology（调度拓扑）
// 解耦与简化： 它将重量级的 ExecutionVertex 和 IntermediateResultPartition 封装为轻量级的 DefaultExecutionVertex 和 DefaultResultPartition，供调度器使用，从而解耦了调度逻辑与执行图的复杂性。
// 计算区域（Region）： 它的主要复杂性在于计算和管理 DefaultSchedulingPipelinedRegion。它根据逻辑图和数据交换模式（Pipelined vs. Blocking）将任务分组，这是 Flink 调度和故障恢复的基本单位。
// 动态更新： 它支持对执行图的增量更新（例如，在自适应批处理模式下），通过 notifyExecutionGraphUpdated 方法来处理新加入的 JobVertex。

/** Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}. */
public class DefaultExecutionTopology implements SchedulingTopology {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionTopology.class);
    // 顶点索引。
    // 存储所有调度执行顶点，通过 ID 快速查找。是调度拓扑中的节点集合。
    private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;
    // 顶点列表。
    // 存储所有执行顶点，可能按拓扑顺序排列，方便迭代。
    private final List<DefaultExecutionVertex> executionVerticesList;
    // 结果分区索引。
    // 存储所有调度结果分区，通过 ID 快速查找。是调度拓扑中的边集合。
    private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;
    // 顶点到区域的映射。
    // 记录每个执行顶点所属的流水线区域，用于快速确定任务的调度单元。
    private final Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex;
    // 区域列表。
    // 存储计算出的所有流水线区域，是调度和故障恢复的逻辑分组。
    private final List<DefaultSchedulingPipelinedRegion> pipelinedRegions;
    // 边管理器引用。
    // 引用执行图中的边管理器，用于查询顶点之间的连接和依赖关系（如 ConsumedPartitionGroup）。
    private final EdgeManager edgeManager;
    // 拓扑排序 ID 提供者。
    //   一个函数，用于获取按拓扑顺序排列的执行顶点 ID 列表。
    private final Supplier<List<ExecutionVertexID>> sortedExecutionVertexIds;
    // 将 JobVertexID 映射到逻辑管道区域（LogicalPipelinedRegion）。
    // LogicalPipelinedRegion 是 Flink 作业的逻辑分区，表示一组可以并行执行的任务
    private final Map<JobVertexID, DefaultLogicalPipelinedRegion>
            logicalPipelinedRegionsByJobVertexId;
    // 拓扑监听器。
    // 存储所有注册的监听器，用于在拓扑更新时通知外部组件。
    /** Listeners that will be notified whenever the scheduling topology is updated. */
    private final List<SchedulingTopologyListener> schedulingTopologyListeners = new ArrayList<>();

    private DefaultExecutionTopology(
            Supplier<List<ExecutionVertexID>> sortedExecutionVertexIds,
            EdgeManager edgeManager,
            Map<JobVertexID, DefaultLogicalPipelinedRegion> logicalPipelinedRegionsByJobVertexId) {
        this.sortedExecutionVertexIds = checkNotNull(sortedExecutionVertexIds);
        this.edgeManager = checkNotNull(edgeManager);
        this.logicalPipelinedRegionsByJobVertexId =
                checkNotNull(logicalPipelinedRegionsByJobVertexId);

        this.executionVerticesById = new HashMap<>();
        this.executionVerticesList = new ArrayList<>();
        this.resultPartitionsById = new HashMap<>();
        this.pipelinedRegionsByVertex = new HashMap<>();
        this.pipelinedRegions = new ArrayList<>();
    }

    @Override
    public Iterable<DefaultExecutionVertex> getVertices() {
        return Collections.unmodifiableList(executionVerticesList);
    }

    @Override
    public DefaultExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
        final DefaultExecutionVertex executionVertex = executionVerticesById.get(executionVertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
        }
        return executionVertex;
    }

    @Override
    public DefaultResultPartition getResultPartition(
            final IntermediateResultPartitionID intermediateResultPartitionId) {
        final DefaultResultPartition resultPartition =
                resultPartitionsById.get(intermediateResultPartitionId);
        if (resultPartition == null) {
            throw new IllegalArgumentException(
                    "can not find partition: " + intermediateResultPartitionId);
        }
        return resultPartition;
    }

    @Override
    public void registerSchedulingTopologyListener(SchedulingTopologyListener listener) {
        checkNotNull(listener);
        schedulingTopologyListeners.add(listener);
    }

    @Override
    public Iterable<DefaultSchedulingPipelinedRegion> getAllPipelinedRegions() {
        checkNotNull(pipelinedRegions);

        return Collections.unmodifiableCollection(pipelinedRegions);
    }

    @Override
    public DefaultSchedulingPipelinedRegion getPipelinedRegionOfVertex(
            final ExecutionVertexID vertexId) {
        checkNotNull(pipelinedRegionsByVertex);

        final DefaultSchedulingPipelinedRegion pipelinedRegion =
                pipelinedRegionsByVertex.get(vertexId);
        if (pipelinedRegion == null) {
            throw new IllegalArgumentException("Unknown execution vertex " + vertexId);
        }
        return pipelinedRegion;
    }

    public EdgeManager getEdgeManager() {
        return edgeManager;
    }
    // 将底层的 ExecutionGraph 转换成一个 JobVertex ID 到其所属的逻辑流水线区域 (DefaultLogicalPipelinedRegion) 的映射表。
    // 这个映射是后续计算物理调度区域的基础。
    // 接收一个 ExecutionGraph（Flink 物理执行图）作为输入参数。
    private static Map<JobVertexID, DefaultLogicalPipelinedRegion>
            computeLogicalPipelinedRegionsByJobVertexId(final ExecutionGraph executionGraph) {
        // 从 ExecutionGraph 中获取所有 ExecutionJobVertex（JobVertex 在执行图中的抽象）的列表，
        // 并确保它们是拓扑排序的（即上游 JobVertex 在下游 JobVertex 之前）。
        // 返回一个 Iterable<ExecutionJobVertex>。
        List<JobVertex> topologicallySortedJobVertices =
                IterableUtils.toStream(executionGraph.getVerticesTopologically())
                        .map(ExecutionJobVertex::getJobVertex)
                        .collect(Collectors.toList());
        // 获得的拓扑排序的 JobVertex 列表为输入，构建出一个 DefaultLogicalTopology 对象。
        Iterable<DefaultLogicalPipelinedRegion> logicalPipelinedRegions =
                DefaultLogicalTopology.fromTopologicallySortedJobVertices(
                                topologicallySortedJobVertices)
                        .getAllPipelinedRegions();

        Map<JobVertexID, DefaultLogicalPipelinedRegion> logicalPipelinedRegionsByJobVertexId =
                new HashMap<>();
        //构建 JobVertex ID 到区域的映射表
        for (DefaultLogicalPipelinedRegion logicalPipelinedRegion : logicalPipelinedRegions) {
            for (LogicalVertex vertex : logicalPipelinedRegion.getVertices()) {
                logicalPipelinedRegionsByJobVertexId.put(vertex.getId(), logicalPipelinedRegion);
            }
        }

        return logicalPipelinedRegionsByJobVertexId;
    }

    public void notifyExecutionGraphUpdated(
            final DefaultExecutionGraph executionGraph,
            final List<ExecutionJobVertex> newlyInitializedJobVertices) {

        checkNotNull(executionGraph, "execution graph can not be null");

        final Set<JobVertexID> newJobVertexIds =
                newlyInitializedJobVertices.stream()
                        .map(ExecutionJobVertex::getJobVertexId)
                        .collect(Collectors.toSet());

        // any mustBePipelinedConsumed input should be from within this new set so that existing
        // pipelined regions will not change
        newlyInitializedJobVertices.stream()
                .map(ExecutionJobVertex::getJobVertex)
                .flatMap(v -> v.getInputs().stream())
                .map(JobEdge::getSource)
                .filter(r -> r.getResultType().mustBePipelinedConsumed())
                .map(IntermediateDataSet::getProducer)
                .map(JobVertex::getID)
                .forEach(id -> checkState(newJobVertexIds.contains(id)));

        final Iterable<ExecutionVertex> newExecutionVertices =
                newlyInitializedJobVertices.stream()
                        .flatMap(jobVertex -> Stream.of(jobVertex.getTaskVertices()))
                        .collect(Collectors.toList());

        generateNewExecutionVerticesAndResultPartitions(newExecutionVertices);

        generateNewPipelinedRegions(newExecutionVertices);

        ensureCoLocatedVerticesInSameRegion(pipelinedRegions, executionGraph);

        notifySchedulingTopologyUpdated(newExecutionVertices);
    }

    private void notifySchedulingTopologyUpdated(Iterable<ExecutionVertex> newExecutionVertices) {
        List<ExecutionVertexID> newVertexIds =
                IterableUtils.toStream(newExecutionVertices)
                        .map(ExecutionVertex::getID)
                        .collect(Collectors.toList());
        for (SchedulingTopologyListener listener : schedulingTopologyListeners) {
            listener.notifySchedulingTopologyUpdated(this, newVertexIds);
        }
    }
    //从执行图构建调度拓扑图
    public static DefaultExecutionTopology fromExecutionGraph(
            DefaultExecutionGraph executionGraph) {
        checkNotNull(executionGraph, "execution graph can not be null");

        EdgeManager edgeManager = executionGraph.getEdgeManager();

        DefaultExecutionTopology schedulingTopology =
                new DefaultExecutionTopology(
                        () ->
                                IterableUtils.toStream(executionGraph.getAllExecutionVertices())
                                        .map(ExecutionVertex::getID)
                                        .collect(Collectors.toList()),
                        edgeManager,
                        computeLogicalPipelinedRegionsByJobVertexId(executionGraph));

        schedulingTopology.notifyExecutionGraphUpdated(
                executionGraph,
                IterableUtils.toStream(executionGraph.getVerticesTopologically())
                        .filter(ExecutionJobVertex::isInitialized)
                        .collect(Collectors.toList()));

        return schedulingTopology;
    }

    private void generateNewExecutionVerticesAndResultPartitions(
            Iterable<ExecutionVertex> newExecutionVertices) {
        for (ExecutionVertex vertex : newExecutionVertices) {
            List<DefaultResultPartition> producedPartitions =
                    generateProducedSchedulingResultPartition(
                            vertex.getProducedPartitions(),
                            edgeManager::getConsumerVertexGroupsForPartition);

            producedPartitions.forEach(
                    partition -> resultPartitionsById.put(partition.getId(), partition));

            DefaultExecutionVertex schedulingVertex =
                    generateSchedulingExecutionVertex(
                            vertex,
                            producedPartitions,
                            edgeManager.getConsumedPartitionGroupsForVertex(vertex.getID()),
                            resultPartitionsById::get);
            executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
        }

        executionVerticesList.clear();
        for (ExecutionVertexID vertexID : sortedExecutionVertexIds.get()) {
            executionVerticesList.add(executionVerticesById.get(vertexID));
        }
    }

    private static List<DefaultResultPartition> generateProducedSchedulingResultPartition(
            Map<IntermediateResultPartitionID, IntermediateResultPartition>
                    producedIntermediatePartitions,
            Function<IntermediateResultPartitionID, List<ConsumerVertexGroup>>
                    partitionConsumerVertexGroupsRetriever) {

        List<DefaultResultPartition> producedSchedulingPartitions =
                new ArrayList<>(producedIntermediatePartitions.size());

        producedIntermediatePartitions
                .values()
                .forEach(
                        irp ->
                                producedSchedulingPartitions.add(
                                        new DefaultResultPartition(
                                                irp.getPartitionId(),
                                                irp.getIntermediateResult().getId(),
                                                irp.getResultType(),
                                                () ->
                                                        irp.hasDataAllProduced()
                                                                ? ResultPartitionState
                                                                        .ALL_DATA_PRODUCED
                                                                : ResultPartitionState.CREATED,
                                                () ->
                                                        partitionConsumerVertexGroupsRetriever
                                                                .apply(irp.getPartitionId()),
                                                irp::getConsumedPartitionGroups)));

        return producedSchedulingPartitions;
    }

    private static DefaultExecutionVertex generateSchedulingExecutionVertex(
            ExecutionVertex vertex,
            List<DefaultResultPartition> producedPartitions,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {

        DefaultExecutionVertex schedulingVertex =
                new DefaultExecutionVertex(
                        vertex.getID(),
                        producedPartitions,
                        vertex::getExecutionState,
                        consumedPartitionGroups,
                        resultPartitionRetriever);

        producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));

        return schedulingVertex;
    }

    private void generateNewPipelinedRegions(Iterable<ExecutionVertex> newExecutionVertices) {

        final Iterable<DefaultExecutionVertex> newSchedulingExecutionVertices =
                IterableUtils.toStream(newExecutionVertices)
                        .map(ExecutionVertex::getID)
                        .map(executionVerticesById::get)
                        .collect(Collectors.toList());

        Map<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>>
                sortedExecutionVerticesInPipelinedRegion = new IdentityHashMap<>();

        for (DefaultExecutionVertex schedulingVertex : newSchedulingExecutionVertices) {
            sortedExecutionVerticesInPipelinedRegion
                    .computeIfAbsent(
                            logicalPipelinedRegionsByJobVertexId.get(
                                    schedulingVertex.getId().getJobVertexId()),
                            ignore -> new ArrayList<>())
                    .add(schedulingVertex);
        }

        long buildRegionsStartTime = System.nanoTime();

        Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // A SchedulingPipelinedRegion can be derived from just one LogicalPipelinedRegion.
        // Thus, we can traverse all LogicalPipelinedRegions and convert them into
        // SchedulingPipelinedRegions one by one. The LogicalPipelinedRegions and
        // SchedulingPipelinedRegions are both connected with inter-region blocking edges.
        for (Map.Entry<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>> entry :
                sortedExecutionVerticesInPipelinedRegion.entrySet()) {

            DefaultLogicalPipelinedRegion logicalPipelinedRegion = entry.getKey();
            List<DefaultExecutionVertex> schedulingExecutionVertices = entry.getValue();

            if (containsIntraRegionAllToAllEdge(logicalPipelinedRegion)) {
                // For edges inside one LogicalPipelinedRegion, if there is any all-to-all edge, it
                // could be under two circumstances:
                //
                // 1. Pipelined all-to-all edge:
                //     Pipelined all-to-all edge will connect all vertices pipelined. Therefore,
                // all execution vertices derived from this LogicalPipelinedRegion should be in one
                // SchedulingPipelinedRegion.
                //
                // 2. Blocking all-to-all edge:
                //     For intra-region blocking all-to-all edge, we must make sure all the vertices
                // are inside one SchedulingPipelinedRegion, so that there will be no deadlock
                // happens during scheduling. For more details about this case, please refer to
                // FLINK-17330 (https://issues.apache.org/jira/browse/FLINK-17330).
                //
                // Therefore, if a LogicalPipelinedRegion contains any intra-region all-to-all
                // edge, we just convert the entire LogicalPipelinedRegion to a sole
                // SchedulingPipelinedRegion directly.
                rawPipelinedRegions.add(new HashSet<>(schedulingExecutionVertices));
            } else {
                // If there are only pointwise edges inside the LogicalPipelinedRegion, we can use
                // SchedulingPipelinedRegionComputeUtil to compute the regions with O(N) computation
                // complexity.
                rawPipelinedRegions.addAll(
                        SchedulingPipelinedRegionComputeUtil.computePipelinedRegions(
                                schedulingExecutionVertices,
                                executionVerticesById::get,
                                resultPartitionsById::get));
            }
        }

        for (Set<? extends SchedulingExecutionVertex> rawPipelinedRegion : rawPipelinedRegions) {
            //noinspection unchecked
            final DefaultSchedulingPipelinedRegion pipelinedRegion =
                    new DefaultSchedulingPipelinedRegion(
                            (Set<DefaultExecutionVertex>) rawPipelinedRegion,
                            resultPartitionsById::get);
            pipelinedRegions.add(pipelinedRegion);

            for (SchedulingExecutionVertex executionVertex : rawPipelinedRegion) {
                pipelinedRegionsByVertex.put(executionVertex.getId(), pipelinedRegion);
            }
        }

        long buildRegionsDuration = (System.nanoTime() - buildRegionsStartTime) / 1_000_000;
        LOG.info(
                "Built {} new pipelined regions in {} ms, total {} pipelined regions currently.",
                rawPipelinedRegions.size(),
                buildRegionsDuration,
                pipelinedRegions.size());
    }

    /**
     * Check if the {@link DefaultLogicalPipelinedRegion} contains intra-region all-to-all edges or
     * not.
     */
    private static boolean containsIntraRegionAllToAllEdge(
            DefaultLogicalPipelinedRegion logicalPipelinedRegion) {
        for (LogicalVertex vertex : logicalPipelinedRegion.getVertices()) {
            for (LogicalEdge inputEdge : vertex.getInputs()) {
                if (inputEdge.getDistributionPattern() == DistributionPattern.ALL_TO_ALL
                        && logicalPipelinedRegion.contains(inputEdge.getProducerVertexId())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Co-location constraints are only used for iteration head and tail. A paired head and tail
     * needs to be in the same pipelined region so that they can be restarted together.
     */
    private static void ensureCoLocatedVerticesInSameRegion(
            List<DefaultSchedulingPipelinedRegion> pipelinedRegions,
            ExecutionGraph executionGraph) {

        final Map<CoLocationConstraint, DefaultSchedulingPipelinedRegion> constraintToRegion =
                new HashMap<>();
        for (DefaultSchedulingPipelinedRegion region : pipelinedRegions) {
            for (DefaultExecutionVertex vertex : region.getVertices()) {
                final CoLocationConstraint constraint =
                        getCoLocationConstraint(vertex.getId(), executionGraph);
                if (constraint != null) {
                    final DefaultSchedulingPipelinedRegion regionOfConstraint =
                            constraintToRegion.get(constraint);
                    checkState(
                            regionOfConstraint == null || regionOfConstraint == region,
                            "co-located tasks must be in the same pipelined region");
                    constraintToRegion.putIfAbsent(constraint, region);
                }
            }
        }
    }

    private static CoLocationConstraint getCoLocationConstraint(
            ExecutionVertexID executionVertexId, ExecutionGraph executionGraph) {

        CoLocationGroup coLocationGroup =
                Objects.requireNonNull(
                                executionGraph.getJobVertex(executionVertexId.getJobVertexId()))
                        .getCoLocationGroup();
        return coLocationGroup == null
                ? null
                : coLocationGroup.getLocationConstraint(executionVertexId.getSubtaskIndex());
    }
}
