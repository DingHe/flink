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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
// 核心作用是：根据拓扑结构中的流水线区域（Pipelined Region）来制定和执行调度计划。
// 区域级调度：它不以单个任务（ExecutionVertex）为单位进行调度，而是以整个流水线区域为基本调度单元。一个区域内的所有任务会同时启动，实现数据在区域内的流水线传输。
// 管理阻塞依赖：它负责分析和跟踪区域之间的阻塞性数据依赖（Non-Pipelined/BLOCKING Shuffle）。只有当上游区域产生的阻塞数据完全准备好后，下游区域才会被激活调度。
// 实现惰性调度：它实现了惰性调度（Lazy Scheduling），特别是针对批处理作业。作业启动时只调度源头区域（Source Region），其他区域根据数据依赖按需启动，从而优化了资源利用率。
// 该策略是 Flink 实现 FLIP-134 (批处理 DataStream API) 及其 Region-Based 调度机制的执行者。
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {
    // 调度操作执行者。
    // 用于执行实际的调度命令，如分配槽位和部署任务。
    // 调度策略只负责“决定”做什么，而 schedulerOperations 负责“执行”。
    private final SchedulerOperations schedulerOperations;
    // 调度拓扑结构。
    // 包含了整个作业图的执行顶点、结果分区以及它们之间的连接关系，是调度策略分析依赖的基础。
    private final SchedulingTopology schedulingTopology;

    /** External consumer regions of each ConsumedPartitionGroup. */
    // 分区组的消费者区域映射。
    // 存储了每个消耗分区组（ConsumedPartitionGroup，即输入）对应的下游（消费方）区域集合。用于快速查找某个分区组可用后，可以激活哪些下游区域。
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new IdentityHashMap<>();
    // 区域内顶点的有序列表。
    // 存储了每个区域内的所有执行顶点 ID，并按其部署顺序排序。当区域被调度时，会按此列表顺序部署任务。
    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted =
            new IdentityHashMap<>();

    /** All produced partition groups of one schedulingPipelinedRegion. */
    // 区域的产出分区组。
    // 存储了每个区域内部所有生产出来的结果分区组的集合。
    private final Map<SchedulingPipelinedRegion, Set<ConsumedPartitionGroup>>
            producedPartitionGroupsOfRegion = new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    // 跨区域消耗的分区组集合。
    // 存储了那些由多个上游区域共同生产的分区组。这在调度多输入区域时需要特殊处理。
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
            Collections.newSetFromMap(new IdentityHashMap<>());
    // 已调度的区域集合。
    // 跟踪已经被成功调度并启动的区域，避免重复调度，并作为下游区域启动的判断依据。
    private final Set<SchedulingPipelinedRegion> scheduledRegions =
            Collections.newSetFromMap(new IdentityHashMap<>());

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }
    // 初始化调度策略的核心依赖数据结构
    private void init() {

        initCrossRegionConsumedPartitionGroups();

        initPartitionGroupConsumerRegions();

        initProducedPartitionGroupsOfRegion();

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            // 查找区域： 对当前遍历到的任务 (vertex)，调用拓扑方法获取它所属的流水线区域对象。
            // 检查映射表中是否已有当前 region 对应的任务列表。如果没有，则创建一个新的空 ArrayList；如果有，则返回已存在的列表。
            final SchedulingPipelinedRegion region =
                    schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted
                    .computeIfAbsent(region, r -> new ArrayList<>())
                    .add(vertex.getId());
        }
    }
    // 作用是初始化 producedPartitionGroupsOfRegion 映射表。
    // 这个映射表记录了每个流水线区域（Region）在完成执行后，会产生哪些阻塞性数据输入（ConsumedPartitionGroup）供下游区域消费。
    private void initProducedPartitionGroupsOfRegion() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            Set<ConsumedPartitionGroup> producedPartitionGroupsSetOfRegion = new HashSet<>();
            // 遍历当前区域 (region) 内的所有执行顶点（任务）。
            for (SchedulingExecutionVertex executionVertex : region.getVertices()) {
                producedPartitionGroupsSetOfRegion.addAll(
                        IterableUtils.toStream(executionVertex.getProducedResults())
                                .flatMap(
                                        // 每一个结果分区 (partition)，调用 getConsumedPartitionGroups() 方法，获取所有消耗该分区的下游分区组，并将其转换为 Stream。
                                        partition ->
                                                partition.getConsumedPartitionGroups().stream())
                                .collect(Collectors.toSet()));
            }
            producedPartitionGroupsOfRegion.put(region, producedPartitionGroupsSetOfRegion);
        }
    }
    // 作用是初始化一个集合 (crossRegionConsumedPartitionGroups)，
    // 该集合包含了所有由多个上游流水线区域（Pipelined Region）共同生产的阻塞性数据依赖（ConsumedPartitionGroup）
    private void initCrossRegionConsumedPartitionGroups() {
        // 定义并初始化一个映射表。
        // 键（Key）是数据输入组 (ConsumedPartitionGroup)，值（Value）是生产这个数据输入组的所有上游区域 (SchedulingPipelinedRegion) 的集合
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
                producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();
        // 遍历所有下游区域，以识别它们的输入依赖。
        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            // 遍历当前下游区域 (pipelinedRegion) 所消耗的所有**非流水线（阻塞）**分区组。非流水线（即阻塞）依赖是区域调度的主要边界。
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                // 检查映射表是否已包含当前 consumedPartitionGroup 作为键：
                // 如果不存在，则调用辅助方法 getProducerRegionsForConsumedPartitionGroup 计算出生产该分区组的所有上游区域，并将结果存入映射表。如果存在，则不做任何操作
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(
                        consumedPartitionGroup, this::getProducerRegionsForConsumedPartitionGroup);
            }
        }
        // 识别跨区域的多生产者依赖
        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                // 获取生产者集合。
                // 从第一部分建立的映射表中，取出生产当前 consumedPartitionGroup 的所有上游区域集合。
                final Set<SchedulingPipelinedRegion> producerRegions =
                        producerRegionsByConsumedPartitionGroup.get(consumedPartitionGroup);
                // 核心判断逻辑： 检查两个条件是否同时成立：
                // 1. producerRegions.size() > 1：生产该分区组的上游区域数量是否大于 1（即存在多个生产者）。
                // 2. producerRegions.contains(pipelinedRegion)：当前正在检查的下游区域 (pipelinedRegion) 也是该分区组的生产者之一。
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    // 根据一个消耗分区组（ConsumedPartitionGroup），找出所有生产该分区组数据的上游流水线区域（SchedulingPipelinedRegion）的集合。
    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        // 初始化生产者区域集合。
        // 创建一个空的 Set 集合用于存储找到的所有上游生产者区域。
        final Set<SchedulingPipelinedRegion> producerRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        // 查找并添加生产者区域
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }
    // 根据一个中间结果分区 ID（IntermediateResultPartitionID），向上追溯，找出生产该数据分区的上游流水线区域（SchedulingPipelinedRegion）。
    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }
    // 作用是初始化 partitionGroupConsumerRegions 映射表。
    // 这个映射表记录了哪些流水线区域（Region）是特定阻塞性数据输入（ConsumedPartitionGroup）的消费者。
    // 建立了从阻塞数据到依赖于该数据的下游区域的反向索引
    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            // 遍历当前区域 (region) 所消耗的所有非流水线（即阻塞性）分区组。
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllNonPipelinedConsumedPartitionGroups()) {
                // 条件判断（条件 A）： 检查当前消耗的分区组是否属于跨区域的多生产者依赖集合
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    // 获取或创建 Set：
                    // 检查当前 consumedPartitionGroup 是否已作为键存在于映射表中：
                    // 如果存在，则返回其对应的下游区域 Set 集合；如果不存在，则创建一个新的空 HashSet 作为值，并将其关联到该键。
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getBlockingDownstreamRegionsOfVertex(
            SchedulingExecutionVertex executionVertex) {
        return IterableUtils.toStream(executionVertex.getProducedResults())
                .filter(partition -> !partition.getResultType().canBePipelinedConsumed())
                .flatMap(partition -> partition.getConsumedPartitionGroups().stream())
                .filter(
                        group ->
                                crossRegionConsumedPartitionGroups.contains(group)
                                        || group.areAllPartitionsFinished())
                .flatMap(
                        partitionGroup ->
                                partitionGroupConsumerRegions
                                        .getOrDefault(partitionGroup, Collections.emptySet())
                                        .stream())
                .collect(Collectors.toSet());
    }

    @Override
    public void startScheduling() {
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());
        maybeScheduleRegions(sourceRegions);
    }
    // 判断一个流水线区域是否是作业的源头区域的关键函数。
    // 检查一个区域是否有任何外部的阻塞性输入依赖来确定其是否为源头区域。
    // 核心判断逻辑： 如果一个区域没有外部的、非流水线（即阻塞性）输入，那么它就是可以立即启动的源头区域（Source Region）。
    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                    || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        scheduledRegions.removeAll(regionsToRestart);
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            maybeScheduleRegions(
                    getBlockingDownstreamRegionsOfVertex(
                            schedulingTopology.getVertex(executionVertexId)));
        }
    }

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    // 主要作用是：从一组给定的触发区域开始，递归地检查并收集所有当前输入依赖已满足、可以被调度的后续区域，然后按照拓扑顺序部署它们。
    // 接收一个初始触发区域集合 (regions) 作为输入。
    // 这个集合通常是源头区域，或由于上游任务完成/重启而需要检查的下游区域。
    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
        // 初始化待调度集合
        // 创建一个空的 HashSet，用于最终收集所有通过依赖检查、确定可以被调度的区域。
        final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
        Set<SchedulingPipelinedRegion> nextRegions = regions;
        while (!nextRegions.isEmpty()) {
            // 1. 检查 nextRegions 中的区域，将所有可调度的区域添加到 regionsToSchedule 中。
            // 2. 返回这些被调度的区域的下游区域集合，作为新的 nextRegions，供下一轮循环检查。
            nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
        }
        // schedule regions in topological order.
        // 根据作业的拓扑结构和区域间的依赖关系，对 regionsToSchedule 中的区域进行拓扑排序。
        // 确保上游区域先于下游区域被部署。
        SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                        schedulingTopology, regionsToSchedule)
                // 对每一个区域调用 scheduleRegion() 方法，提交实际的槽位分配和任务部署请求。
                .forEach(this::scheduleRegion);
    }
    // 接收一组待检查的区域 (currentRegions)，遍历它们，检查哪个区域的输入依赖已满足（可调度），将可调度的区域添加到最终部署列表 (regionsToSchedule) 中，并返回这些被调度区域的所有下游区域，作为下一轮检查的起点。
    // currentRegions 参数 1： 本轮需要检查是否可以调度的区域集合。
    // 参数 2： 用于收集所有确定可以调度的区域的集合（结果会被更新）。
    private Set<SchedulingPipelinedRegion> addSchedulableAndGetNextRegions(
            Set<SchedulingPipelinedRegion> currentRegions,
            Set<SchedulingPipelinedRegion> regionsToSchedule) {
        // 初始化下一轮集合
        // 创建一个空的 HashSet，用于收集当前被确认可调度的区域的下游区域，作为方法返回值。
        Set<SchedulingPipelinedRegion> nextRegions = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        // 初始化访问记录： 创建一个 Set，用于记录在本轮循环中已经被处理过的产出分区组。
        final Set<ConsumedPartitionGroup> visitedConsumedPartitionGroups = new HashSet<>();

        for (SchedulingPipelinedRegion currentRegion : currentRegions) {
            if (isRegionSchedulable(currentRegion, consumableStatusCache, regionsToSchedule)) {
                regionsToSchedule.add(currentRegion);
                producedPartitionGroupsOfRegion
                        .getOrDefault(currentRegion, Collections.emptySet())
                        .forEach(
                                (producedPartitionGroup) -> {
                                    if (!producedPartitionGroup
                                            .getResultPartitionType()
                                            .canBePipelinedConsumed()) {
                                        return;
                                    }
                                    // If this group has been visited, there is no need
                                    // to repeat the determination.
                                    if (visitedConsumedPartitionGroups.contains(
                                            producedPartitionGroup)) {
                                        return;
                                    }
                                    visitedConsumedPartitionGroups.add(producedPartitionGroup);
                                    nextRegions.addAll(
                                            partitionGroupConsumerRegions.getOrDefault(
                                                    producedPartitionGroup,
                                                    Collections.emptySet()));
                                });
            }
        }
        return nextRegions;
    }

    // Flink 区域调度策略中用于判断一个流水线区域是否满足启动条件的核心决策函数。
    private boolean isRegionSchedulable(
            final SchedulingPipelinedRegion region, // 正在被检查的区域对象。
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache, // 用于缓存输入分区组可消费状态的 Map。
            final Set<SchedulingPipelinedRegion> regionToSchedule) { // 当前调度批次中已经确定要调度的区域集合
        return !regionToSchedule.contains(region) // 防止重复调度（本批次）
                && !scheduledRegions.contains(region) // 防止重复调度（全局）
                && areRegionInputsAllConsumable(region, consumableStatusCache, regionToSchedule);
    }

    private void scheduleRegion(final SchedulingPipelinedRegion region) {
        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");
        scheduledRegions.add(region);
        schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
    }
    // Flink 区域调度策略中用于**严格检查一个区域的所有阻塞性输入依赖是否都已满足（可消费）**的关键函数。
    // 只有当一个区域的所有阻塞性输入都就绪时，它才能被调度。
    // 这个方法通过区分跨区域多生产者依赖和外部单生产者依赖来执行检查。
    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,  // 参数 1： 正在被检查的区域对象。
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,  // 参数 2： 用于缓存分区组可消费状态的 Map。
            final Set<SchedulingPipelinedRegion> regionToSchedule) { // 参数 3： 当前调度批次中已确定要调度的区域集合。
        // 遍历当前区域所消耗的所有非流水线（即阻塞性）分区组。
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            // 分支 1：处理跨区域多生产者依赖。 检查当前分区组是否由多个上游区域共同生产。
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                // 如果是复杂依赖，则调用专门的方法进行检查。该方法会确认所有相关的生产者区域是否都已调度或将在本批次调度。
                if (!isDownstreamOfCrossRegionConsumedPartitionSchedulable(
                        consumedPartitionGroup, region, regionToSchedule)) {
                    return false;
                }
                // 分支 2：处理外部单生产者依赖。 检查当前分区组是否是由外部单个上游区域生产的（即不是本区域内部产生的，也不是多生产者产生的）。
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(
                        consumedPartitionGroup,
                        (group) ->
                                // 判断该分区组是否可消费
                                isDownstreamConsumedPartitionGroupSchedulable(
                                        group, regionToSchedule))) {
                    return false;
                }
            }
        }
        return true;
    }
    // Flink 区域调度策略中用于**检查一个下游消耗的分区组是否可调度（即其上游依赖是否满足）**的关键函数。
    // 检查一个外部的、单生产者依赖（ConsumedPartitionGroup）是否已就绪。它的判断逻辑根据依赖的数据传输类型（流水线式或阻塞式）而有所不同。
    private boolean isDownstreamConsumedPartitionGroupSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup, // 需要检查的输入分区组（依赖）。
            final Set<SchedulingPipelinedRegion> regionToSchedule) { // 当前调度批次中已确定要调度的区域集合。
        // 分支 1: 检查流水线式（PIPELINED）依赖。 如果分区组的类型允许流水线消费（即数据可以边产生边被下游任务消费），执行此分支。这种情况下，只需要确保上游生产者区域已启动或计划启动。
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                if (!scheduledRegions.contains(producerRegion)
                        && !regionToSchedule.contains(producerRegion)) {
                    return false;
                }
            }
        } else {
            // 分支 2: 检查阻塞式（BLOCKING）依赖。
            // 如果分区组的类型是严格阻塞（数据必须全部写入后才能被下游消费），执行此分支。
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (schedulingTopology.getResultPartition(partitionId).getState()
                        != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }
    // 专门处理那些由多个上游区域共同生产的阻塞性输入（ConsumedPartitionGroup）。
    // 由于这些输入可能包含来自外部区域和当前区域自身的输入，因此需要更复杂的逻辑来确保所有外部生产者都已就绪。
    private boolean isDownstreamOfCrossRegionConsumedPartitionSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup, // 需要检查的跨区域分区组。
            final SchedulingPipelinedRegion pipelinedRegion, // 正在尝试调度的下游消费区域。
            final Set<SchedulingPipelinedRegion> regionToSchedule) { // 当前调度批次中已确定要调度的区域集合。
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                // 判断当前分区 (partitionId) 的生产者是否在当前消费区域 (pipelinedRegion) 之外。
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)) {
                    // 找出这个外部分区的上游生产者区域。
                    SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                    // 检查这个外部生产者区域是否已就绪。必须满足：1. 不在本次部署批次中
                    // 2. 且尚未被全局调度/运行过
                    if (!regionToSchedule.contains(producerRegion)
                            && !scheduledRegions.contains(producerRegion)) {
                        return false;
                    }
                }
            }
        } else {
            // 遍历该分区组内的所有数据分区 ID。
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                        // 状态检查： 如果它是外部依赖，则检查该分区的状态。对于 BLOCKING 类型，必须要求其状态是 ALL_DATA_PRODUCED（所有数据已产出
                        && schedulingTopology.getResultPartition(partitionId).getState()
                                != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }
    // 判断一个消耗分区组（ConsumedPartitionGroup）是否由当前考察的流水线区域（SchedulingPipelinedRegion）之外的区域所生产。
    // 接收一个消耗分区组 (consumedPartitionGroup) 和一个流水线区域 (pipelinedRegion) 作为输入，并返回一个布尔值。
    private boolean isExternalConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    // 判断一个中间结果分区（IntermediateResultPartitionID）是否由当前考察的流水线区域（SchedulingPipelinedRegion）之外的区域所生产。
    // 接收一个中间结果分区 ID (partitionId) 和一个流水线区域对象 (pipelinedRegion) 作为输入，并返回一个布尔值。
    private boolean isExternalConsumedPartition(
            IntermediateResultPartitionID partitionId, SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
