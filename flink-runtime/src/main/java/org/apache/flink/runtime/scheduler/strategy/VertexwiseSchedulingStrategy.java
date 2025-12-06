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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** 仅仅支持批处理
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of vertex (which
 * indicates this strategy only supports batch jobs). Note that this strategy implements {@link
 * SchedulingTopologyListener}, so it can handle the updates of scheduling topology.
 */
// (基于顶点的调度策略) 是 Flink 早期和批处理作业中使用的经典调度策略。
// 调度粒度：它以单个执行顶点（Execution Vertex，即作业图中的一个任务实例）为最小调度单位。
// 适用场景：该策略仅支持批处理作业。对于批处理作业，任务必须等待其所有上游输入数据都完成产出后才能启动（严格的阻塞依赖）。
// 工作方式：它通过检查任务的输入依赖是否满足（即所有上游生产者是否已完成），来决定哪些任务可以启动。它在拓扑结构中追踪依赖，并以广度优先的方式，一次性地调度所有准备就绪的任务批次。
public class VertexwiseSchedulingStrategy
        implements SchedulingStrategy, SchedulingTopologyListener {

    private static final Logger LOG = LoggerFactory.getLogger(VertexwiseSchedulingStrategy.class);
    // 调度操作接口。
    // 提供了将任务部署到执行环境的方法（如分配槽位和部署任务），是策略与 Flink 调度器核心交互的接口。
    private final SchedulerOperations schedulerOperations;
    // 调度拓扑结构。
    // 包含作业中所有任务（执行顶点）和它们之间的数据依赖关系（结果分区）。策略通过它查询拓扑信息。
    private final SchedulingTopology schedulingTopology;
    // 新增顶点集合
    // 暂存通过 notifySchedulingTopologyUpdated 方法通知的新加入的执行顶点 ID，这些顶点将在下一次 maybeScheduleVertices 调用中被纳入考虑
    private final Set<ExecutionVertexID> newVertices = new HashSet<>();
    // 已调度顶点集合。
    // 记录历史上所有已被成功调度（已尝试分配槽位和部署）的执行顶点 ID。用于避免重复调度已完成或正在运行的任务。
    private final Set<ExecutionVertexID> scheduledVertices = new HashSet<>();
    // 输入可消费性决策器
    // 封装了判断一个任务的输入依赖（ConsumedPartitionGroup）是否可消费的复杂逻辑，特别是处理阻塞性和流水线依赖的规则。
    private final InputConsumableDecider inputConsumableDecider;

    public VertexwiseSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology,
            final InputConsumableDecider.Factory inputConsumableDeciderFactory) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);
        this.inputConsumableDecider =
                inputConsumableDeciderFactory.createInstance(
                        schedulingTopology, scheduledVertices::contains);
        LOG.info(
                "Using InputConsumableDecider {} for VertexwiseSchedulingStrategy.",
                inputConsumableDecider.getClass().getName());
        schedulingTopology.registerSchedulingTopologyListener(this);
    }
    // 核心作用是识别作业中的所有源头任务（Source Vertices），并触发对它们的首次调度尝试。
    @Override
    public void startScheduling() {
        Set<ExecutionVertexID> sourceVertices =
                IterableUtils.toStream(schedulingTopology.getVertices())
                        // 过滤操作： 对流中的顶点进行过滤。
                        // 保留那些消耗分区组（ConsumedPartitionGroup）为空的顶点。
                        // 在 Flink 拓扑中，没有输入依赖（即没有需要等待上游数据）的任务就是源头任务。
                        .filter(vertex -> vertex.getConsumedPartitionGroups().isEmpty())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        maybeScheduleVertices(sourceVertices);
    }

    @Override
    public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
        scheduledVertices.removeAll(verticesToRestart);
        maybeScheduleVertices(verticesToRestart);
    }

    @Override
    public void onExecutionStateChange(
            ExecutionVertexID executionVertexId, ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            SchedulingExecutionVertex executionVertex =
                    schedulingTopology.getVertex(executionVertexId);

            Set<ExecutionVertexID> consumerVertices =
                    IterableUtils.toStream(executionVertex.getProducedResults())
                            .map(SchedulingResultPartition::getConsumerVertexGroups)
                            .flatMap(Collection::stream)
                            .filter(
                                    group ->
                                            inputConsumableDecider
                                                    .isConsumableBasedOnFinishedProducers(
                                                            group.getConsumedPartitionGroup()))
                            .flatMap(IterableUtils::toStream)
                            .collect(Collectors.toSet());

            maybeScheduleVertices(consumerVertices);
        }
    }

    @Override
    public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {}

    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {
        checkState(schedulingTopology == this.schedulingTopology);
        newVertices.addAll(newExecutionVertices);
    }
    // 从一组给定的触发任务（Vertices）开始，递归地检查所有可调度的任务，收集它们，并最终以正确的拓扑顺序进行部署。
    // 这是一个基于依赖的广度优先调度过程。
    private void maybeScheduleVertices(final Set<ExecutionVertexID> vertices) {
        Set<ExecutionVertexID> allCandidates;
        // 检查是否存在新加入的执行顶点（通过拓扑更新通知）。
        if (newVertices.isEmpty()) {
            allCandidates = vertices;
        } else {
            allCandidates = new HashSet<>(vertices);
            allCandidates.addAll(newVertices);
            newVertices.clear();
        }

        final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();
        // 递归检查与收集可调度任务
        Set<ExecutionVertexID> nextVertices = allCandidates;
        // 只要 nextVertices 集合不为空（即还有任务需要检查），循环就继续执行。这实现了广度优先的依赖检查。
        while (!nextVertices.isEmpty()) {
            // 1. 检查 nextVertices 中的任务，将所有满足依赖的可调度任务添加到 verticesToSchedule。
            // 2. 返回这些被调度任务的下游任务集合，作为新的 nextVertices，供下一轮循环检查。
            nextVertices = addToScheduleAndGetVertices(nextVertices, verticesToSchedule);
        }
        // 首先对 verticesToSchedule 中的任务进行拓扑排序，然后逐一调用 schedulerOperations.allocateSlotsAndDeploy 提交任务部署请求。
        scheduleVerticesOneByOne(verticesToSchedule);
        scheduledVertices.addAll(verticesToSchedule);
    }

    @Override
    public void scheduleAllVerticesIfPossible() {
        newVertices.clear();
        Set<ExecutionVertexID> verticesToSchedule =
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .filter(vertex -> !vertex.getState().equals(ExecutionState.FINISHED))
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        maybeScheduleVertices(verticesToSchedule);
    }
    // 执行一轮广度优先的依赖检查：遍历当前的候选任务集合 (currentVertices)，将满足调度条件的任务添加到部署列表 (verticesToSchedule) 中，
    // 并收集这些被调度任务的下游任务作为下一轮检查的候选。
    private Set<ExecutionVertexID> addToScheduleAndGetVertices(
            Set<ExecutionVertexID> currentVertices,
            Set<ExecutionVertexID> verticesToSchedule) {
        Set<ExecutionVertexID> nextVertices = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new IdentityHashMap<>();
        final Set<ConsumerVertexGroup> visitedConsumerVertexGroup =
                Collections.newSetFromMap(new IdentityHashMap<>());

        for (ExecutionVertexID currentVertex : currentVertices) {
            if (isVertexSchedulable(currentVertex, consumableStatusCache, verticesToSchedule)) {
                verticesToSchedule.add(currentVertex);
                Set<ConsumerVertexGroup> canBePipelinedConsumerVertexGroups =
                        IterableUtils.toStream(
                                        schedulingTopology
                                                .getVertex(currentVertex)
                                                .getProducedResults())
                                .map(SchedulingResultPartition::getConsumerVertexGroups)
                                .flatMap(Collection::stream)
                                .filter(
                                        (consumerVertexGroup) ->
                                                consumerVertexGroup
                                                        .getResultPartitionType()
                                                        .canBePipelinedConsumed())
                                .collect(Collectors.toSet());
                for (ConsumerVertexGroup consumerVertexGroup : canBePipelinedConsumerVertexGroups) {
                    if (!visitedConsumerVertexGroup.contains(consumerVertexGroup)) {
                        visitedConsumerVertexGroup.add(consumerVertexGroup);
                        nextVertices.addAll(
                                IterableUtils.toStream(consumerVertexGroup)
                                        .collect(Collectors.toSet()));
                    }
                }
            }
        }
        return nextVertices;
    }

    private boolean isVertexSchedulable(
            final ExecutionVertexID vertex,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<ExecutionVertexID> verticesToSchedule) {
        return !verticesToSchedule.contains(vertex)
                && !scheduledVertices.contains(vertex)
                && inputConsumableDecider.isInputConsumable(
                        schedulingTopology.getVertex(vertex),
                        verticesToSchedule,
                        consumableStatusCache);
    }

    private void scheduleVerticesOneByOne(final Set<ExecutionVertexID> verticesToSchedule) {
        if (verticesToSchedule.isEmpty()) {
            return;
        }
        final List<ExecutionVertexID> sortedVerticesToSchedule =
                SchedulingStrategyUtils.sortExecutionVerticesInTopologicalOrder(
                        schedulingTopology, verticesToSchedule);

        sortedVerticesToSchedule.forEach(
                id -> schedulerOperations.allocateSlotsAndDeploy(Collections.singletonList(id)));
    }

    /** The factory for creating {@link VertexwiseSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        private final InputConsumableDecider.Factory inputConsumableDeciderFactory;

        public Factory(InputConsumableDecider.Factory inputConsumableDeciderFactory) {
            this.inputConsumableDeciderFactory = inputConsumableDeciderFactory;
        }

        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new VertexwiseSchedulingStrategy(
                    schedulerOperations, schedulingTopology, inputConsumableDeciderFactory);
        }
    }
}
