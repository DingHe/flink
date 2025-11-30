/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.topology.Result;
import org.apache.flink.runtime.topology.Vertex;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Common utils for computing pipelined regions. */
// 专门用于计算作业图中的流水线区域（Pipelined Regions）
// 核心职责是实现一种图算法，用于将 Flink 的逻辑或执行拓扑（基于 Vertex 和 Result 接口）划分为一系列互不相交的流水线区域（Pipelined Regions）
// 定义故障隔离边界：在 Flink 的现代调度器（如 AdaptiveScheduler）和区域故障恢复机制中，流水线区域是故障恢复的最小单元。如果一个任务失败，只有它所在的整个区域需要被重启和恢复。
// 确定区域成员：它通过遍历拓扑结构中所有必须保持流水线连接（Must-Be-Pipelined）的边，将通过这些边相连的顶点合并到同一个区域。
// 图合并逻辑：它使用并查集（Union-Find）或类似的概念，通过 VertexGroupComputeUtil.mergeVertexGroups 方法，将通过流水线边连接的顶点的区域进行合并。
// 简而言之：它负责回答一个关键问题：在一个 Flink 作业中，哪些任务集合是紧密耦合在一起的，如果其中一个任务失败，所有这些任务都必须被重启？这个集合就是流水线区域。
public final class PipelinedRegionComputeUtil {
    // 根据“必须流水线传输（pipelined）”的输入依赖，把一组按拓扑排序的顶点分成若干 原始（raw）region（每个 region 是一组顶点）。
    // 当一个顶点以必须流水线方式消费某个结果（即消费方必须和生产方在同一流水线区域），消费者与生产者所在的 region 要合并。
    // 最终返回一个映射：每个顶点指向它所属的 region（region 用 Set<V> 表示）
    static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>>
            Map<V, Set<V>> buildRawRegions(
                    // 按拓扑排序的顶点集合。
                    // 输入，算法按照数据流的顺序（从源到汇）进行迭代，确保在处理消费顶点时，其上游的生产顶点已被处理。
                    // 拓扑顺序保证：当处理某个顶点时，它的所有生产者（上游顶点）已经被遍历并且已有 region 信息。
                    final Iterable<? extends V> topologicallySortedVertices,
                    // 一个函数式接口（Function<V, Iterable<R>>），
                    // 对给定顶点返回它必须以流水线方式消费的输入 Result 列表。也就是说，只对这些必须 pipelined 的边才触发 region 合并；非必须流水线的边不影响 region 划分。
                    final Function<V, Iterable<R>> getMustBePipelinedConsumedResults) {
        // 键是顶点 V，值是该顶点所属的 region（即 Set<V>）
        final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (V vertex : topologicallySortedVertices) {
            Set<V> currentRegion = new HashSet<>();
            // 先把顶点加入到 currentRegion 并把 vertex -> currentRegion 放进 vertexToRegion。
            currentRegion.add(vertex);
            vertexToRegion.put(vertex, currentRegion);

            // Each vertex connected through not mustBePipelined consumingConstraint is considered
            // as a
            // single region.
            // 只有“必须流水线消费”的入边会导致合并
            for (R consumedResult : getMustBePipelinedConsumedResults.apply(vertex)) {
                // 从被消费的 Result 中取出生产者顶点（即上游顶点）
                final V producerVertex = consumedResult.getProducer();
                // 根据生产者顶点从 vertexToRegion 中取得它当前所属的 region（producerRegion）
                final Set<V> producerRegion = vertexToRegion.get(producerVertex);

                if (producerRegion == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getId()
                                    + " failover region is null"
                                    + " while calculating failover region for the consumer task "
                                    + vertex.getId()
                                    + ". This should be a failover region building bug.");
                }

                // check if it is the same as the producer region, if so skip the merge
                // this check can significantly reduce compute complexity in All-to-All
                // PIPELINED edge case
                // 当 currentRegion 与 producerRegion 不同，需要把两者合并成一个 region。
                if (currentRegion != producerRegion) {
                    currentRegion =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentRegion, producerRegion, vertexToRegion);
                }
            }
        }

        return vertexToRegion;
    }

    private PipelinedRegionComputeUtil() {}
}
