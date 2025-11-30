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

package org.apache.flink.runtime.topology;

/**
 * A pipelined region is a set of vertices connected via pipelined data exchanges.
 *
 * @param <VID> the type of the vertex ids
 * @param <RID> the type of the result ids
 * @param <V> the type of the vertices
 * @param <R> the type of the result
 */
// PipelinedRegion（流水线区域）是 Flink 调度和故障恢复机制中的一个核心逻辑单元
// 它代表了拓扑图（Topology）中的一个子图。这个子图中的所有顶点（Vertex）通过流水线式数据交换（Pipelined Data Exchanges） 相互连接。
// 区域的边界通常由阻塞式数据交换（Blocking Data Exchanges） 决定。
// 内部： 区域内的算子之间数据是流式的（Streaming），上游发一条，下游收一条，必须同时在线（Simultaneous deployment）。
// 外部： 区域之间的数据通常是落盘的（Batch Shuffle），上游完全运行结束后，下游才开始运行。
// 调度策略（Scheduling）： 在 Flink 的调度器中，PipelinedRegion 通常是最小的调度单元。
// 为了避免死锁（Backpressure导致），属于同一个 PipelinedRegion 的所有任务（Task）通常需要被同时调度到 TaskManager 上执行。
// 容错（Failover）： 在细粒度恢复（Fine-Grained Recovery）场景下，如果一个任务失败，通常需要重启整个 PipelinedRegion，因为区域内的中间数据没有持久化，无法单独恢复某个中间节点。
// 它是 Flink 将复杂的作业图（JobGraph）切分成多个可以独立调度、独立恢复的“岛屿”的抽象。
public interface PipelinedRegion<
        VID extends VertexID,
        RID extends ResultID,
        V extends Vertex<VID, RID, V, R>, // 顶点类型。具体的计算节点对象。
        R extends Result<VID, RID, V, R>> { // 结果类型。具体的输出结果对象。

    /**
     * Returns vertices that are in this pipelined region.
     *
     * @return Iterable over all vertices in this pipelined region
     */
    // 获取属于该流水线区域的所有顶点。
    // 调度器在部署这个 Region 时，会调用此方法拿到所有需要部署的任务顶点，然后为它们申请资源。
    Iterable<? extends V> getVertices();

    /**
     * Returns the vertex with the specified vertex id.
     *
     * @param vertexId the vertex id used to look up the vertex
     * @return the vertex with the specified id
     * @throws IllegalArgumentException if there is no vertex in this pipelined region with the
     *     specified vertex id
     */
    // 根据 ID 查找区域内的特定顶点。
    V getVertex(VID vertexId);

    /**
     * Returns whether the vertex is in this pipelined region or not.
     *
     * @param vertexId the vertex id used to look up
     * @return the vertex is in this pipelined region or not
     */
    // 检查某个顶点是否属于该区域。
    boolean contains(VID vertexId);
}
