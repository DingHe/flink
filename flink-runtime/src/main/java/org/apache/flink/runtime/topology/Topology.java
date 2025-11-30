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

package org.apache.flink.runtime.topology;

/** Extends the {@link BaseTopology} by pipelined regions. */
// Flink 运行时对“拓扑结构”定义的进一步扩展。如果说 BaseTopology 只是定义了点（Vertex）和线（Result），
// 那么 Topology 接口则引入了面（Region） 的概念。
// 在 BaseTopology 的基础上，增加了对 PipelinedRegion（流水线区域） 的支持
// 继承自 BaseTopology。这意味着它不仅拥有获取所有顶点的能力，还拥有获取所有“流水线区域”的能力。
// 结构化视图： 在 Flink 的执行层面，一个复杂的作业图（DAG）会被切割成多个 PipelinedRegion。这个接口提供了这种“结构化”的视图。
// 调度与执行的核心： 调度器（Scheduler）使用这个接口来理解哪些任务是可以同时调度、流式传输数据的（在一个 Region 内），哪些任务之间是有阻塞边界的（跨 Region）。
public interface Topology<
                VID extends VertexID,
                RID extends ResultID,
                V extends Vertex<VID, RID, V, R>,
                R extends Result<VID, RID, V, R>,
                PR extends PipelinedRegion<VID, RID, V, R>> // PR	区域类型	PipelinedRegion	(新增) 表示该拓扑中使用的具体的流水线区域实现类。
        extends BaseTopology<VID, RID, V, R> {

    /**
     * Returns all pipelined regions in this topology.
     *
     * @return Iterable over pipelined regions in this topology
     */
    // 获取当前拓扑结构中所有的流水线区域。
    // 调度器通常会遍历这些 Region。对于流式作业，
    // 通常一个 Region 内的所有 Task 需要被一起调度（Slot Sharing 或者 Co-location），以确保流水线不会断裂。此方法提供了这种遍历能力。
    Iterable<? extends PR> getAllPipelinedRegions();

    /**
     * The pipelined region for a specified vertex.
     *
     * @param vertexId the vertex id identifying the vertex for which the pipelined region should be
     *     returned
     * @return the pipelined region of the vertex
     * @throws IllegalArgumentException if there is no vertex in this topology with the specified
     *     vertex id
     */
    // 根据顶点 ID，查找该顶点所属的流水线区域。
    default PR getPipelinedRegionOfVertex(VID vertexId) {
        throw new UnsupportedOperationException();
    }
}
