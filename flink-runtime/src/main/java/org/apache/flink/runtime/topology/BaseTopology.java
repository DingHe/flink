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

/**
 * Base topology for all logical and execution topologies. A topology consists of {@link Vertex} and
 * {@link Result}.
 */
// BaseTopology 是 Flink 运行时拓扑结构的基础接口。
// 它定义了所有逻辑拓扑 (Logical Topology) 和执行拓扑 (Execution Topology) 都应遵循的基本契约。
// 定义拓扑结构的基础骨架： 它提供了一个抽象，用于表示 Flink 作业的计算图。一个拓扑结构（Topology）由两种基本元素组成：
// 顶点 (Vertex): 代表计算操作或任务（例如，一个算子，如 Map、Filter 或 Sink）。
// 结果 (Result): 代表数据流或数据交换的连接点（连接两个顶点的数据通道）。
// 提供遍历顶点的通用方法： 它定义了一个获取所有顶点的通用 API，特别是要求这些顶点是拓扑排序的（Topologically Sorted），
// 这意味着顶点的返回顺序遵循数据流的方向，上游的顶点先于下游的顶点。
public interface BaseTopology<
        VID extends VertexID, // Vertex ID (顶点 ID)：表示拓扑中一个顶点的唯一标识符类型。
        RID extends ResultID, // Result ID (结果 ID)：表示拓扑中一个结果/数据流的唯一标识符类型
        V extends Vertex<VID, RID, V, R>, // Vertex (顶点)：表示拓扑中计算节点的类型。
        R extends Result<VID, RID, V, R>> { // Result (结果)：表示拓扑中数据流的类型。

    /**
     * Returns an iterable over all vertices, topologically sorted.
     *
     * @return topologically sorted iterable over all vertices
     */
    // 获取所有顶点（拓扑排序）
    // 保证返回的顶点是拓扑排序的。在 Flink 中，这意味着你可以按返回的顺序依次处理顶点，
    // 保证在处理任何一个顶点时，其所有输入依赖（上游顶点）都已经被考虑或处理。
    Iterable<? extends V> getVertices();
}
