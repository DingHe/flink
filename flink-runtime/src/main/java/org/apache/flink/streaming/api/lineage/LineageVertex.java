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
 *
 */

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

/**
 * Lineage vertex represents the connectors in lineage graph, including source {@link
 * SourceLineageVertex} and sink.
 */
// LineageVertex 是 Flink 中用于描述数据血缘图 (Lineage Graph) 上的顶点的接口。
// 在 Flink 的数据血缘（Lineage）追踪系统中，这个接口扮演着关键角色，主要用于标识数据流图中的连接器 (Connectors)，特别是：
// 数据源 (Source)： 数据的起始点。
// 数据汇 (Sink)： 数据的终点。
// 这些连接器是 Flink Job 与外部数据系统（如 Kafka topic、文件、数据库表等）交互的边界。
// 通过 LineageVertex，外部系统可以了解到 Flink Job 正在读取或写入哪些具体的数据集。
@PublicEvolving
public interface LineageVertex {
    /* List of input (for source) or output (for sink) datasets interacted with by the connector */
    // 获取相关数据集列表。
    // 这是 LineageVertex 接口中定义的唯一方法，用于返回此顶点（连接器）所交互的外部数据集列表。
    // 对于 Source (数据源)： 返回它从哪些外部数据集读取数据。* 对于 Sink (数据汇)： 返回它将数据写入到哪些外部数据集。
    List<LineageDataset> datasets();
}
