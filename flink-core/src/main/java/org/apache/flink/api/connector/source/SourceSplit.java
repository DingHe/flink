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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

/** An interface for all the Split types to extend. */
// SourceSplit 接口是 Flink 统一 Source API 的基础组件之一，它代表了数据源中的一个逻辑分片（Logical Shard）。
// 抽象数据单元： 它是对外部数据源中可独立读取的数据片段的抽象。
// 例如，对于文件系统，一个 Split 可以是一个文件路径；对于 Kafka，它可以是一个分区（Topic Partition）。
// 分片分配基础： Flink 的 SplitEnumerator 发现这些 SourceSplit 并将它们分配给下游的 SourceReader 进行处理。
// 提供唯一标识： 每个 SourceSplit 必须提供一个唯一的 ID，用于在 Flink 运行时环境中识别、追踪和管理该分片的状态。
// SourceSplit 是 Flink 数据源中最小的可分配工作单元的抽象，是实现并行读取和容错的基础。
@Public
public interface SourceSplit {

    /**
     * Get the split id of this source split.
     *
     * @return id of this source split.
     */
    // 返回此数据分片的唯一标识符
    // 这个 ID 必须是稳定的，且在整个 Source 中是唯一的，FLink 依赖它来跟踪分片的处理进度和状态。
    String splitId();
}
