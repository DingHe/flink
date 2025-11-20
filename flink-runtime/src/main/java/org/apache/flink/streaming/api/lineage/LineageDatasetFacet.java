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

/** Facet interface for dataset. */
// LineageDatasetFacet 是 Flink 数据血缘（Lineage）系统中的一个接口，它代表了外部数据集（LineageDataset）的额外元数据片段或特性（Facet）
// 元数据封装： 它提供了一种标准化的方式，将关于外部数据集（如 Kafka Topic、数据库表、文件）的特定、非核心的附加信息封装起来。
@PublicEvolving
public interface LineageDatasetFacet {
    /** Name for the facet which will be used as key in facets of LineageDataset. */
    // 获取 Facet 名称。 返回此元数据片段的唯一标识符（名称）。
    String name();
}
