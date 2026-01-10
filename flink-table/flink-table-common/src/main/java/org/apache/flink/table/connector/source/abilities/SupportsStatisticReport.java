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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.plan.stats.TableStats;

/**
 * Enables to report the estimated statistics provided by the {@link DynamicTableSource}.
 *
 * <p>Statistics are one of the most important inputs to the optimizer cost model, which will
 * generate the most effective execution plan with the lowest cost among all considered candidate
 * plans.
 *
 * <p>This interface is used to compensate for the missing of statistics in the catalog. The planner
 * will call {@link #reportStatistics} method when the planner detects the statistics from catalog
 * is unknown. Therefore, the method will be executed as needed.
 *
 * <p>Note: This method is called at plan optimization phase, the implementation of this interface
 * should be as light as possible, but more complete information.
 */
// SupportsStatisticReport 的主要作用是为 Flink 优化器提供精细化的成本估算依据。
// 在 SQL 优化过程中，Flink 的 CBO（基于成本的优化器，Cost-Based Optimizer） 需要决定如何连接表、是否使用索引或选择哪种 Join 算法。
// 背景问题：通常情况下，统计信息（如行数、字段的最大/最小值、空值数量等）存储在外部 Catalog（如 Hive Metastore）中。但很多时候，Catalog 里的信息是过时的、缺失的，或者某些数据源根本没有 Catalog。
// 接口方案：如果数据源实现了此接口，当优化器发现 Catalog 中没有统计信息时，会直接调用该接口询问 Source：“你预估你现在有多少条数据？”。
@PublicEvolving
public interface SupportsStatisticReport {

    /**
     * Returns the estimated statistics of this {@link DynamicTableSource}, else {@link
     * TableStats#UNKNOWN} if some situations are not supported or cannot be handled.
     */
    TableStats reportStatistics();
}
