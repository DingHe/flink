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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/**
 * A {@link DynamicTableSource} that scans all rows from an external storage system during runtime.
 *
 * <p>The scanned rows don't have to contain only insertions but can also contain updates and
 * deletions. Thus, the table source can be used to read a (finite or infinite) changelog. The given
 * {@link ChangelogMode} indicates the set of changes that the planner can expect during runtime.
 *
 * <p>For regular batch scenarios, the source can emit a bounded stream of insert-only rows.
 *
 * <p>For regular streaming scenarios, the source can emit an unbounded stream of insert-only rows.
 *
 * <p>For change data capture (CDC) scenarios, the source can emit bounded or unbounded streams with
 * insert, update, and delete rows. See also {@link RowKind}.
 *
 * <p>A {@link ScanTableSource} can implement the following abilities that might mutate an instance
 * during planning:
 *
 * <ul>
 *   <li>{@link SupportsWatermarkPushDown}
 *   <li>{@link SupportsSourceWatermark}
 *   <li>{@link SupportsFilterPushDown}
 *   <li>{@link SupportsAggregatePushDown}
 *   <li>{@link SupportsProjectionPushDown}
 *   <li>{@link SupportsPartitionPushDown}
 *   <li>{@link SupportsReadingMetadata}
 * </ul>
 *
 * <p>In the last step, the planner will call {@link #getScanRuntimeProvider(ScanContext)} for
 * obtaining a provider of runtime implementation.
 */
// Flink 中用于描述全量扫描或连续读取变更日志能力的数据源接口。
// 它扩展了 DynamicTableSource 接口，专门用于那些需要读取整个表内容（无论是有界还是无界）的场景
// 定义数据源的扫描能力：告诉 Flink 规划器，该数据源可以像批处理一样扫描所有数据，也可以像流处理一样连续产生数据
// 支持变更日志：它能够处理包含插入（INSERT）、更新（UPDATE）和删除（DELETE）等多种变更类型的行，这对于变更数据捕获（CDC）场景至关重要

@PublicEvolving
public interface ScanTableSource extends DynamicTableSource {

    /**
     * Returns the set of changes that the planner can expect during runtime.
     *
     * @see RowKind
     */
    //表示该数据源在运行时可以提供的变更日志类型
    ChangelogMode getChangelogMode();

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * ScanRuntimeProvider} serves as the base interface. Concrete {@link ScanRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a source
     * implementation emits internal data structures (see {@link
     * org.apache.flink.table.data.RowData} for more information).
     *
     * <p>The given {@link ScanContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * <p>{@link SourceProvider} is the recommended core interface. {@code SourceFunctionProvider}
     * in {@code flink-table-api-java-bridge} and {@link InputFormatProvider} are available for
     * backwards compatibility.
     *
     * @see SourceProvider
     */
    //返回一个运行时提供者（ScanRuntimeProvider），这个提供者包含了实际执行数据扫描的逻辑
    ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext);

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /**
     * Context for creating runtime implementation via a {@link ScanRuntimeProvider}.
     *
     * <p>It offers utilities by the planner for creating runtime implementation with minimal
     * dependencies to internal data structures.
     *
     * <p>Methods should be called in {@link #getScanRuntimeProvider(ScanContext)}. The returned
     * instances are {@link Serializable} and can be directly passed into the runtime implementation
     * class.
     */
    @PublicEvolving
    interface ScanContext extends DynamicTableSource.Context {
        // may introduce scan specific methods in the future
    }

    /**
     * Provides actual runtime implementation for reading the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * ScanRuntimeProvider} serves as the base interface. Concrete {@link ScanRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>{@link SourceProvider} is the recommended core interface. {@code SourceFunctionProvider}
     * in {@code flink-table-api-java-bridge} and {@link InputFormatProvider} are available for
     * backwards compatibility.
     */
    @PublicEvolving
    interface ScanRuntimeProvider {

        /** Returns whether the data is bounded or not. */
        boolean isBounded();
    }
}
