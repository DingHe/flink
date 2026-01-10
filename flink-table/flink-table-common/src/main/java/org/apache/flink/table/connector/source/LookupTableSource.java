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
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/**
 * A {@link DynamicTableSource} that looks up rows of an external storage system by one or more keys
 * during runtime.
 *
 * <p>Compared to {@link ScanTableSource}, the source does not have to read the entire table and can
 * lazily fetch individual values from a (possibly continuously changing) external table when
 * necessary.
 *
 * <p>Note: Compared to {@link ScanTableSource}, a {@link LookupTableSource} does only support
 * emitting insert-only changes currently (see also {@link RowKind}). Further abilities are not
 * supported.
 *
 * <p>In the last step, the planner will call {@link #getLookupRuntimeProvider(LookupContext)} for
 * obtaining a provider of runtime implementation. The key fields that are required to perform a
 * lookup are derived from a query by the planner and will be provided in the given {@link
 * LookupContext#getKeys()}. The values for those key fields are passed during runtime.
 */
// Flink 中用于描述按键查找外部存储系统行的能力的数据源接口。它扩展了 DynamicTableSource 接口，
// 但与 ScanTableSource 的全量扫描不同，LookupTableSource 专门用于按需、懒惰地查询单个或少量数据
// 支持维表 Join：它主要用于实现维表 Join（DIM JOIN）。
// 在流处理中，当一条主数据流的记录到达时，LookupTableSource 可以根据记录中的键，从外部维表（例如 HBase, MySQL, Redis）中高效地查询出对应的维表数据

@PublicEvolving
public interface LookupTableSource extends DynamicTableSource {

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There exist different interfaces for runtime implementation which is why {@link
     * LookupRuntimeProvider} serves as the base interface.
     *
     * <p>Independent of the provider interface, a source implementation can work on either
     * arbitrary objects or internal data structures (see {@link org.apache.flink.table.data} for
     * more information).
     *
     * <p>The given {@link LookupContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * @see LookupFunctionProvider
     * @see AsyncLookupFunctionProvider
     */
    //返回一个运行时提供者（LookupRuntimeProvider），这个提供者包含了实际执行数据查找的逻辑
    LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context);

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /**
     * Context for creating runtime implementation via a {@link LookupRuntimeProvider}.
     *
     * <p>It offers utilities by the planner for creating runtime implementation with minimal
     * dependencies to internal data structures.
     *
     * <p>Methods should be called in {@link #getLookupRuntimeProvider(LookupContext)}. Returned
     * instances that are {@link Serializable} can be directly passed into the runtime
     * implementation class.
     */
    @PublicEvolving
    interface LookupContext extends DynamicTableSource.Context {

        /**
         * Returns an array of key index paths that should be used during the lookup. The indices
         * are 0-based and support composite keys within (possibly nested) structures.
         *
         * <p>For example, given a table with data type {@code ROW < i INT, s STRING, r ROW < i2
         * INT, s2 STRING > >}, this method would return {@code [[0], [2, 1]]} when {@code i} and
         * {@code s2} are used for performing a lookup.
         *
         * @return array of key index paths
         */
        //它返回一个二维数组，表示用于查找的键字段索引路径。这个路径由 Flink 规划器根据 SQL 查询语句中的 Join 条件自动推断并提供。
        // 例如，对于一个嵌套的行类型，[[0], [2, 1]] 表示查找键是第一个字段（索引为0）和第三个字段中的第二个嵌套字段（索引为1）
        int[][] getKeys();
    }

    /**
     * Provides actual runtime implementation for reading the data.
     *
     * <p>There exist different interfaces for runtime implementation which is why {@link
     * LookupRuntimeProvider} serves as the base interface.
     *
     * @see LookupFunctionProvider
     * @see AsyncLookupFunctionProvider
     */
    @PublicEvolving
    interface LookupRuntimeProvider {
        // marker interface
    }
}
