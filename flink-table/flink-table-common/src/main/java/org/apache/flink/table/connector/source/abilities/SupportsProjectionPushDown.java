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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;

/**
 * Enables to push down a (possibly nested) projection into a {@link ScanTableSource}.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * CREATE TABLE t (i INT, r ROW < d DOUBLE, b BOOLEAN>, s STRING);
 * SELECT s, r.d FROM t;
 * }</pre>
 *
 * <p>In the above example, {@code r.d} and {@code s} are required fields. Other fields can be
 * skipped in a projection. Compared to table's schema, fields are reordered.
 *
 * <p>By default, if this interface is not implemented, a projection is applied in a subsequent
 * operation after the source.
 *
 * <p>For efficiency, a source can push a projection further down in order to be close to the actual
 * data generation. A projection is only selecting fields that are used by a query (possibly in a
 * different field order). It does not contain any computation. A projection can either be performed
 * on the fields of the top-level row only or consider nested fields as well (see {@link
 * #supportsNestedProjection()}).
 *
 * @see Projection
 * @see ProjectedRowData
 */
// 主要作用是将查询中需要的列（字段）信息下推到物理数据源层。
// 默认行为（不实现此接口）：Flink 会读取该表的所有列，然后在数据进入 Flink 算子后，通过一个 Calc（投影）算子把不需要的列过滤掉。这会造成带宽和 I/O 的浪费。
// 优化行为（实现此接口）：Flink 优化器（Planner）会计算出查询真正需要的列索引，并将其直接告诉物理 Source。Source 在读取数据时（如读取 Parquet 文件或查询数据库）只读取这些特定列，从而显著提升性能。

@PublicEvolving
public interface SupportsProjectionPushDown {

    /** Returns whether this source supports nested projection. */
    // 是否支持嵌套投影
    // 告知 Flink 优化器，该数据源是否能够处理对嵌套字段（如 ROW、STRUCT 类型内部的子字段）的投影。
    boolean supportsNestedProjection();

    /** @deprecated Please implement {@link #applyProjection(int[][], DataType)} */
    // int[][] projectedFields（投影字段的索引路径）
    @Deprecated
    default void applyProjection(int[][] projectedFields) {
        throw new UnsupportedOperationException(
                "No implementation provided for SupportsProjectionPushDown. "
                        + "Please implement SupportsProjectionPushDown#applyProjection(int[][], DataType)");
    }

    /**
     * Provides the field index paths that should be used for a projection. The indices are 0-based
     * and support fields within (possibly nested) structures if this is enabled via {@link
     * #supportsNestedProjection()}.
     *
     * <p>In the example mentioned in {@link SupportsProjectionPushDown}, this method would receive:
     *
     * <ul>
     *   <li>{@code [[2], [1]]} which is equivalent to {@code [["s"], ["r"]]} if {@link
     *       #supportsNestedProjection()} returns false.
     *   <li>{@code [[2], [1, 0]]} which is equivalent to {@code [["s"], ["r", "d"]]]} if {@link
     *       #supportsNestedProjection()} returns true.
     * </ul>
     *
     * <p>Note: Use the passed data type instead of {@link ResolvedSchema#toPhysicalRowDataType()}
     * for describing the final output data type when creating {@link TypeInformation}.
     *
     * @param projectedFields field index paths of all fields that must be present in the physically
     *     produced data
     * @param producedDataType the final output type of the source, with the projection applied
     */
    // projectedFields：一个二维数组，代表字段的索引路径（基于 0）
    // 例如：[[2], [1, 0]]。
    // [2]：表示取原始 Schema 中索引为 2 的顶级字段（如例子中的 s）
    // [1, 0]：表示取索引为 1 的顶级字段（如 r）内部索引为 0 的子字段（如 d）
    // producedDataType：应用投影后，Source 最终生成的物理输出数据类型（包含正确的字段名和类型）。
    default void applyProjection(int[][] projectedFields, DataType producedDataType) {
        applyProjection(projectedFields);
    }
}
