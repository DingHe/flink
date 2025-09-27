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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Source of a dynamic table from an external storage system.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion. By definition, a dynamic table can change over time.
 *
 * <p>When reading a dynamic table, the content can either be considered as:
 *
 * <ul>
 *   <li>A changelog (finite or infinite) for which all changes are consumed continuously until the
 *       changelog is exhausted. See {@link ScanTableSource} for more information.
 *   <li>A continuously changing or very large external table whose content is usually never read
 *       entirely but queried for individual values when necessary. See {@link LookupTableSource}
 *       for more information.
 * </ul>
 *
 * <p>Note: Both interfaces can be implemented at the same time. The planner decides about their
 * usage depending on the specified query.
 *
 * <p>Instances of the above-mentioned interfaces can be seen as factories that eventually produce
 * concrete runtime implementation for reading the actual data.
 *
 * <p>Depending on the optionally declared abilities such as {@link SupportsProjectionPushDown} or
 * {@link SupportsFilterPushDown}, the planner might apply changes to an instance and thus mutates
 * the produced runtime implementation.
 */
//Flink 中用于描述从外部存储系统读取数据的能力的接口。它代表了一个抽象的数据源，是 Flink 动态表概念的核心组成部分
//有两种主要类型
//ScanTableSource：用于全量扫描或连续读取变更日志（changelog）的数据源，例如读取 Kafka 中的所有消息或 HDFS 中的文件
//LookupTableSource：用于在必要时查询外部系统中的单个值，例如在 Join 操作中根据主键去查询一个数据库中的记录
@PublicEvolving
public interface DynamicTableSource {

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    //创建当前 DynamicTableSource 实例的一个深拷贝
    DynamicTableSource copy();

    /** Returns a string that summarizes this source for printing to a console or log. */
    //返回一个字符串，用于概括性地描述该数据源
    String asSummaryString();

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /**
     * Base context for creating runtime implementation via a {@link
     * ScanTableSource.ScanRuntimeProvider} and {@link LookupTableSource.LookupRuntimeProvider}.
     *
     * <p>It offers utilities by the planner for creating runtime implementation with minimal
     * dependencies to internal data structures.
     *
     * <p>Methods should be called in {@link
     * ScanTableSource#getScanRuntimeProvider(ScanTableSource.ScanContext)} and {@link
     * LookupTableSource#getLookupRuntimeProvider(LookupTableSource.LookupContext)}. The returned
     * instances are {@link Serializable} and can be directly passed into the runtime implementation
     * class.
     */
    //创建运行时实现的上下文
    @PublicEvolving
    interface Context {

        /**
         * Creates type information describing the internal data structures of the given {@link
         * DataType}.
         *
         * @see ResolvedSchema#toPhysicalRowDataType()
         */
        //根据给定的 DataType 或 LogicalType，创建 Flink 内部数据结构所需的**TypeInformation**
        <T> TypeInformation<T> createTypeInformation(DataType producedDataType);

        /**
         * Creates type information describing the internal data structures of the given {@link
         * LogicalType}.
         */
        //根据给定的 DataType 或 LogicalType，创建 Flink 内部数据结构所需的**TypeInformation**
        <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType);

        /**
         * Creates a converter for mapping between objects specified by the given {@link DataType}
         * and Flink's internal data structures that can be passed into a runtime implementation.
         *
         * <p>For example, a {@link Row} and its fields can be converted into {@link RowData}, or a
         * (possibly nested) POJO can be converted into the internal representation for structured
         * types.
         *
         * @see LogicalType#supportsInputConversion(Class)
         * @see ResolvedSchema#toPhysicalRowDataType()
         */
        //创建一个数据结构转换器（DataStructureConverter），用于在运行时将外部对象（如 org.apache.flink.types.Row）转换为 Flink 的内部数据结构（如 org.apache.flink.table.data.RowData）
        DataStructureConverter createDataStructureConverter(DataType producedDataType);
    }

    /**
     * Converter for mapping between objects and Flink's internal data structures during runtime.
     *
     * <p>On request, the planner will provide a specialized (possibly code generated) converter
     * that can be passed into a runtime implementation.
     *
     * <p>For example, a {@link Row} and its fields can be converted into {@link RowData}, or a
     * (possibly nested) POJO can be converted into the internal representation for structured
     * types.
     *
     * @see LogicalType#supportsInputConversion(Class)
     */
    // Flink Table API 中一个关键接口，
    // 它的核心作用是在运行时将外部数据结构（如 Java 对象、POJO、Row 等）转换为 Flink 内部的高效数据结构（如 RowData、BinaryStringData 等）
    // Flink 的 Table 模块为了追求极致的性能，使用了自己的一套紧凑、二进制格式的内部数据结构。然而，外部数据源和用户自定义函数（UDF）通常使用标准的 Java 对象。
    // DataStructureConverter 就是负责处理这两种数据格式之间转换的桥梁
    @PublicEvolving
    interface DataStructureConverter extends RuntimeConverter {

        /** Converts the given object into an internal data structure. */
        //将给定的外部数据结构转换为 Flink 内部的数据结构
        //externalStructure: 这是一个输入参数，代表待转换的外部数据对象。这个对象可以是标准的 Java 对象，例如一个 Row、一个 POJO、一个 List 或其他任何外部数据格式。参数可以为 null

        @Nullable
        Object toInternal(@Nullable Object externalStructure);
    }
}
