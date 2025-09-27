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

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * The {@link ChangelogCsvFormat} is a decoding format that uses a {@link DeserializationSchema}
 * during runtime. It supports emitting {@code INSERT} and {@code DELETE} changes.
 */
//专门用于处理包含变更日志（Changelog）信息的 CSV 格式数据。
// 它的核心作用是定义如何将外部的 CSV 数据，包括 INSERT 和 DELETE 操作，解码成 Flink 内部的 RowData 格式
public final class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    //定义了 CSV 文件中用于分隔列的字符
    private final String columnDelimiter;

    public ChangelogCsvFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }
    //创建并返回一个运行时解码器，用于将字节流反序列化为 RowData
    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        //创建一个数据结构转换器，用于将反序列化后的数据转换为 Flink 的内部数据格式
        final DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        //封装了 CSV 解析和变更日志处理的全部逻辑
        return new ChangelogCsvDeserializer(
                parsingTypes, converter, producedTypeInfo, columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // define that this format can produce INSERT and DELETE rows
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
