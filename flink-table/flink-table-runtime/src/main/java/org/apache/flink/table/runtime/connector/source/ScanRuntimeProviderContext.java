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

package org.apache.flink.table.runtime.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;
// 主要用作 ScanTableSource.ScanContext 接口的实现。
// 它的核心作用是为批处理或流式扫描（Scan）的运行时提供必要的上下文信息和工具，
// 特别是用于创建类型信息和数据结构转换器
/** Implementation of {@link ScanTableSource.Context}. */
@Internal
public final class ScanRuntimeProviderContext implements ScanTableSource.ScanContext {

    public static final ScanRuntimeProviderContext INSTANCE = new ScanRuntimeProviderContext();
    //根据给定的 DataType 创建 Flink 遗留的 TypeInformation 对象
    @Override
    public TypeInformation<?> createTypeInformation(DataType producedDataType) {
        validateInputDataType(producedDataType);
        return InternalTypeInfo.of(producedDataType.getLogicalType());
    }
    //根据给定的 LogicalType 创建 TypeInformation 对象
    @Override
    public TypeInformation<?> createTypeInformation(LogicalType producedLogicalType) {
        return InternalTypeInfo.of(producedLogicalType);
    }
    //根据给定的 DataType 创建一个数据结构转换器
    @Override
    public DataStructureConverter createDataStructureConverter(DataType producedDataType) {
        validateInputDataType(producedDataType);
        return new DataStructureConverterWrapper(
                DataStructureConverters.getConverter(producedDataType));
    }
}
