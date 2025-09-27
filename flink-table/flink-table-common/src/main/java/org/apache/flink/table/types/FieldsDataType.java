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

package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;

/**
 * A data type that contains field data types (i.e. row and structured type).
 *
 * @see DataTypes for a list of supported data types
 */
// Flink 表生态系统中用于表示包含字段的复合数据类型（例如 ROW 和结构化类型）的具体实现类。
// 它的核心作用是为像 ROW<name STRING, age INT> 这样的数据类型提供一个具体的、可实例化的 DataType 对象
@PublicEvolving
public final class FieldsDataType extends DataType {
    //存储该行或结构化类型的字段的数据类型列表
    private final List<DataType> fieldDataTypes;

    public FieldsDataType(
            LogicalType logicalType,
            @Nullable Class<?> conversionClass,
            List<DataType> fieldDataTypes) {
        super(logicalType, conversionClass);
        Preconditions.checkNotNull(fieldDataTypes, "Field data types must not be null.");
        this.fieldDataTypes =
                fieldDataTypes.stream().map(this::updateInnerDataType).collect(Collectors.toList());
    }

    public FieldsDataType(LogicalType logicalType, List<DataType> fieldDataTypes) {
        this(logicalType, null, fieldDataTypes);
    }

    @Override
    public DataType notNull() {
        return new FieldsDataType(logicalType.copy(false), conversionClass, fieldDataTypes);
    }

    @Override
    public DataType nullable() {
        return new FieldsDataType(logicalType.copy(true), conversionClass, fieldDataTypes);
    }

    @Override
    public DataType bridgedTo(Class<?> newConversionClass) {
        return new FieldsDataType(
                logicalType,
                Preconditions.checkNotNull(
                        newConversionClass, "New conversion class must not be null."),
                fieldDataTypes);
    }

    @Override
    public List<DataType> getChildren() {
        return fieldDataTypes;
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FieldsDataType that = (FieldsDataType) o;
        return fieldDataTypes.equals(that.fieldDataTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldDataTypes);
    }

    // --------------------------------------------------------------------------------------------

    private DataType updateInnerDataType(DataType innerDataType) {
        if (conversionClass == RowData.class) {
            return innerDataType.bridgedTo(
                    toInternalConversionClass(innerDataType.getLogicalType()));
        }
        return innerDataType;
    }
}
