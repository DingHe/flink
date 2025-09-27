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
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;

/**
 * A data type that contains a key and value data type (e.g. {@code MAP}).
 *
 * @see DataTypes for a list of supported data types
 */
// Flink 表生态系统中用于表示键值对复合数据类型的具体实现类。
// 它的核心作用是为像 MAP（映射）这样的数据类型提供一个具体的、可实例化的 DataType 对象
//与 AtomicDataType（用于基本类型）不同，KeyValueDataType 包含了两个子数据类型：一个用于键 (keyDataType)，另一个用于值 (valueDataType)。
// 这种设计允许 Flink 准确地描述和处理像 MAP<STRING, INT> 这样的复杂数据结构
@PublicEvolving
public final class KeyValueDataType extends DataType {
    //存储该键值对类型的键的数据类型
    private final DataType keyDataType;
    //存储该键值对类型的值的数据类型
    private final DataType valueDataType;
    //创建一个具有指定逻辑类型、物理转换类、键类型和值类型的 KeyValueDataType 实例
    public KeyValueDataType(
            LogicalType logicalType,
            @Nullable Class<?> conversionClass,
            DataType keyDataType,
            DataType valueDataType) {
        super(logicalType, conversionClass);
        Preconditions.checkNotNull(keyDataType, "Key data type must not be null.");
        Preconditions.checkNotNull(valueDataType, "Value data type must not be null.");
        this.keyDataType = updateInnerDataType(keyDataType);
        this.valueDataType = updateInnerDataType(valueDataType);
    }

    public KeyValueDataType(LogicalType logicalType, DataType keyDataType, DataType valueDataType) {
        this(logicalType, null, keyDataType, valueDataType);
    }

    public DataType getKeyDataType() {
        return keyDataType;
    }

    public DataType getValueDataType() {
        return valueDataType;
    }

    @Override
    public DataType notNull() {
        return new KeyValueDataType(
                logicalType.copy(false), conversionClass, keyDataType, valueDataType);
    }

    @Override
    public DataType nullable() {
        return new KeyValueDataType(
                logicalType.copy(true), conversionClass, keyDataType, valueDataType);
    }

    @Override
    public DataType bridgedTo(Class<?> newConversionClass) {
        return new KeyValueDataType(
                logicalType,
                Preconditions.checkNotNull(
                        newConversionClass, "New conversion class must not be null."),
                keyDataType,
                valueDataType);
    }

    @Override
    public List<DataType> getChildren() {
        return Arrays.asList(keyDataType, valueDataType);
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
        KeyValueDataType that = (KeyValueDataType) o;
        return keyDataType.equals(that.keyDataType) && valueDataType.equals(that.valueDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyDataType, valueDataType);
    }

    // --------------------------------------------------------------------------------------------

    private DataType updateInnerDataType(DataType innerDataType) {
        if (conversionClass == MapData.class) {
            return innerDataType.bridgedTo(
                    toInternalConversionClass(innerDataType.getLogicalType()));
        }
        return innerDataType;
    }
}
