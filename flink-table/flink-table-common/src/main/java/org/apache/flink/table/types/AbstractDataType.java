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
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Highest abstraction that describes the data type of a value in the table ecosystem. This class
 * describes two kinds of data types:
 *
 * <p>Fully resolved data types that can be used directly to declare input and/or output types of
 * operations. This kind is represented in subclasses of {@link DataType}.
 *
 * <p>Partially resolved data types that can be resolved to {@link DataType} but require a lookup in
 * a catalog or configuration first. This kind is represented in subclasses of {@link
 * UnresolvedDataType}.
 *
 * <p>Note: Use {@link DataTypes} for producing instances of this class.
 *
 * @param <T> kind of data type returned after mutation
 */
// Flink 表生态系统中描述数据类型的最高抽象接口。
// 它不直接代表一个具体的数据类型，而是定义了所有数据类型（无论是已解析的还是未解析的）都必须具备的基本行为，
// 例如设置可空性（nullability）和指定物理表示（bridged class）
    //区分了两种数据类型
    //已解析的数据类型（DataType）：可以直接用于声明操作的输入/输出类型，包含了完整的信息
    //未解析的数据类型（UnresolvedDataType）：在解析前需要查阅 Catalog 或配置，如 TIMESTAMP(3) 在没有指定时区时就是一个未解析的类型
@PublicEvolving
public interface AbstractDataType<T extends AbstractDataType<T>> {

    /**
     * Adds a hint that null values are not expected in the data for this type.
     *
     * @return a new, reconfigured data type instance
     */
    //表明该数据类型的数据不期望为空
    T notNull();

    /**
     * Adds a hint that null values are expected in the data for this type (default behavior).
     *
     * <p>This method exists for explicit declaration of the default behavior or for invalidation of
     * a previous call to {@link #notNull()}.
     *
     * @return a new, reconfigured data type instance
     */
    //表明该数据类型的数据可以为空
    T nullable();

    /**
     * Adds a hint that data should be represented using the given class when entering or leaving
     * the table ecosystem.
     *
     * <p>A supported conversion class depends on the logical type and its nullability property.
     *
     * <p>Please see the implementation of {@link LogicalType#supportsInputConversion(Class)},
     * {@link LogicalType#supportsOutputConversion(Class)}, or the documentation for more
     * information about supported conversions.
     *
     * @return a new, reconfigured data type instance
     */
    //指定当数据进入或离开表生态系统时，应使用给定的 Java 类来表示
    T bridgedTo(Class<?> newConversionClass);
}
