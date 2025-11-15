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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical type that describes the data type of a value. It does not imply a concrete physical
 * representation for transmission or storage but defines the boundaries between JVM-based languages
 * and the table ecosystem.
 *
 * <p>The definition of a logical type is similar to the SQL standard's "data type" terminology but
 * also contains information about the nullability of a value for efficient handling of scalar
 * expressions.
 *
 * <p>Subclasses of this class define characteristics of built-in or user-defined types. Every
 * logical type must support nullability.
 *
 * <p>Instances of this class describe the fully parameterized, immutable type with additional
 * information such as numeric precision or expected length.
 *
 * <p>Contracts how logical types relate to other types are defined by {@link LogicalTypeCasts} and
 * {@link LogicalTypeMerging}.
 * <p>NOTE: A logical type is just a description of a type, a planner or runtime might not support
 * every type in every logical precision yet!
 */
// LogicalType 是 Flink Table/SQL API 中用于描述数据类型的抽象基类，它处于 Flink 的类型系统、SQL 标准和 JVM 语言之间。
// LogicalType 的核心作用是在 Flink 的表生态系统（Table Ecosystem）中提供一个与物理存储和传输无关的、标准化的类型描述。它定义了数据值在关系代数和 SQL 语义层面的特征。
// 语义描述： 专注于数据的逻辑特征（例如，这是一个 INTEGER，它可空，它有特定的精度），而不是数据在内存中如何编码（这是 PhysicalType 的职责）
// 互操作性边界： 定义了 JVM 语言（如 Java/Scala）中的原生类型和 Flink 表系统内部类型之间的转换规则（通过 supportsInputConversion 和 supportsOutputConversion）。
// 类型元数据： 封装了类型的可空性 (isNullable) 和根类型 (typeRoot)，这些是表达式和规划器优化的基础。
// 代表了 Flink 中数据的**“是什么”（语义），而“如何存储”**（物理）则由其他组件处理。
@PublicEvolving
public abstract class LogicalType implements Serializable {
    private static final long serialVersionUID = 1L;
    // 可空性标记。
    // 表示该类型的值是否允许为 null。这是逻辑类型定义的重要组成部分，有助于编译器优化。
    private final boolean isNullable;
    // 逻辑类型根。
    // 表示该类型的基本分类（例如 INTEGER, VARCHAR, ROW）。
    // 它是一个不含参数的类型骨架，用于快速识别类型种类。
    private final LogicalTypeRoot typeRoot;

    public LogicalType(boolean isNullable, LogicalTypeRoot typeRoot) {
        this.isNullable = isNullable;
        this.typeRoot = Preconditions.checkNotNull(typeRoot);
    }
    //返回是否可空
    /** Returns whether a value of this type can be {@code null}. */
    public boolean isNullable() {
        return isNullable;
    }
    //返回该类型的根类型（即基本的类型分类）
    /**
     * Returns the root of this type. It is an essential description without additional parameters.
     */
    public LogicalTypeRoot getTypeRoot() {
        return typeRoot;
    }

    /**
     * Returns whether the root of the type equals to the {@code typeRoot} or not.
     * @param typeRoot The root type to check against for equality
     */
    // 判断当前类型是否与指定的根类型相同
    public boolean is(LogicalTypeRoot typeRoot) {
        return this.typeRoot == typeRoot;
    }

    /**
     * Returns whether the root of the type equals to at least on of the {@code typeRoots} or not.
     * @param typeRoots The root types to check against for equality
     */
    // 判断当前类型是否与给定的一组根类型中的任何一个匹配
    public boolean isAnyOf(LogicalTypeRoot... typeRoots) {
        return Arrays.stream(typeRoots).anyMatch(tr -> this.typeRoot == tr);
    }

    /**
     * Returns whether the root of the type is part of at least one family of the {@code typeFamily}
     * or not.
     * @param typeFamilies The families to check against for equality
     */
    //  判断当前类型是否属于给定的类型族中的任何一个
    public boolean isAnyOf(LogicalTypeFamily... typeFamilies) {
        return Arrays.stream(typeFamilies).anyMatch(tf -> this.typeRoot.getFamilies().contains(tf));
    }

    /**
     * Returns whether the family type of the type equals to the {@code family} or not.
     * @param family The family type to check against for equality
     */
    // 判断当前类型是否属于给定的类型族中的任何一个
    public boolean is(LogicalTypeFamily family) {
        return typeRoot.getFamilies().contains(family);
    }

    /**
     * Returns a deep copy of this type with possibly different nullability.
     *
     * @param isNullable the intended nullability of the copied type
     * @return a deep copy
     */
    public abstract LogicalType copy(boolean isNullable);

    /**
     * Returns a deep copy of this type. It requires an implementation of {@link #copy(boolean)}.
     *
     * @return a deep copy
     */
    public final LogicalType copy() {
        return copy(isNullable);
    }

    /**
     * Returns a string that fully serializes this instance. The serialized string can be used for
     * transmitting or persisting a type.
     *
     * <p>See {@link LogicalTypeParser} for the reverse operation.
     * @return detailed string for transmission or persistence
     */
    // 返回一个字符串，完全序列化当前类型，适合用于传输或持久化
    public abstract String asSerializableString();

    /**
     * Returns a string that summarizes this type for printing to a console. An implementation might
     * shorten long names or skips very specific properties.
     *
     * <p>Use {@link #asSerializableString()} for a type string that fully serializes this instance.
     *
     * @return summary string of this type for debugging purposes
     */
    public String asSummaryString() {
        return asSerializableString();
    }

    /**
     * Returns whether an instance of the given class can be represented as a value of this logical
     * type when entering the table ecosystem. This method helps for the interoperability between
     * JVM-based languages and the relational type system.
     *
     * <p>A supported conversion directly maps an input class to a logical type without loss of
     * precision or type widening.
     *
     * <p>For example, {@code java.lang.Long} or {@code long} can be used as input for {@code
     * BIGINT} independent of the set nullability.
     * @param clazz input class to be converted into this logical type
     * @return flag that indicates if instances of this class can be used as input into the table
     *     ecosystem
     * @see #getDefaultConversion()
     */
    // 抽象方法
    // 检查 JVM 类到逻辑类型的输入转换。
    // 判断给定的 JVM 类实例是否可以无损地作为输入（进入 Flink 表生态系统）转换为该逻辑类型。
    public abstract boolean supportsInputConversion(Class<?> clazz);

    /**
     * Returns whether a value of this logical type can be represented as an instance of the given
     * class when leaving the table ecosystem. This method helps for the interoperability between
     * JVM-based languages and the relational type system.
     *
     * <p>A supported conversion directly maps a logical type to an output class without loss of
     * precision or type widening.
     *
     * <p>For example, {@code java.lang.Long} or {@code long} can be used as output for {@code
     * BIGINT} if the type is not nullable. If the type is nullable, only {@code java.lang.Long} can
     * represent this.
     * @param clazz output class to be converted from this logical type
     * @return flag that indicates if instances of this class can be used as output from the table
     *     ecosystem
     * @see #getDefaultConversion()
     */
    // 抽象方法
    // 检查逻辑类型到 JVM 类的输出转换。
    // 判断该逻辑类型的值是否可以无损地作为输出（离开 Flink 表生态系统）转换为给定的 JVM 类实例。
    public abstract boolean supportsOutputConversion(Class<?> clazz);

    /**
     * Returns the default conversion class. A value of this logical type is expected to be an
     * instance of the given class when entering or is represented as an instance of the given class
     * when leaving the table ecosystem if no other conversion has been specified.
     * <p>For example, {@code java.lang.Long} is the default input and output for {@code BIGINT}.
     *
     * @return default class to represent values of this logical type
     * @see #supportsInputConversion(Class)
     * @see #supportsOutputConversion(Class)
     */
    // 抽象方法
    // 返回默认的 JVM 转换类。
    // 例如，BIGINT 的默认转换类是 java.lang.Long。
    public abstract Class<?> getDefaultConversion();
    // 抽象方法
    // 返回当前类型的子类型列表。
    // 用于复合类型（如 ROW 或 ARRAY），方便遍历类型的内部结构。
    public abstract List<LogicalType> getChildren();
    //接受一个访问者（Visitor 模式），用于对类型进行处理
    public abstract <R> R accept(LogicalTypeVisitor<R> visitor);

    @Override
    public String toString() {
        return asSummaryString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalType that = (LogicalType) o;
        return isNullable == that.isNullable && typeRoot == that.typeRoot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isNullable, typeRoot);
    }

    // --------------------------------------------------------------------------------------------

    protected String withNullability(String format, Object... params) {
        if (!isNullable) {
            return String.format(format + " NOT NULL", params);
        }
        return String.format(format, params);
    }

    protected static Set<String> conversionSet(String... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }
}
