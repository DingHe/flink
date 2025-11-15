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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.table.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Logical type of a sequence of fields. A field consists of a field name, field type, and an
 * optional description. The most specific type of a row of a table is a row type. In this case,
 * each column of the row corresponds to the field of the row type that has the same ordinal
 * position as the column. Compared to the SQL standard, an optional field description simplifies
 * the handling with complex structures.
 *
 * <p>The serialized string representation is {@code ROW<n0 t0 'd0', n1 t1 'd1', ...>} where {@code
 * n} is the unique name of a field, {@code t} is the logical type of a field, {@code d} is the
 * description of a field. {@code ROW(...)} is a synonym for being closer to the SQL standard.
 */
// RowType 是 Flink Table/SQL API 中用于描述结构化数据类型，即行（Row） 或 记录（Record） 的逻辑类型。
// 复合类型定义： 将一组具有名称、类型和可选描述的字段（RowField）按顺序组合起来，形成一个复合数据类型。
// 表结构映射： 表（Table）的行类型在 Flink 内部就是用 RowType 来表示的，其中 RowType 中的每个字段对应表中的一列。
// 支持嵌套结构： 作为 Flink 中基本的复合类型，它支持字段类型是其他任意 LogicalType（包括另一个 RowType），从而能够表达复杂的嵌套数据结构（如 JSON 或 STRUCT）
// RowType 是 Flink 中用于描述**“一张表”**或 “一行记录” 结构和模式（Schema）的权威对象。
@PublicEvolving
public final class RowType extends LogicalType {
    private static final long serialVersionUID = 1L;
    // RowType 的序列化字符串格式 (ROW<%s>)。
    public static final String FORMAT = "ROW<%s>";
    // 定义 RowType 支持的 JVM 转换类型（org.apache.flink.types.Row 和 org.apache.flink.table.data.RowData）
    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(Row.class.getName(), RowData.class.getName());
    // RowType 的默认 JVM 转换类，即 org.apache.flink.types.Row。
    private static final Class<?> DEFAULT_CONVERSION = Row.class;

    /** Describes a field of a {@link RowType}. */
    // RowField 封装了行中单个列的所有元数据。
    @PublicEvolving
    public static final class RowField implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

        public static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";
        // 字段的名称。
        // 在行中必须唯一，且不能为空白。
        private final String name;
        // 字段的逻辑类型。
        // 描述该字段值的具体数据类型（如 INT、VARCHAR）
        private final LogicalType type;
        // 字段的可选描述/注释。
        // 提供了额外的人类可读信息。
        private final @Nullable String description;

        public RowField(String name, LogicalType type, @Nullable String description) {
            this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
            this.type = Preconditions.checkNotNull(type, "Field type must not be null.");
            this.description = description;
        }

        public RowField(String name, LogicalType type) {
            this(name, type, null);
        }

        public String getName() {
            return name;
        }

        public LogicalType getType() {
            return type;
        }

        public Optional<String> getDescription() {
            return Optional.ofNullable(description);
        }

        public RowField copy() {
            return new RowField(name, type.copy(), description);
        }

        public String asSummaryString() {
            return formatString(type.asSummaryString(), true);
        }

        public String asSerializableString() {
            return formatString(type.asSerializableString(), false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RowField rowField = (RowField) o;
            return name.equals(rowField.name)
                    && type.equals(rowField.type)
                    && Objects.equals(description, rowField.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, description);
        }

        private String formatString(String typeString, boolean excludeDescription) {
            if (description == null) {
                return String.format(
                        FIELD_FORMAT_NO_DESCRIPTION, escapeIdentifier(name), typeString);
            } else if (excludeDescription) {
                return String.format(
                        FIELD_FORMAT_WITH_DESCRIPTION, escapeIdentifier(name), typeString, "...");
            } else {
                return String.format(
                        FIELD_FORMAT_WITH_DESCRIPTION,
                        escapeIdentifier(name),
                        typeString,
                        escapeSingleQuotes(description));
            }
        }
    }
    // 核心属性
    // 包含所有字段描述的列表。
    // 该列表是不可修改的 (unmodifiableList)，保持了 RowType 的不可变性。
    private final List<RowField> fields;

    public RowType(boolean isNullable, List<RowField> fields) {
        super(isNullable, LogicalTypeRoot.ROW);
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(fields, "Fields must not be null.")));

        validateFields(fields);
    }

    public RowType(List<RowField> fields) {
        this(true, fields);
    }

    public List<RowField> getFields() {
        return fields;
    }

    public List<String> getFieldNames() {
        return fields.stream().map(RowField::getName).collect(Collectors.toList());
    }

    public LogicalType getTypeAt(int i) {
        return fields.get(i).getType();
    }

    public int getFieldCount() {
        return fields.size();
    }

    public int getFieldIndex(String fieldName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new RowType(
                isNullable, fields.stream().map(RowField::copy).collect(Collectors.toList()));
    }

    @Override
    public String asSummaryString() {
        return withNullability(
                FORMAT,
                fields.stream().map(RowField::asSummaryString).collect(Collectors.joining(", ")));
    }

    @Override
    public String asSerializableString() {
        return withNullability(
                FORMAT,
                fields.stream()
                        .map(RowField::asSerializableString)
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.unmodifiableList(
                fields.stream().map(RowField::getType).collect(Collectors.toList()));
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
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
        RowType rowType = (RowType) o;
        return fields.equals(rowType.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    // --------------------------------------------------------------------------------------------

    private static void validateFields(List<RowField> fields) {
        final List<String> fieldNames =
                fields.stream().map(f -> f.name).collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
            throw new ValidationException(
                    "Field names must contain at least one non-whitespace character.");
        }
        final Set<String> duplicates =
                fieldNames.stream()
                        .filter(n -> Collections.frequency(fieldNames, n) > 1)
                        .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new ValidationException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    public static RowType of(LogicalType... types) {
        return of(true, types);
    }

    public static RowType of(boolean isNullable, LogicalType... types) {
        final List<RowField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new RowField("f" + i, types[i]));
        }
        return new RowType(isNullable, fields);
    }

    public static RowType of(LogicalType[] types, String[] names) {
        return of(true, types, names);
    }

    public static RowType of(boolean nullable, LogicalType[] types, String[] names) {
        List<RowField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new RowField(names[i], types[i]));
        }
        return new RowType(nullable, fields);
    }
}
