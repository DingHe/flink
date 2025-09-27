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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.TimestampData;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of a timestamp WITHOUT time zone consisting of {@code year-month-day
 * hour:minute:second[.fractional]} with up to nanosecond precision and values ranging from {@code
 * 0000-01-01 00:00:00.000000000} to {@code 9999-12-31 23:59:59.999999999}. Compared to the SQL
 * standard, leap seconds (23:59:60 and 23:59:61) are not supported as the semantics are closer to
 * {@link java.time.LocalDateTime}.
 *  Flink 中表示无时区（WITHOUT TIME ZONE）时间戳的逻辑类型，它描述了一个精确到纳秒的时间戳类型，值的范围是从 0000-01-01 00:00:00.000000000 到 9999-12-31 23:59:59.999999999，不支持闰秒
 * <p>The serialized string representation is {@code TIMESTAMP(p)} where {@code p} is the number of
 * digits of fractional seconds (=precision). {@code p} must have a value between 0 and 9 (both
 * inclusive). If no precision is specified, {@code p} is equal to 6. {@code TIMESTAMP(p) WITHOUT
 * TIME ZONE} is a synonym for this type.
 *
 * <p>A conversion from and to {@code long} is not supported as this would imply a time zone.
 * However, this type is time zone free. For more {@link java.time.Instant}-like semantics use
 * {@link LocalZonedTimestampType}.
 *
 * @see ZonedTimestampType
 * @see LocalZonedTimestampType
 */
@PublicEvolving
public final class TimestampType extends LogicalType {
    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = 0; //时间戳精度的最小值（0）

    public static final int MAX_PRECISION = 9; //时间戳精度的最大值（9）

    public static final int DEFAULT_PRECISION = 6; //默认精度（6）

    private static final String FORMAT = "TIMESTAMP(%d)"; //时间戳的格式化字符串，使用精度 p 来表示时间戳精度

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(
                    java.sql.Timestamp.class.getName(),
                    java.time.LocalDateTime.class.getName(),
                    TimestampData.class.getName());  //支持的输入输出转换类集合

    private static final Class<?> DEFAULT_CONVERSION = java.time.LocalDateTime.class;

    private final TimestampKind kind;

    private final int precision;  //时间戳精度

    /**
     * Internal constructor that allows attaching additional metadata about time attribute
     * properties. The additional metadata does not affect equality or serializability.
     * 内部构造函数，允许附加关于时间属性的额外元数据。kind 表示时间戳的种类，precision 表示精度
     * <p>Use {@link #getKind()} for comparing this metadata.
     */
    @Internal
    public TimestampType(boolean isNullable, TimestampKind kind, int precision) {
        super(isNullable, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Timestamp precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        if (kind == TimestampKind.PROCTIME) {
            throw new ValidationException("TimestampType can not be used as PROCTIME type.");
        }
        this.kind = kind;
        this.precision = precision;
    }
    //构造一个可为空的时间戳类型，指定精度
    public TimestampType(boolean isNullable, int precision) {
        this(isNullable, TimestampKind.REGULAR, precision);
    }

    public TimestampType(int precision) {
        this(true, precision);
    }

    public TimestampType() {
        this(DEFAULT_PRECISION);
    }

    @Internal
    public TimestampKind getKind() {
        return kind;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TimestampType(isNullable, kind, precision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, precision);
    }

    @Override
    public String asSummaryString() {
        if (kind != TimestampKind.REGULAR) {
            return String.format("%s *%s*", asSerializableString(), kind);
        }
        return asSerializableString();
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
        return Collections.emptyList();
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
        TimestampType that = (TimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
