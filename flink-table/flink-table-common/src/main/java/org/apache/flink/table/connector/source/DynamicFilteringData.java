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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Data for dynamic filtering. */
// DynamicFilteringData 类是 Flink Table/SQL 模块中用于支持**动态过滤（Dynamic Filtering）**优化的核心数据结构。
// 动态过滤数据载体： 它封装了从一个流（通常是小表）中收集到的、用于**过滤另一个流（通常是大表或分区）**的关键数据集合。
// 实现 Join 优化： 在 Flink Table/SQL 中，动态过滤常用于优化 Join 操作。它将 Join 小表的结果（例如，作为分区键的值）发送给大表的 Source，大表的 Source 在读取数据时利用这些值来跳过不必要的分区或文件块。
// 延迟反序列化： 为了在网络传输和检查点中保持高效，它以字节数组 (byte[]) 的形式存储过滤数据。只有当 Source 真正需要使用这些数据进行过滤时，才会执行反序列化
// 它携带了需要在运行时从一个数据流传递给另一个数据流的过滤条件集合，以实现运行时的数据裁剪优化，提升查询性能。
@PublicEvolving
public class DynamicFilteringData implements Serializable {
    // 行数据类型信息。
    // 描述了内部存储的 RowData 的 Flink 类型信息，用于反序列化。
    private final TypeInformation<RowData> typeInfo;
    // 行数据的逻辑类型。
    // 描述了内部存储的 RowData 的逻辑结构和字段类型。
    private final RowType rowType;

    /**
     * Serialized rows for filtering. The types of the row values must be Flink internal data type,
     * i.e. type returned by the FieldGetter. The list should be sorted and distinct.
     */
    // 序列化后的过滤数据。
    // 存储的是用于过滤的 RowData 对象的字节数组列表。这些数据在发送到 Source 端之前已完成序列化。
    private final List<byte[]> serializedData;

    /** Whether the data actually does filter. If false, everything is considered contained. */
    // 是否执行过滤的标志。
    // 如果为 false，则表示无需执行过滤，所有数据都被认为匹配（通常在 Join 小表为空时设置）。
    private final boolean isFiltering;
    // 准备状态标志。
    // 标志数据是否已从字节数组反序列化并准备好用于查询。使用 volatile 确保多线程可见性。
    private transient volatile boolean prepared = false;
    // 反序列化后的过滤数据（哈希映射）。
    // 存储反序列化后的 RowData，使用哈希值作为键，用于加速查找。
    private transient Map<Integer, List<RowData>> dataMap;
    // 字段获取器数组。
    // 用于从 RowData 中快速、高效地提取字段值，用于哈希和比较操作
    private transient RowData.FieldGetter[] fieldGetters;

    public DynamicFilteringData(
            TypeInformation<RowData> typeInfo,
            RowType rowType,
            List<byte[]> serializedData,
            boolean isFiltering) {
        this.typeInfo = checkNotNull(typeInfo);
        this.rowType = checkNotNull(rowType);
        this.serializedData = checkNotNull(serializedData);
        this.isFiltering = isFiltering;
    }

    public boolean isFiltering() {
        return isFiltering;
    }

    public RowType getRowType() {
        return rowType;
    }

    /**
     * Returns true if the dynamic filtering data contains the specific row.
     *
     * @param row the row to be tested. Types of the row values must be Flink internal data type,
     *     i.e. type returned by the FieldGetter.
     * @return true if the dynamic filtering data contains the specific row
     */
    // 检查是否包含特定行。
    // 首先检查 isFiltering。如果为 true，则调用 prepare() 进行反序列化，
    // 然后使用哈希查找和逐字段比较 (matchRow) 来确定给定的 RowData 是否存在于过滤数据集合中。
    public boolean contains(RowData row) {
        if (!isFiltering) {
            return true;
        } else if (row.getArity() != rowType.getFieldCount()) {
            throw new TableException("The arity of RowData is different");
        } else {
            prepare();
            List<RowData> mayMatchRowData = dataMap.get(hash(row));
            if (mayMatchRowData == null) {
                return false;
            }
            for (RowData mayMatch : mayMatchRowData) {
                if (matchRow(row, mayMatch)) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean matchRow(RowData row, RowData mayMatch) {
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            if (!Objects.equals(
                    fieldGetters[i].getFieldOrNull(row),
                    fieldGetters[i].getFieldOrNull(mayMatch))) {
                return false;
            }
        }
        return true;
    }

    private void prepare() {
        if (!prepared) {
            synchronized (this) {
                if (!prepared) {
                    doPrepare();
                    prepared = true;
                }
            }
        }
    }

    private void doPrepare() {
        this.dataMap = new HashMap<>();
        if (isFiltering) {
            this.fieldGetters =
                    IntStream.range(0, rowType.getFieldCount())
                            .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                            .toArray(RowData.FieldGetter[]::new);

            TypeSerializer<RowData> serializer =
                    typeInfo.createSerializer(new SerializerConfigImpl());
            for (byte[] bytes : serializedData) {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais)) {
                    RowData partition = serializer.deserialize(inView);
                    List<RowData> partitions =
                            dataMap.computeIfAbsent(hash(partition), k -> new ArrayList<>());
                    partitions.add(partition);
                } catch (Exception e) {
                    throw new TableException("Unable to deserialize the value.", e);
                }
            }
        }
    }

    private int hash(RowData row) {
        return Objects.hash(Arrays.stream(fieldGetters).map(g -> g.getFieldOrNull(row)).toArray());
    }

    public static boolean isEqual(DynamicFilteringData data, DynamicFilteringData another) {
        if (data == null) {
            return another == null;
        }
        if (another == null
                || (data.isFiltering != another.isFiltering)
                || !data.typeInfo.equals(another.typeInfo)
                || !data.rowType.equals(another.rowType)
                || data.serializedData.size() != another.serializedData.size()) {
            return false;
        }

        BytePrimitiveArrayComparator comparator = new BytePrimitiveArrayComparator(true);
        for (int i = 0; i < data.serializedData.size(); i++) {
            if (comparator.compare(data.serializedData.get(i), another.serializedData.get(i))
                    != 0) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public Collection<RowData> getData() {
        prepare();
        return dataMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "DynamicFilteringData{"
                + "isFiltering="
                + isFiltering
                + ", data size="
                + serializedData.size()
                + '}';
    }
}
