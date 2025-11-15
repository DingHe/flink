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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract implementation for streaming unbounded Join operator which defines some member fields
 * can be shared between different implementations.
 */
// AbstractStreamingJoinOperator 是 Flink Table/SQL 在流模式下实现无界连接 (Unbounded Join) 算子的抽象基类。
// 统一流式连接骨架： 为所有基于状态的流式 Join 算子（例如 StreamHashJoinOperator, StreamIntervalJoinOperator 等）提供一个共同的基础结构和共享成员字段。
// 管理连接元数据： 存储和初始化连接操作的核心配置，包括左右输入的数据类型、代码生成的连接条件（JoinCondition）以及状态保留时间。
// 处理 Null Key 过滤： 封装了连接键为 NULL 时的过滤逻辑（通过 JoinConditionWithNullFilters）
// 支持外连接计数： 提供了 OuterRecord 和 AssociatedRecords 等内部结构，专门用于跟踪在外连接（Outer Join）中，一侧记录与另一侧记录的匹配次数 (numOfAssociations)，这是正确处理外连接语义（即输出未匹配行）的关键。

public abstract class AbstractStreamingJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = -376944622236540545L;
    // 左输入状态名称。
    // 用于命名存储左输入数据的 Flink 状态（State）变量。
    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    // 右输入状态名称。
    // 用于命名存储右输入数据的 Flink 状态（State）变量。
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";
    // 代码生成的连接条件。
    // 包含通过 Flink 代码生成器创建的连接条件（通常是等值连接外的非等值条件）的运行时代码。
    private final GeneratedJoinCondition generatedJoinCondition;
    // 左输入行类型信息。
    // 描述左输入流 RowData 的内部类型结构，用于状态序列化和反序列化。
    protected final InternalTypeInfo<RowData> leftType;
    // 右输入行类型信息。
    // 描述右输入流 RowData 的内部类型结构。
    protected final InternalTypeInfo<RowData> rightType;
    // 左输入侧规范。
    // 定义左输入侧数据在 Join 逻辑中的具体行为和状态存储方式。
    protected final JoinInputSideSpec leftInputSideSpec;
    // 右输入侧规范。
    // 定义右输入侧数据在 Join 逻辑中的具体行为和状态存储方式。
    protected final JoinInputSideSpec rightInputSideSpec;
    // 空键过滤标记。
    // 长度为 2 的布尔数组，指示是否应该过滤掉左右输入中连接键为 NULL 的记录。
    private final boolean[] filterNullKeys;
    // 左状态保留时间。
    // 定义左输入状态在 Flink 状态后端中应保留的时长。
    // 这对于处理流中的 TTL（Time-To-Live）和状态清理至关重要。
    protected final long leftStateRetentionTime;
    // 右状态保留时间。
    // 定义右输入状态应保留的时长。
    protected final long rightStateRetentionTime;
    // 运行时连接条件。
    // 包含实际执行连接条件判断的函数实例，并集成了 filterNullKeys 的逻辑。
    protected transient JoinConditionWithNullFilters joinCondition;
    // 时间戳收集器。
    // 用于将连接结果输出到下游算子，并带有时间戳信息。
    protected transient TimestampedCollector<RowData> collector;

    public AbstractStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.leftStateRetentionTime = leftStateRetentionTime;
        this.rightStateRetentionTime = rightStateRetentionTime;
        this.filterNullKeys = filterNullKeys;
    }

    @Override
    public void open() throws Exception {
        super.open();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(DefaultOpenContext.INSTANCE);

        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    /**
     * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper of
     * {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and {@link
     * #getOuterRecords()}. See the method Javadoc for more details.
     */
    // 内部类用于封装与当前输入行匹配的另一侧的所有记录，
    // 特别是用于外连接（Outer Join）中
    protected static final class AssociatedRecords {
        // 存储匹配记录的列表。
        private final List<OuterRecord> records;

        private AssociatedRecords(List<OuterRecord> records) {
            checkNotNull(records);
            this.records = records;
        }
        // 检查匹配记录列表是否为空。
        public boolean isEmpty() {
            return records.isEmpty();
        }
        // 返回匹配记录的数量。
        public int size() {
            return records.size();
        }

        /**
         * Gets the iterable of records. This is usually be called when the {@link
         * AssociatedRecords} is from inner side.
         */
        public Iterable<RowData> getRecords() {
            return new RecordsIterable(records);
        }

        /**
         * Gets the iterable of {@link OuterRecord} which composites record and numOfAssociations.
         * This is usually be called when the {@link AssociatedRecords} is from outer side.
         */
        public Iterable<OuterRecord> getOuterRecords() {
            return records;
        }

        /**
         * Creates an {@link AssociatedRecords} which represents the records associated to the input
         * row.
         */
        // 接收输入行、另一侧的状态视图 (otherSideStateView) 和连接条件 (condition)。
        // 它遍历另一侧状态中的所有记录，应用连接条件进行匹配，并封装成 AssociatedRecords 返回。
        public static AssociatedRecords of(
                RowData input, // 当前处理的输入记录
                boolean inputIsLeft, // 输入记录是否来自 Join 的左侧（如果为 true，则来自左侧；否则来自右侧）。这决定了 JoinCondition 应该如何应用。
                JoinRecordStateView otherSideStateView, // 另一侧输入的状态视图。它封装了另一侧输入流所有已存储的记录。
                JoinCondition condition) // join 的匹配条件（通常是 Join Key 相等加上额外的非等值条件）。
                throws Exception {
            List<OuterRecord> associations = new ArrayList<>();
            // 如果为 true，说明该 Join 是外连接（如 Left/Right/Full Join），并且需要跟踪状态中每条记录的关联计数。
            if (otherSideStateView instanceof OuterJoinRecordStateView) {
                OuterJoinRecordStateView outerStateView =
                        (OuterJoinRecordStateView) otherSideStateView;
                // 获取另一侧状态中存储的所有记录及其关联计数的集合。返回的 Tuple2 中 f0 是记录，f1 是关联计数。
                Iterable<Tuple2<RowData, Integer>> records =
                        outerStateView.getRecordsAndNumOfAssociations();
                for (Tuple2<RowData, Integer> record : records) {
                    // 执行 Join 条件判断
                    boolean matched =
                            inputIsLeft
                                    ? condition.apply(input, record.f0)
                                    : condition.apply(record.f0, input);
                    if (matched) {
                        associations.add(new OuterRecord(record.f0, record.f1));
                    }
                }
            } else {
                // 如果为 false，则为内连接（Inner Join），无需跟踪关联计数。
                Iterable<RowData> records = otherSideStateView.getRecords();
                for (RowData record : records) {
                    boolean matched =
                            inputIsLeft
                                    ? condition.apply(input, record)
                                    : condition.apply(record, input);
                    if (matched) {
                        // use -1 as the default number of associations
                        associations.add(new OuterRecord(record, -1));
                    }
                }
            }
            return new AssociatedRecords(associations);
        }
    }

    /** A lazy Iterable which transform {@code List<OuterReocord>} to {@code Iterable<RowData>}. */
    private static final class RecordsIterable implements IterableIterator<RowData> {
        private final List<OuterRecord> records;
        private int index = 0;

        private RecordsIterable(List<OuterRecord> records) {
            this.records = records;
        }

        @Override
        public Iterator<RowData> iterator() {
            index = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return index < records.size();
        }

        @Override
        public RowData next() {
            RowData row = records.get(index).record;
            index++;
            return row;
        }
    }

    /**
     * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The {@code
     * numOfAssociations} represents the number of associated records in the other side. It is used
     * when the record is from outer side (e.g. left side in LEFT OUTER JOIN). When the {@code
     * numOfAssociations} is ZERO, we need to send a null padding row. This is useful to avoid
     * recompute the associated numbers every time.
     *
     * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the {@code
     * numOfAssociations} will always be {@code -1}.
     */
    // 用于在外连接中携带额外信息，即匹配次数。
    protected static final class OuterRecord {
        // 实际的行数据。
        public final RowData record;
        // 匹配次数。
        // 表示该记录与另一侧的记录已经匹配了多少次。
        // 在 Left Outer Join 中，如果左侧记录的 numOfAssociations 为 0，则需要输出一次填充 Null 的行。如果记录来自内部连接侧，则通常为 -1。
        public final int numOfAssociations;

        private OuterRecord(RowData record, int numOfAssociations) {
            this.record = record;
            this.numOfAssociations = numOfAssociations;
        }
    }
}
