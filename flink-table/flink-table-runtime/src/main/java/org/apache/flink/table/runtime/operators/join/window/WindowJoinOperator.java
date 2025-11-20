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

package org.apache.flink.table.runtime.operators.join.window;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SlicingWindowTimerServiceImpl;
import org.apache.flink.table.runtime.operators.window.tvf.state.WindowListState;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;
import java.util.IdentityHashMap;
import java.util.List;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * Streaming window join operator.
 *
 * <p>Note: currently, {@link WindowJoinOperator} doesn't support early-fire and late-arrival. Thus
 * late elements (elements belong to emitted windows) will be simply dropped.
 *
 * <p>Note: currently, {@link WindowJoinOperator} doesn't support DELETE or UPDATE_BEFORE input row.
 */
// WindowJoinOperator 是 Flink Table API 中用于实现**基于事件时间（Event Time）的窗口连接（Window Join）**的抽象基类。
// 处理双流连接： 它是一个 TwoInputStreamOperator，能够接收来自两个输入流（左流和右流）的数据。
// 窗口划分和聚合： 它的主要职责是根据 Join Key 将来自两个输入流的数据按**窗口（Window）**进行分组和缓存。这里的窗口通常是 Table/SQL 中定义的 TVF (Table-Valued Function) 窗口（如 TUMBLE 或 HOP）
// 定时器驱动计算： 利用 事件时间定时器 来精确控制何时一个窗口应该结束并触发连接计算，确保连接结果的正确性。
// 状态管理： 使用 Flink Keyed State 存储属于每个 Join Key 和每个窗口的数据。
// 迟到数据处理： 检查输入元素是否属于已经触发（Fire）的窗口。如果迟到，则将其丢弃并记录相关指标。
// 注意： 目前该算子仅支持 INSERT 类型的输入流（即不支持 DELETE 或 UPDATE_BEFORE 等撤回消息），并且仅支持事件时间定时器来触发计算。
public abstract class WindowJoinOperator extends TableStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                Triggerable<RowData, Long>,
                KeyContext {

    private static final long serialVersionUID = 1L;
    // Flink Metrics 的名称常量，用于报告迟到数据的数量、丢弃率和 Watermark 延迟。
    private static final String LEFT_LATE_ELEMENTS_DROPPED_METRIC_NAME =
            "leftNumLateRecordsDropped";
    private static final String LEFT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME =
            "leftLateRecordsDroppedRate";
    private static final String RIGHT_LATE_ELEMENTS_DROPPED_METRIC_NAME =
            "rightNumLateRecordsDropped";
    private static final String RIGHT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME =
            "rightLateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";
    private static final String LEFT_RECORDS_STATE_NAME = "left-records";
    private static final String RIGHT_RECORDS_STATE_NAME = "right-records";
    // 用于对左右输入流的 RowData 进行序列化/反序列化和深拷贝，以便安全地存入状态。
    protected final RowDataSerializer leftSerializer;
    protected final RowDataSerializer rightSerializer;
    // 由 Flink Code Generator 生成的连接条件（Java 代码），封装了 Join 谓词逻辑。
    private final GeneratedJoinCondition generatedJoinCondition;
    // 左/右输入流中，窗口结束时间戳所在的字段索引。
    // 窗口结束时间戳用于确定数据所属的窗口和注册定时器。
    private final int leftWindowEndIndex;
    private final int rightWindowEndIndex;
    // 布尔数组，指示在执行 Join 谓词时是否需要对 Join Key 中的某些字段进行 Null 值过滤（即 NULL IS NOT NULL 语义）。
    private final boolean[] filterNullKeys;
    // 配置的时区，用于在处理和计算窗口时间戳时进行时区转换，特别是与 TIMESTAMP_LTZ 类型相关。
    private final ZoneId shiftTimeZone;
    // 窗口定时器服务。
    // 封装了 Flink 底层定时器服务的接口，并结合 shiftTimeZone，专门用于注册和查询窗口时间（SlicingWindowTimerServiceImpl 是其实现）。
    private transient WindowTimerService<Long> windowTimerService;

    // ------------------------------------------------------------------------
    // 运行时连接条件实例。
    // 它是 GeneratedJoinCondition 实例化后的对象，包含实际的 apply(left, right) 逻辑，并且加入了 Null Key 过滤的逻辑。
    protected transient JoinConditionWithNullFilters joinCondition;

    /** This is used for emitting elements with a given timestamp. */
    // 时间戳收集器。
    // 用于将连接后的结果行 (RowData) 发送给下游操作符。
    // 它会自动将输出记录的时间戳抹除（collector.eraseTimestamp()）
    protected transient TimestampedCollector<RowData> collector;
    // 窗口列表状态。
    // 封装了 Keyed State，用于在窗口关闭前，按窗口结束时间戳 (Long 命名空间) 存储左/右输入流中的所有 RowData 记录。
    private transient WindowListState<Long> leftWindowState;
    private transient WindowListState<Long> rightWindowState;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    // 运行时指标（Metrics），用于统计迟到丢弃记录数、丢弃率，以及 Watermark 延迟时间。
    private transient Counter leftNumLateRecordsDropped;
    private transient Meter leftLateRecordsDroppedRate;
    private transient Counter rightNumLateRecordsDropped;
    private transient Meter rightLateRecordsDroppedRate;
    private transient Gauge<Long> watermarkLatency;

    WindowJoinOperator(
            TypeSerializer<RowData> leftSerializer,
            TypeSerializer<RowData> rightSerializer,
            GeneratedJoinCondition generatedJoinCondition,
            int leftWindowEndIndex,
            int rightWindowEndIndex,
            boolean[] filterNullKeys,
            ZoneId shiftTimeZone) {
        this.leftSerializer = (RowDataSerializer) leftSerializer;
        this.rightSerializer = (RowDataSerializer) rightSerializer;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftWindowEndIndex = leftWindowEndIndex;
        this.rightWindowEndIndex = rightWindowEndIndex;
        this.filterNullKeys = filterNullKeys;
        this.shiftTimeZone = shiftTimeZone;
    }
    // 初始化操作符
    @Override
    public void open() throws Exception {
        super.open();
        // 初始化 collector 并抹除时间戳
        this.collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        final LongSerializer windowSerializer = LongSerializer.INSTANCE;
        // 初始化 InternalTimerService 和 windowTimerService (SlicingWindowTimerServiceImpl)
        InternalTimerService<Long> internalTimerService =
                getInternalTimerService("window-timers", windowSerializer, this);
        this.windowTimerService =
                new SlicingWindowTimerServiceImpl(internalTimerService, shiftTimeZone);

        // init join condition
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(DefaultOpenContext.INSTANCE);

        // init state
        // 初始化左右两侧的 WindowListState (leftWindowState, rightWindowState)，用于缓存数据
        ListStateDescriptor<RowData> leftRecordStateDesc =
                new ListStateDescriptor<>(LEFT_RECORDS_STATE_NAME, leftSerializer);
        ListState<RowData> leftListState =
                getOrCreateKeyedState(windowSerializer, leftRecordStateDesc);
        this.leftWindowState =
                new WindowListState<>((InternalListState<RowData, Long, RowData>) leftListState);

        ListStateDescriptor<RowData> rightRecordStateDesc =
                new ListStateDescriptor<>(RIGHT_RECORDS_STATE_NAME, rightSerializer);
        ListState<RowData> rightListState =
                getOrCreateKeyedState(windowSerializer, rightRecordStateDesc);
        this.rightWindowState =
                new WindowListState<>((InternalListState<RowData, Long, RowData>) rightListState);

        // metrics
        this.leftNumLateRecordsDropped = metrics.counter(LEFT_LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.leftLateRecordsDroppedRate =
                metrics.meter(
                        LEFT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(leftNumLateRecordsDropped));
        this.rightNumLateRecordsDropped = metrics.counter(RIGHT_LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.rightLateRecordsDroppedRate =
                metrics.meter(
                        RIGHT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(rightNumLateRecordsDropped));
        this.watermarkLatency =
                metrics.gauge(
                        WATERMARK_LATENCY_METRIC_NAME,
                        () -> {
                            long watermark = windowTimerService.currentWatermark();
                            if (watermark < 0) {
                                return 0L;
                            } else {
                                return windowTimerService.currentProcessingTime() - watermark;
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        collector = null;
        if (joinCondition != null) {
            joinCondition.close();
        }
    }
    // 处理来自输入流 1/2 的元素。
    // 只是简单地调用私有方法 processElement 来处理记录。
    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element, leftWindowEndIndex, leftLateRecordsDroppedRate, leftWindowState);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element, rightWindowEndIndex, rightLateRecordsDroppedRate, rightWindowState);
    }
    // 核心输入处理逻辑
    private void processElement(
            StreamRecord<RowData> element,
            int windowEndIndex,
            Meter lateRecordsDroppedRate,
            WindowListState<Long> recordState)
            throws Exception {
        // 获取输入行的窗口结束时间戳 (windowEnd)
        RowData inputRow = element.getValue();
        long windowEnd = inputRow.getLong(windowEndIndex);
        // 迟到判断： 检查 windowEnd 是否小于当前 Watermark（使用 isWindowFired）。
        // 如果是，说明记录迟到，增加迟到计数，并丢弃记录后返回
        if (isWindowFired(windowEnd, windowTimerService.currentWatermark(), shiftTimeZone)) {
            // element is late and should be dropped
            lateRecordsDroppedRate.markEvent();
            return;
        }
        // 状态存储： 如果不是迟到数据且是 INSERT 消息，则将记录添加到对应的 WindowListState 中，以 windowEnd 作为命名空间
        if (RowDataUtil.isAccumulateMsg(inputRow)) {
            recordState.add(windowEnd, inputRow);
        } else {
            // Window join could not handle retraction input stream
            throw new UnsupportedOperationException(
                    "This is a bug and should not happen. Please file an issue.");
        }
        // 注册定时器： 调用 windowTimerService.registerEventTimeWindowTimer(windowEnd) 为该窗口注册一个事件时间定时器。
        // always register time for every element
        windowTimerService.registerEventTimeWindowTimer(windowEnd);
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, Long> timer) throws Exception {
        // Window join only support event-time now
        throw new UnsupportedOperationException(
                "This is a bug and should not happen. Please file an issue.");
    }
    // 处理事件时间定时器触发（核心）
    @Override
    public void onEventTime(InternalTimer<RowData, Long> timer) throws Exception {
        // 设置 Key/Namespace： 设置当前 Key 和窗口命名空间 (window = timer.getNamespace())
        setCurrentKey(timer.getKey());
        Long window = timer.getNamespace();
        // join left records and right records
        // 获取状态数据
        List<RowData> leftData = leftWindowState.get(window);
        List<RowData> rightData = rightWindowState.get(window);
        join(leftData, rightData);
        // clear state
        if (leftData != null) {
            leftWindowState.clear(window);
        }
        if (rightData != null) {
            rightWindowState.clear(window);
        }
    }
    // 执行连接逻辑。 这是留给子类实现的核心连接逻辑
    public abstract void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords);
    // 实现 Semi Join (WHERE EXISTS) 或 Anti Join (WHERE NOT EXISTS)
    // join 方法遍历左侧记录，对每条左侧记录，检查右侧记录集中是否存在至少一条匹配的记录。
    // 如果是 Semi Join (!isAntiJoin)，找到匹配就输出左侧记录。
    // 如果是 Anti Join (isAntiJoin)，找不到匹配才输出左侧记录。
    static class SemiAntiJoinOperator extends WindowJoinOperator {

        private final boolean isAntiJoin;

        SemiAntiJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                boolean isAntiJoin,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
            this.isAntiJoin = isAntiJoin;
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null) {
                return;
            }
            if (rightRecords == null) {
                if (isAntiJoin) {
                    for (RowData leftRecord : leftRecords) {
                        collector.collect(leftRecord);
                    }
                }
                return;
            }
            for (RowData leftRecord : leftRecords) {
                boolean matches = false;
                for (RowData rightRecord : rightRecords) {
                    if (joinCondition.apply(leftRecord, rightRecord)) {
                        matches = true;
                        break;
                    }
                }
                if (matches) {
                    if (!isAntiJoin) {
                        // emit left record if there are matched rows on the other side
                        collector.collect(leftRecord);
                    }
                } else {
                    if (isAntiJoin) {
                        // emit left record if there is no matched row on the other side
                        collector.collect(leftRecord);
                    }
                }
            }
        }
    }

    static class InnerJoinOperator extends WindowJoinOperator {
        private transient JoinedRowData outRow;

        InnerJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void open() throws Exception {
            super.open();
            outRow = new JoinedRowData();
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null || rightRecords == null) {
                return;
            }
            for (RowData leftRecord : leftRecords) {
                for (RowData rightRecord : rightRecords) {
                    if (joinCondition.apply(leftRecord, rightRecord)) {
                        outRow.setRowKind(RowKind.INSERT);
                        outRow.replace(leftRecord, rightRecord);
                        collector.collect(outRow);
                    }
                }
            }
        }
    }

    private abstract static class AbstractOuterJoinOperator extends WindowJoinOperator {

        private static final long serialVersionUID = 1L;

        private transient RowData leftNullRow;
        private transient RowData rightNullRow;
        private transient JoinedRowData outRow;

        AbstractOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void open() throws Exception {
            super.open();
            leftNullRow = new GenericRowData(leftSerializer.getArity());
            rightNullRow = new GenericRowData(rightSerializer.getArity());
            outRow = new JoinedRowData();
        }

        protected void outputNullPadding(RowData row, boolean isLeft) {
            if (isLeft) {
                outRow.replace(row, rightNullRow);
            } else {
                outRow.replace(leftNullRow, row);
            }
            outRow.setRowKind(RowKind.INSERT);
            collector.collect(outRow);
        }

        protected void outputNullPadding(Iterable<RowData> rows, boolean isLeft) {
            for (RowData row : rows) {
                outputNullPadding(row, isLeft);
            }
        }

        protected void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
            if (inputIsLeft) {
                outRow.replace(inputRow, otherRow);
            } else {
                outRow.replace(otherRow, inputRow);
            }
            outRow.setRowKind(RowKind.INSERT);
            collector.collect(outRow);
        }
    }

    static class LeftOuterJoinOperator extends AbstractOuterJoinOperator {

        private static final long serialVersionUID = 1L;

        LeftOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null) {
                return;
            }
            if (rightRecords == null) {
                outputNullPadding(leftRecords, true);
            } else {
                for (RowData leftRecord : leftRecords) {
                    boolean matches = false;
                    for (RowData rightRecord : rightRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                        }
                    }
                    if (!matches) {
                        // padding null for left side
                        outputNullPadding(leftRecord, true);
                    }
                }
            }
        }
    }

    static class RightOuterJoinOperator extends AbstractOuterJoinOperator {

        private static final long serialVersionUID = 1L;

        RightOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (rightRecords == null) {
                return;
            }
            if (leftRecords == null) {
                outputNullPadding(rightRecords, false);
            } else {
                for (RowData rightRecord : rightRecords) {
                    boolean matches = false;
                    for (RowData leftRecord : leftRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                        }
                    }
                    if (!matches) {
                        outputNullPadding(rightRecord, false);
                    }
                }
            }
        }
    }

    static class FullOuterJoinOperator extends AbstractOuterJoinOperator {

        private static final long serialVersionUID = 1L;

        FullOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null && rightRecords == null) {
                return;
            }
            if (rightRecords == null) {
                outputNullPadding(leftRecords, true);
            } else if (leftRecords == null) {
                outputNullPadding(rightRecords, false);
            } else {
                IdentityHashMap<RowData, Boolean> emittedRightRecords = new IdentityHashMap<>();
                for (RowData leftRecord : leftRecords) {
                    boolean matches = false;
                    for (RowData rightRecord : rightRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                            emittedRightRecords.put(rightRecord, Boolean.TRUE);
                        }
                    }
                    // padding null for left side
                    if (!matches) {
                        outputNullPadding(leftRecord, true);
                    }
                }
                // padding null for never emitted right side
                for (RowData rightRecord : rightRecords) {
                    if (!emittedRightRecords.containsKey(rightRecord)) {
                        outputNullPadding(rightRecord, false);
                    }
                }
            }
        }
    }
}
