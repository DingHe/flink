/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

// ChainingOutput 是实现 Operator Chaining（算子链） 机制的核心组件。
// 在 Flink 中，为了减少任务间的数据传输开销（如序列化/反序列化、网络传输），多个逻辑算子通常会被合并成一个算子链并在同一个线程内执行。
// 核心作用是充当算子链中两个相邻算子之间的“桥梁”：
// 同步传递： 它将上游算子的输出直接作为下游算子的输入（通过方法调用实现），而不是放入缓冲区或网络。
// 指标统计： 它负责维护算子链传递过程中的计数逻辑（如上游的输出记录数和下游的输入记录数）。
// 控制信号转发： 除了普通数据，它还负责将水位线（Watermark）、延迟标记（Latency Marker）和水位线状态（Watermark Status）传递给下游。
class ChainingOutput<T>
        implements WatermarkGaugeExposingOutput<StreamRecord<T>>,
                OutputWithChainingCheck<StreamRecord<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(ChainingOutput.class);
    // 下游算子的抽象接口，
    // 通过调用它的 processElement 等方法来传递数据。
    protected final Input<T> input;
    // 记录上游算子的输出数量
    protected final Counter numRecordsOut;
    // 记录下游算子的输入数量
    protected final Counter numRecordsIn;
    // 用于度量当前的 Watermark 值，方便监控
    protected final WatermarkGauge watermarkGauge = new WatermarkGauge();
    // 用于标识“旁路输出（Side Output）”。
    // 如果为空，则表示这是主流输出。
    @Nullable protected final OutputTag<T> outputTag;
    // 记录当前的水位线状态（活跃或空闲），避免重复发送相同的状态。
    protected WatermarkStatus announcedStatus = WatermarkStatus.ACTIVE;
    // 专门用于处理记录的传递，减少虚函数调用的开销。
    protected final ThrowingConsumer<StreamRecord<T>, Exception> recordProcessor;

    public ChainingOutput(
            Input<T> input,
            @Nullable Counter prevNumRecordsOut,
            OperatorMetricGroup curOperatorMetricGroup,
            @Nullable OutputTag<T> outputTag) {
        this.input = input;
        if (prevNumRecordsOut != null) {
            this.numRecordsOut = prevNumRecordsOut;
        } else {
            // Uses a dummy counter here to avoid checking the existence of numRecordsOut on the
            // per-record path.
            this.numRecordsOut = new SimpleCounter();
        }
        this.numRecordsIn = curOperatorMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
        this.outputTag = outputTag;
        this.recordProcessor = RecordProcessorUtils.getRecordProcessor(input);
    }

    // 处理主流输出
    // 如果当前 ChainingOutput 设置了 outputTag（说明它是负责旁路输出的），则忽略主流数据；
    // 否则调用 pushToOperator

    @Override
    public void collect(StreamRecord<T> record) {
        if (this.outputTag != null) {
            // we are not responsible for emitting to the main output.
            return;
        }

        pushToOperator(record);
    }
    // 处理旁路输出
    // 检查传入的 outputTag 是否与自身负责的 Tag 匹配。只有匹配时，才将数据推送到下游。
    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        if (OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
            pushToOperator(record);
        }
    }
    // 用于判断下游是否是链式调用的
    // 直接调用 collect 并返回 false（因为在这里数据已经同步处理完了）
    @Override
    public boolean collectAndCheckIfChained(StreamRecord<T> record) {
        collect(record);
        return false;
    }

    @Override
    public <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record) {
        collect(outputTag, record);
        return false;
    }
    // 真正将数据交给下游算子
    protected <X> void pushToOperator(StreamRecord<X> record) {
        try {
            // we know that the given outputTag matches our OutputTag so the record
            // must be of the type that our operator expects.
            @SuppressWarnings("unchecked")
            StreamRecord<T> castRecord = (StreamRecord<T>) record;
            //  同时增加上游的 numRecordsOut 和下游的 numRecordsIn 指标
            numRecordsOut.inc();
            numRecordsIn.inc();
            // 将数据交给下游
            recordProcessor.accept(castRecord);
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }
    // 将 Watermark 传递给下游
    // 如果当前状态是 IDLE（空闲），则不发送；否则更新本地的 watermarkGauge 并调用下游的 processWatermark。
    @Override
    public void emitWatermark(Watermark mark) {
        if (announcedStatus.isIdle()) {
            return;
        }
        try {
            watermarkGauge.setCurrentWatermark(mark.getTimestamp());
            input.processWatermark(mark);
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }
    // 传递延迟测量标记
    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        try {
            input.processLatencyMarker(latencyMarker);
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    @Override
    public void close() {
        // nothing is owned by ChainingOutput and should be closed, see FLINK-20888
    }

    @Override
    public Gauge<Long> getWatermarkGauge() {
        return watermarkGauge;
    }

    // 传递算子的活跃/空闲状态。
    // 只有当状态发生变化时才向下游传递，减少冗余调用。
    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        if (!announcedStatus.equals(watermarkStatus)) {
            announcedStatus = watermarkStatus;
            try {
                input.processWatermarkStatus(watermarkStatus);
            } catch (Exception e) {
                throw new ExceptionInChainedOperatorException(e);
            }
        }
    }
    // 传递记录属性（如数据新鲜度等元数据）
    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {
        try {
            input.processRecordAttributes(recordAttributes);
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }
}
