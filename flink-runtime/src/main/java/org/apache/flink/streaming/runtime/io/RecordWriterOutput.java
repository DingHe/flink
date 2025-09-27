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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.InternalWatermark;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OutputWithChainingCheck;
import org.apache.flink.streaming.runtime.tasks.WatermarkGaugeExposingOutput;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
//RecordWriterOutput 是 Flink 流处理框架中的一个核心类，用于将流数据从一个算子（Operator）的输出端传递到下游算子的输入端。它使用 RecordWriter 实现数据的序列化和传输
/** Implementation of {@link Output} that sends data using a {@link RecordWriter}. */
@Internal
public class RecordWriterOutput<OUT>
        implements WatermarkGaugeExposingOutput<StreamRecord<OUT>>,
                OutputWithChainingCheck<StreamRecord<OUT>> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordWriterOutput.class);
    //核心的底层数据传输组件，负责将序列化后的数据发送到网络缓冲区。支持数据的分区（Partitioning）、广播（Broadcasting）等模式
    private RecordWriter<SerializationDelegate<StreamElement>> recordWriter;
    //一个封装器，用于将流元素 (StreamElement) 进行序列化。它将实际的记录数据包装起来，方便通过 RecordWriter 传输
    private SerializationDelegate<StreamElement> serializationDelegate;
    //是否支持非对齐检查点（Unaligned Checkpoints）。用于优化检查点处理时的吞吐量
    private final boolean supportsUnalignedCheckpoints;
    //用于标识是否是特定的侧输出（Side Output）。主输出通常不需要此标签
    private final OutputTag outputTag;
    //用于监控和记录当前的水位线（Watermark），方便度量和调试
    private final WatermarkGauge watermarkGauge = new WatermarkGauge();
    //当前的水位状态，可能是 ACTIVE 或 IDLE。控制是否可以发射 Watermark
    private WatermarkStatus announcedStatus = WatermarkStatus.ACTIVE;

    // Uses a dummy counter here to avoid checking the existence of numRecordsOut on the
    // per-record path. 记录输出记录数量的计数器。默认使用 SimpleCounter，可以自定义
    private Counter numRecordsOut = new SimpleCounter();

    @SuppressWarnings("unchecked")
    public RecordWriterOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            TypeSerializer<OUT> outSerializer,
            OutputTag outputTag,
            boolean supportsUnalignedCheckpoints) {

        checkNotNull(recordWriter);
        this.outputTag = outputTag;
        // generic hack: cast the writer to generic Object type so we can use it
        // with multiplexed records and watermarks
        this.recordWriter =
                (RecordWriter<SerializationDelegate<StreamElement>>) (RecordWriter<?>) recordWriter;

        TypeSerializer<StreamElement> outRecordSerializer =
                new StreamElementSerializer<>(outSerializer);

        if (outSerializer != null) {
            serializationDelegate = new SerializationDelegate<>(outRecordSerializer);
        }

        this.supportsUnalignedCheckpoints = supportsUnalignedCheckpoints;
    }

    @Override  //通过recordWriter把数据发送到下游，如果数据被处理为链式（chained），计数器自增
    public void collect(StreamRecord<OUT> record) {
        if (collectAndCheckIfChained(record)) {
            numRecordsOut.inc();
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        if (collectAndCheckIfChained(outputTag, record)) {
            numRecordsOut.inc();
        }
    }

    @Override
    public boolean collectAndCheckIfChained(StreamRecord<OUT> record) {
        if (this.outputTag != null) {
            // we are not responsible for emitting to the main output.
            return false;
        }

        pushToRecordWriter(record);
        return true;
    }

    @Override //将带有特定 OutputTag 的侧输出发送到下游
    public <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record) {
        if (!OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
            // we are not responsible for emitting to the side-output specified by this
            // OutputTag.
            return false;
        }

        pushToRecordWriter(record);
        return true;
    }
    //使用RecordWriter把序列化的StreamRecord发送出去
    private <X> void pushToRecordWriter(StreamRecord<X> record) {
        serializationDelegate.setInstance(record);

        try {
            recordWriter.emit(serializationDelegate);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public void emitWatermark(Watermark mark) {
        if (announcedStatus.isIdle()) {
            return;
        }

        watermarkGauge.setCurrentWatermark(mark.getTimestamp());
        //检查 recordWriter 是否支持子分区推导
        if (recordWriter.isSubpartitionDerivable()) {
            serializationDelegate.setInstance(mark);

            try {
                recordWriter.broadcastEmit(serializationDelegate); //如果支持，直接广播水位线
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        } else {
            for (int i = 0; i < recordWriter.getNumberOfSubpartitions(); i++) { //如果不支持，为每个分区分别发送水位线
                serializationDelegate.setInstance(new InternalWatermark(mark.getTimestamp(), i));

                try {
                    recordWriter.emit(serializationDelegate, i);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        if (!announcedStatus.equals(watermarkStatus)) {
            announcedStatus = watermarkStatus;
            serializationDelegate.setInstance(watermarkStatus);
            try {
                recordWriter.broadcastEmit(serializationDelegate);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
    }

    @Override //发送延迟标记，用于监控数据延迟
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        serializationDelegate.setInstance(latencyMarker);

        try {
            recordWriter.randomEmit(serializationDelegate);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    public void setNumRecordsOut(Counter numRecordsOut) {
        this.numRecordsOut = checkNotNull(numRecordsOut);
    }
    //广播事件到所有分区
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        if (event instanceof CheckpointBarrier && !supportsUnalignedCheckpoints) {
            final CheckpointBarrier barrier = (CheckpointBarrier) event;
            event = barrier.withOptions(barrier.getCheckpointOptions().withUnalignedUnsupported());
            isPriorityEvent = false;
        }
        recordWriter.broadcastEvent(event, isPriorityEvent);
    }
   //检查点对齐超时时触发的逻辑
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        recordWriter.alignedBarrierTimeout(checkpointId);
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        recordWriter.abortCheckpoint(checkpointId, cause);
    }

    public void flush() throws IOException {
        recordWriter.flushAll();
    }

    @Override
    public void close() {
        recordWriter.close();
    }

    @Override
    public Gauge<Long> getWatermarkGauge() {
        return watermarkGauge;
    }

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {
        if (!recordWriter.isSubpartitionDerivable()) {
            LOG.warn(
                    recordAttributes
                            + " will be ignored, because its correctness cannot not be "
                            + "guaranteed when the subpartition information is not derivable.");
            return;
        }

        try {
            serializationDelegate.setInstance(recordAttributes);
            recordWriter.broadcastEmit(serializationDelegate);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
