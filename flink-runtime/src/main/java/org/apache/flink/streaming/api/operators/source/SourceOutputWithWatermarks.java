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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of the SourceOutput. The records emitted to this output are pushed into a given
 * {@link PushingAsyncDataInput.DataOutput}. The watermarks are pushed into the same output, or into
 * a separate {@link WatermarkOutput}, if one is provided.
 *
 * <h2>Periodic Watermarks</h2>
 *
 * <p>This output does not implement automatic periodic watermark emission. The method {@link
 * SourceOutputWithWatermarks#emitPeriodicWatermark()} needs to be called periodically.
 *
 * <h2>Note on Performance Considerations</h2>
 *
 * <p>The methods {@link SourceOutput#collect(Object)} and {@link SourceOutput#collect(Object,
 * long)} are highly performance-critical (part of the hot loop). To make the code as JIT friendly
 * as possible, we want to have only a single implementation of these two methods, across all
 * classes. That way, the JIT compiler can de-virtualize (and inline) them better.
 *
 * <p>Currently, we have one implementation of these methods for the case where we don't need
 * watermarks (see class {@link NoOpTimestampsAndWatermarks}) and one for the case where we do (this
 * class). When the JVM is dedicated to a single job (or type of job) only one of these classes will
 * be loaded. In mixed job setups, we still have a bimorphic method (rather than a
 * poly/-/mega-morphic method).
 *
 * @param <T> The type of emitted records.
 */
//用于 向数据流发送带有时间戳和水印的记录 的内部实现类
    //整合时间戳和水印生成逻辑：它将 TimestampAssigner（时间戳分配器）和 WatermarkGenerator（水印生成器）的逻辑封装在一起。每当有记录从数据源读取时，它会：
    //使用 TimestampAssigner 为记录分配一个时间戳
    //使用 WatermarkGenerator 根据新到达的事件更新水印
@Internal
public class SourceOutputWithWatermarks<T> implements SourceOutput<T> {
    //记录输出的通道。所有带有时间戳的 StreamRecord 都会通过这个通道发送到下游算子
    private final PushingAsyncDataInput.DataOutput<T> recordsOutput;
    //时间戳分配器。它负责为从数据源读取的每条记录分配一个事件时间戳
    private final TimestampAssigner<T> timestampAssigner;
    //水印生成器。它根据事件的到达情况，生成事件时间水印
    private final WatermarkGenerator<T> watermarkGenerator;
    //事件驱动水印的输出通道。WatermarkGenerator 在每次处理事件后生成的水印会通过这个通道发送
    private final WatermarkOutput onEventWatermarkOutput;
    //周期性水印的输出通道。WatermarkGenerator 在定时器触发时生成的水印会通过这个通道发送
    private final WatermarkOutput periodicWatermarkOutput;
    //可重用的 StreamRecord 对象。为了提高性能，它避免了每次处理记录时都创建新对象，而是通过 replace 方法来更新其内容
    private final StreamRecord<T> reusingRecord;

    /**
     * Creates a new SourceOutputWithWatermarks that emits records to the given DataOutput and
     * watermarks to the (possibly different) WatermarkOutput.
     */
    protected SourceOutputWithWatermarks(
            PushingAsyncDataInput.DataOutput<T> recordsOutput,
            WatermarkOutput onEventWatermarkOutput,
            WatermarkOutput periodicWatermarkOutput,
            TimestampAssigner<T> timestampAssigner,
            WatermarkGenerator<T> watermarkGenerator) {

        this.recordsOutput = checkNotNull(recordsOutput);
        this.onEventWatermarkOutput = checkNotNull(onEventWatermarkOutput);
        this.periodicWatermarkOutput = checkNotNull(periodicWatermarkOutput);
        this.timestampAssigner = checkNotNull(timestampAssigner);
        this.watermarkGenerator = checkNotNull(watermarkGenerator);
        this.reusingRecord = new StreamRecord<>(null);
    }

    // ------------------------------------------------------------------------
    // SourceOutput Methods
    //
    // Note that the two methods below are final, as a partial enforcement
    // of the performance design goal mentioned in the class-level comment.
    // ------------------------------------------------------------------------

    @Override
    public final void collect(T record) {
        collect(record, TimestampAssigner.NO_TIMESTAMP);
    }
    //用于发送带时间戳的记录
    @Override
    public final void collect(T record, long timestamp) {
        try {
            //为记录分配一个最终的时间戳
            final long assignedTimestamp = timestampAssigner.extractTimestamp(record, timestamp);

            // IMPORTANT: The event must be emitted before the watermark generator is called.
            //将带有新时间戳的记录发送到 recordsOutput 通道
            recordsOutput.emitRecord(reusingRecord.replace(record, assignedTimestamp));
            //通知水印生成器新事件已到达，并可能由此生成一个新的水印
            watermarkGenerator.onEvent(record, assignedTimestamp, onEventWatermarkOutput);
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    // ------------------------------------------------------------------------
    // WatermarkOutput Methods
    //
    // These two methods are final as well, to enforce the contract that the
    // watermarks from emitWatermark(Watermark) go to the same output as the
    // watermarks from the watermarkGenerator.onEvent(...) calls in the collect(...)
    // methods.
    // ------------------------------------------------------------------------

    @Override
    public final void emitWatermark(Watermark watermark) {
        onEventWatermarkOutput.emitWatermark(watermark);
    }

    @Override
    public final void markIdle() {
        onEventWatermarkOutput.markIdle();
    }

    @Override
    public void markActive() {
        onEventWatermarkOutput.markActive();
    }

    public final void emitPeriodicWatermark() {
        watermarkGenerator.onPeriodicEmit(periodicWatermarkOutput);
    }

    // ------------------------------------------------------------------------
    // Factories
    // ------------------------------------------------------------------------

    /**
     * Creates a new SourceOutputWithWatermarks that emits records to the given DataOutput and
     * watermarks to the different WatermarkOutputs.
     */
    public static <E> SourceOutputWithWatermarks<E> createWithSeparateOutputs(
            PushingAsyncDataInput.DataOutput<E> recordsOutput,
            WatermarkOutput onEventWatermarkOutput,
            WatermarkOutput periodicWatermarkOutput,
            TimestampAssigner<E> timestampAssigner,
            WatermarkGenerator<E> watermarkGenerator) {

        return new SourceOutputWithWatermarks<>(
                recordsOutput,
                onEventWatermarkOutput,
                periodicWatermarkOutput,
                timestampAssigner,
                watermarkGenerator);
    }
}
