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
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * The variant of {@link PullingAsyncDataInput} that is defined for handling both network input and
 * source input in a unified way via {@link #emitNext(DataOutput)} instead of returning {@code
 * Optional.empty()} via {@link PullingAsyncDataInput#pollNext()}.
 */
// 统一输入处理模型： 为 Flink 任务定义了一个统一的异步数据输入机制，适用于从网络（上游 Task 的输出）和数据源（Source Task 的输入）获取数据。
// 推模式（Pushing）： 区别于传统的拉模式（Pulling）。在这种模式下，数据输入方（例如 InputGate 或 SourceReader）负责主动将数据推送到下游的输出处理逻辑（由 DataOutput 接口定义），而不是等待下游主动调用 pollNext() 来拉取数据。
// 支持背压（Backpressure）： 通过返回的 DataInputStatus 来通知调用方当前输入的状态，从而在没有数据时暂停拉取，实现流量控制和背压。
@Internal
public interface PushingAsyncDataInput<T> extends AvailabilityProvider {

    /**
     * Pushes elements to the output from current data input, and returns the input status to
     * indicate whether there are more available data in current input.
     *
     * <p>This method should be non blocking.
     */
    // 推送下一个元素
    // 负责从当前输入（如网络缓冲区或 Source）中读取下一个可用元素（数据记录或事件），并立即通过提供的 output 接口将其推送到下游
    DataInputStatus emitNext(DataOutput<T> output) throws Exception;

    /**
     * Basic data output interface used in emitting the next element from data input.
     *
     * @param <T> The type encapsulated with the stream record.
     */
    // 定义了 Task 线程用来接收和处理推入数据的具体方法。这些方法通常将数据和事件转发到 Task 的运行时逻辑中（如 StreamTask）
    interface DataOutput<T> {
        // 发出数据记录。
        // 用于处理和向下游算子发送包含实际用户数据的 StreamRecord
        void emitRecord(StreamRecord<T> streamRecord) throws Exception;

        void emitWatermark(Watermark watermark) throws Exception;

        void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception;

        void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception;

        void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception;
    }
}
