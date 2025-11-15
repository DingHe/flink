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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * Interface for stream operators with two inputs. Use {@link
 * org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if you want to
 * implement a custom operator.
 *
 * @param <IN1> The input type of the operator
 * @param <IN2> The input type of the operator
 * @param <OUT> The output type of the operator
 */
// 处理双输入流的算子的接口
// 定义双流处理逻辑：它规定了如何处理来自第一个输入流 (IN1) 和第二个输入流 (IN2) 的元素
// 处理控制流数据：除了数据元素本身，它还定义了如何处理来自两个输入流的控制流数据，例如水印（Watermark）、延迟标记（LatencyMarker）和水印状态（WatermarkStatus）
// IN1：表示第一个输入流中元素的类型
// IN2：表示第二个输入流中元素的类型
// OUT：表示输出流中元素的类型
@PublicEvolving
public interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

    /**
     * Processes one element that arrived on the first input of this two-input operator. This method
     * is guaranteed to not be called concurrently with other methods of the operator.
     */
    // 处理来自第一个输入流的单个元素
    void processElement1(StreamRecord<IN1> element) throws Exception;

    /**
     * Processes one element that arrived on the second input of this two-input operator. This
     * method is guaranteed to not be called concurrently with other methods of the operator.
     */
    // 处理来自第二个输入流的单个元素
    void processElement2(StreamRecord<IN2> element) throws Exception;

    /**
     * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    // 处理来自第一个输入流的水印（Watermark）
    void processWatermark1(Watermark mark) throws Exception;

    /**
     * Processes a {@link Watermark} that arrived on the second input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    // 处理来自第二个输入流的水印
    void processWatermark2(Watermark mark) throws Exception;

    /**
     * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
    // 处理来自第一个输入流的延迟标记（LatencyMarker）
    void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception;

    /**
     * Processes a {@link LatencyMarker} that arrived on the second input of this two-input
     * operator. This method is guaranteed to not be called concurrently with other methods of the
     * operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
    // 处理来自第二个输入流的延迟标记
    void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception;

    /**
     * Processes a {@link WatermarkStatus} that arrived on the first input of this two-input
     * operator. This method is guaranteed to not be called concurrently with other methods of the
     * operator.
     *
     * @see org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus
     */
    // 处理来自第一个输入流的水印状态（WatermarkStatus）
    void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception;

    /**
     * Processes a {@link WatermarkStatus} that arrived on the second input of this two-input
     * operator. This method is guaranteed to not be called concurrently with other methods of the
     * operator.
     *
     * @see org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus
     */
    // 处理来自第二个输入流的水印状态
    void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception;

    /**
     * Processes a {@link RecordAttributes} that arrived on the first input of this operator. This
     * method is guaranteed to not be called concurrently with other methods of the operator.
     */
    // 处理来自第一个输入流的记录属性（RecordAttributes）
    @Experimental
    default void processRecordAttributes1(RecordAttributes recordAttributes) throws Exception {}

    /**
     * Processes a {@link RecordAttributes} that arrived on the second input of this operator. This
     * method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Experimental
    default void processRecordAttributes2(RecordAttributes recordAttributes) throws Exception {}
}
