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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/** An element in a data stream. Can be a record, a Watermark, or a RecordAttributes. */
// 在 Flink 的流处理世界中，操作符（如 Source、Map、Window）不仅仅处理用户数据，还需要处理各种控制信息（如时间戳、检查点、背压信号等）。StreamElement 将这些不同类型的“流元素”统一起来：
// 数据/控制流混合： 它允许用户数据（StreamRecord）和控制信号（Watermark、LatencyMarker、WatermarkStatus）在同一个流、同一个网络通道中传输，简化了流处理的管道设计。
// 多态性封装： Flink 运行时只需处理 StreamElement，并通过其 is...() 方法确定具体类型，然后通过 as...() 方法安全地进行类型转换和处理。
@Internal
public abstract class StreamElement {

    /**
     * Checks whether this element is a watermark.
     *
     * @return True, if this element is a watermark, false otherwise.
     */
    // 检查是否为水位线。
    public final boolean isWatermark() {
        return this instanceof Watermark;
    }

    /**
     * Checks whether this element is a watermark status.
     *
     * @return True, if this element is a watermark status, false otherwise.
     */
    // 检查是否为水位线状态
    public final boolean isWatermarkStatus() {
        return getClass() == WatermarkStatus.class;
    }

    /**
     * Checks whether this element is a record.
     *
     * @return True, if this element is a record, false otherwise.
     */
    // 检查是否为用户数据记录
    public final boolean isRecord() {
        return getClass() == StreamRecord.class;
    }

    /**
     * Checks whether this element is a latency marker.
     *
     * @return True, if this element is a latency marker, false otherwise.
     */
    // 检查是否为延迟标记
    public final boolean isLatencyMarker() {
        return getClass() == LatencyMarker.class;
    }

    /**
     * Check whether this element is record attributes.
     *
     * @return True, if this element is record attributes, false otherwise.
     */
    // 检查是否为记录属性
    public final boolean isRecordAttributes() {
        return getClass() == RecordAttributes.class;
    }

    /**
     * Casts this element into a StreamRecord.
     *
     * @return This element as a stream record.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a stream record.
     */
    @SuppressWarnings("unchecked")
    public final <E> StreamRecord<E> asRecord() {
        return (StreamRecord<E>) this;
    }

    /**
     * Casts this element into a Watermark.
     *
     * @return This element as a Watermark.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a Watermark.
     */
    public final Watermark asWatermark() {
        return (Watermark) this;
    }

    /**
     * Casts this element into a WatermarkStatus.
     *
     * @return This element as a WatermarkStatus.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a Watermark
     *     Status.
     */
    public final WatermarkStatus asWatermarkStatus() {
        return (WatermarkStatus) this;
    }

    /**
     * Casts this element into a LatencyMarker.
     *
     * @return This element as a LatencyMarker.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a LatencyMarker.
     */
    public final LatencyMarker asLatencyMarker() {
        return (LatencyMarker) this;
    }

    /**
     * Casts this element into a RecordAttributes.
     *
     * @return This element as a RecordAttributes.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a
     *     RecordAttributes.
     */
    public final RecordAttributes asRecordAttributes() {
        return (RecordAttributes) this;
    }
}
