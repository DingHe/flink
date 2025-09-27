/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;

/**
 * The {@code WatermarkGenerator} generates watermarks either based on events or periodically (in a
 * fixed interval).
 *
 * <p><b>Note:</b> This WatermarkGenerator subsumes the previous distinction between the {@code
 * AssignerWithPunctuatedWatermarks} and the {@code AssignerWithPeriodicWatermarks}.
 */
//生成事件时间水印的核心接口。水印是 Flink 处理乱序事件和推进事件时间时钟的关键机制。
// 它是一个逻辑时钟，用于告诉 Flink 运行时，到某个时间戳为止的所有事件都已经到达，后续的事件都将被视为迟到（late）
@Public
public interface WatermarkGenerator<T> {

    /**
     * Called for every event, allows the watermark generator to examine and remember the event
     * timestamps, or to emit a watermark based on the event itself.
     */
    //T event：当前到达的数据元素
    //long eventTimestamp：由 TimestampAssigner 为该事件分配的时间戳
    //WatermarkOutput output：一个接口，用于将生成的水印发送到下游
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /**
     * Called periodically, and might emit a new watermark, or not.
     *
     * <p>The interval in which this method is called and Watermarks are generated depends on {@link
     * ExecutionConfig#getAutoWatermarkInterval()}.
     */
    //以固定时间间隔周期性地被调用，用于生成**周期性（periodic）**的水印
    //一个简单的实现是记住目前看到的最高时间戳，然后减去一个固定的延迟（Lateness），作为新的水印值
    void onPeriodicEmit(WatermarkOutput output);
}
