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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A WatermarkGenerator for situations where records are out of order, but you can place an upper
 * bound on how far the events are out of order. An out-of-order bound B means that once an event
 * with timestamp T was encountered, no events older than {@code T - B} will follow any more.
 *
 * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy is
 * the periodic interval length, plus the out-of-orderness bound.
 * 提供的一个 WatermarkGenerator 的具体实现类，它的作用是专门用于处理**有界乱序（Bounded Out-of-Orderness）**的数据流
 * 在实际的流处理场景中，数据到达的顺序往往不是完全按时间戳递增的，可能会有部分乱序。然而，在许多情况下，我们可以假定乱序的程度有一个上限。例如，我们知道任何事件最多会迟到 5 秒
 * //工作原理是
 * //跟踪最大时间戳：每当一个新事件到达时，它会记录到目前为止所见过的最大时间戳（maxTimestamp）
 * //周期性生成水印：它会周期性地生成水印。生成的水印时间戳是 maxTimestamp - maxOutOfOrderness - 1
 */
@Public
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    /** The maximum timestamp encountered so far. */
    //用于存储到目前为止在流中见到的最大事件时间戳
    private long maxTimestamp;

    /** The maximum out-of-orderness that this watermark generator assumes. */
    //表示允许的最大乱序时间，以毫秒为单位
    private final long outOfOrdernessMillis;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ------------------------------------------------------------------------
    //每当一个事件到达时被调用
    //比较当前事件的时间戳 eventTimestamp 和 maxTimestamp，并用两者中的较大值来更新 maxTimestamp。它不会立即发出水印
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }
    //以固定的时间间隔被调用，用于生成水印
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
