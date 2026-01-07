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

/**
 * A {@code TimestampAssigner} assigns event time timestamps to elements. These timestamps are used
 * by all functions that operate on event time, for example event time windows.
 *
 * <p>Timestamps can be an arbitrary {@code long} value, but all built-in implementations represent
 * it as the milliseconds since the Epoch (midnight, January 1, 1970 UTC), the same way as {@link
 * System#currentTimeMillis()} does it.
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 */
// 在 Flink 中，如果你使用的是“处理时间”（Processing Time），Flink 会直接取机器的系统时间。但如果你使用的是“事件时间”（Event Time），Flink 本身并不知道数据里的哪个字段代表时间。
// TimestampAssigner 的职责主要有两点：
// 提取时间戳（Extraction）：从输入的元素中提取出一个 long 类型的毫秒值（Epoch Timestamp）。
// 这个值通常来自于业务数据中的某个字段（如 create_time 或 ts）。
// 分配时间戳（Assignment）：将提取出的时间戳附加到该元素的元数据中。这样，后续的算子（如窗口算子 Window）就能通过这个元数据知道如何对数据进行归类。
// 为流中的数据元素分配事件时间时间戳的核心接口
// 作用就是从数据元素中提取或生成这个时间戳
// TimestampAssigner 通常作为 WatermarkStrategy（水位线策略） 的一部分出现。一个完整的时间处理策略由两部分组成：
// TimestampAssigner：负责找时间。
// WatermarkGenerator：负责根据找回来的时间发信号（水位线）。

// TimestampAssigner 的工作流程
// 输入记录：算子接收到一条原始数据 T。
// 调用方法：框架调用 extractTimestamp(T element, long recordTimestamp)。
// 返回结果：方法返回该记录真实的毫秒时间戳。
// 内部打标：Flink 将这个返回的值存入 StreamRecord 的内部属性中，后续的所有时间计算（如 window.end()）都将以这个值为准。

@Public
@FunctionalInterface
public interface TimestampAssigner<T> {

    /**
     * The value that is passed to {@link #extractTimestamp} when there is no previous timestamp
     * attached to the record.
     */
    // 代表一个记录没有被分配过时间戳的状态
    long NO_TIMESTAMP = Long.MIN_VALUE;

    /**
     * Assigns a timestamp to an element, in milliseconds since the Epoch. This is independent of
     * any particular time zone or calendar.
     *
     * <p>The method is passed the previously assigned timestamp of the element. That previous
     * timestamp may have been assigned from a previous assigner. If the element did not carry a
     * timestamp before, this value is {@link #NO_TIMESTAMP} (= {@code Long.MIN_VALUE}: {@value
     * Long#MIN_VALUE}).
     *
     * @param element The element that the timestamp will be assigned to.
     * @param recordTimestamp The current internal timestamp of the element, or a negative value, if
     *     no timestamp has been assigned yet.
     * @return The new timestamp.
     */
    // 用于提取或分配时间戳
    // T element：要分配时间戳的数据元素
    // long recordTimestamp：该元素之前已经分配的时间戳。如果这是第一次分配，它的值为 NO_TIMESTAMP
    long extractTimestamp(T element, long recordTimestamp);
}
