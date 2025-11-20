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

package org.apache.flink.table.runtime.operators.window.tvf.common;

import org.apache.flink.annotation.Internal;

import java.time.ZoneId;

/**
 * Interface for working with window time and timers which considers timezone for window splitting.
 *
 * @param <W> Type of the window namespace to which timers are scoped.
 */
// WindowTimerService<W> 接口定义了在 Flink Table/SQL 窗口函数（TVF - Table Valued Function）操作符内部，管理时间和定时器所需的核心功能。
// 它的关键在于要**考虑时区（Timezone）**对窗口划分的影响。
// 泛型参数 <W> 代表窗口命名空间 (Window Namespace) 的类型。在 Flink 中，一个命名空间通常用于将定时器（Timers）和状态限定到特定的窗口实例（例如一个 TimeWindow 对象）
@Internal
public interface WindowTimerService<W> {

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     */
    // 获取时区偏移量。
    // 返回用于窗口时间计算的 ZoneId。
    // 如果处理时间（proctime）或行时间（rowtime）的数据类型是 TIMESTAMP_LTZ（带本地时区的 Timestamp），
    // 则返回用户在 TableConfig 中配置的时区；否则，返回 UTC。时区偏移对于处理基于日历时间的窗口（如天、月、年窗口）至关重要。
    ZoneId getShiftTimeZone();

    /** Returns the current processing time. */
    // 返回当前处理时间
    // 返回当前操作符实例所在的 Task Manager 的系统时间（以毫秒为单位）。这是基于机器时钟的时间概念。
    long currentProcessingTime();

    /** Returns the current event-time watermark. */
    // 返回当前水位线。
    // 返回当前操作符接收到的最新的事件时间水位线（Event-Time Watermark，以毫秒为单位）。水位线是 Flink 衡量事件时间进度的机制。
    long currentWatermark();

    /**
     * Registers a window timer to be fired when processing time passes the window. The window you
     * pass here will be provided when the timer fires.
     */
    // 注册一个定时器，
    // 当处理时间超过与给定 window 命名空间相关联的时间点时，该定时器将被触发。
    // 通常用于实现处理时间窗口的关闭和发射。
    void registerProcessingTimeWindowTimer(W window);

    /**
     * Registers a window timer to be fired when event time watermark passes the window. The window
     * you pass here will be provided when the timer fires.
     */
    // 注册一个定时器，当事件时间水位线超过与给定 window 命名空间相关联的时间点时，该定时器将被触发。
    // 这是实现事件时间窗口的主要机制，确保只有当 Flink 确认不再有迟到数据到达时才关闭窗口。
    void registerEventTimeWindowTimer(W window);
}
