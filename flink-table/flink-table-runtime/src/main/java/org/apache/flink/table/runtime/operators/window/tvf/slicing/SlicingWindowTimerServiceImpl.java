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

package org.apache.flink.table.runtime.operators.window.tvf.slicing;

import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerServiceBase;
import org.apache.flink.table.runtime.util.TimeWindowUtil;

import java.time.ZoneId;

/** A {@link WindowTimerService} for slicing window. */
// SlicingWindowTimerServiceImpl 是 Flink Table API 中用于实现切片窗口（Slicing Window） 时间管理和定时器服务的具体实现类。
// 它继承自抽象基类 WindowTimerServiceBase<Long>，并利用 Flink 内部的 InternalTimerService 来实现其功能。
// 核心作用： 为 Slicing 窗口提供时区感知的机制来注册和触发处理时间/事件时间定时器。
// 泛型参数 Long 表示窗口命名空间 (Window Namespace) 的类型。
// 在 Slicing 窗口的上下文中，这个 Long 通常代表窗口的结束时间戳（Epoch Milliseconds）
public class SlicingWindowTimerServiceImpl extends WindowTimerServiceBase<Long> {

    public SlicingWindowTimerServiceImpl(
            InternalTimerService<Long> internalTimerService, ZoneId shiftTimeZone) {
        super(internalTimerService, shiftTimeZone);
    }
    // 注册处理时间窗口定时器。
    // window 参数是窗口的结束时间戳。
    // 调用底层 Flink 定时器服务注册定时器。
    // 1. window：作为命名空间传入，当定时器触发时，它将被作为 window 命名空间返回。
    // 2. 第二个参数（触发时间）：通过 TimeWindowUtil.toEpochMillsForTimer(window - 1, shiftTimeZone) 计算。
    // Slicing 窗口通常在窗口结束时间前 1 毫秒触发（window - 1），以确保所有属于该窗口的数据（时间戳 $\le$ 窗口结束时间）都已被处理。
    // 此外，计算中考虑了 shiftTimeZone，以确保定时器触发时间与用户的时区配置一致。
    @Override
    public void registerProcessingTimeWindowTimer(Long window) {
        internalTimerService.registerProcessingTimeTimer(
                window, TimeWindowUtil.toEpochMillsForTimer(window - 1, shiftTimeZone));
    }

    @Override
    public void registerEventTimeWindowTimer(Long window) {
        internalTimerService.registerEventTimeTimer(
                window, TimeWindowUtil.toEpochMillsForTimer(window - 1, shiftTimeZone));
    }
}
