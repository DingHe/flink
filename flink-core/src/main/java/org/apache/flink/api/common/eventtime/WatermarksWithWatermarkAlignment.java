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

import org.apache.flink.annotation.Internal;

import java.time.Duration;
//作用是作为一个装饰器（Decorator），来封装一个现有的 WatermarkStrategy，并为其添加水印对齐（Watermark Alignment）的配置
/** A helper class to pass a watermark group and max allowed watermark drift to the runtime. */
@Internal
final class WatermarksWithWatermarkAlignment<T> implements WatermarkStrategy<T> {
    //定义了默认的水印对齐状态更新间隔，即 1000 毫秒（1秒）
    static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofMillis(1000);
    //被包装的原始水印策略。这个对象包含了实际的时间戳分配和水印生成逻辑
    private final WatermarkStrategy<T> strategy;
    //水印对齐组的名称
    private final String watermarkGroup;
    //最大允许的水印漂移时间
    private final Duration maxAllowedWatermarkDrift;
    //水印对齐状态的更新间隔
    private final Duration updateInterval;

    public WatermarksWithWatermarkAlignment(
            WatermarkStrategy<T> strategy,
            String watermarkGroup,
            Duration maxAllowedWatermarkDrift,
            Duration updateInterval) {
        this.strategy = strategy;
        this.watermarkGroup = watermarkGroup;
        this.maxAllowedWatermarkDrift = maxAllowedWatermarkDrift;
        this.updateInterval = updateInterval;
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return strategy.createTimestampAssigner(context);
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(
            WatermarkGeneratorSupplier.Context context) {
        return strategy.createWatermarkGenerator(context);
    }

    @Override
    public WatermarkAlignmentParams getAlignmentParameters() {
        return new WatermarkAlignmentParams(
                maxAllowedWatermarkDrift.toMillis(), watermarkGroup, updateInterval.toMillis());
    }
}
