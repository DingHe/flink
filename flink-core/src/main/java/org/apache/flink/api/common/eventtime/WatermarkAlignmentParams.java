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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
//用于配置水印对齐功能的一个不可变（final）参数类。它将水印对齐所需的所有配置项封装在一起，以便在运行时传递和使用
/** Configuration parameters for watermark alignment. */
@PublicEvolving
public final class WatermarkAlignmentParams implements Serializable {
    public static final WatermarkAlignmentParams WATERMARK_ALIGNMENT_DISABLED =
            new WatermarkAlignmentParams(Long.MAX_VALUE, "", 0);
    //最大允许漂移：允许水印之间存在多大的时间差
    private final long maxAllowedWatermarkDrift;
    //表示协调器更新水印对齐状态的频率，以毫秒为单位
    private final long updateInterval;
    //用于将多个数据源或分区划分到同一个水印对齐组中。只有在同一个组内的水印才会进行对齐
    private final String watermarkGroup;

    public WatermarkAlignmentParams(
            long maxAllowedWatermarkDrift, String watermarkGroup, long updateInterval) {
        this.maxAllowedWatermarkDrift = maxAllowedWatermarkDrift;
        this.watermarkGroup = watermarkGroup;
        this.updateInterval = updateInterval;
    }

    public boolean isEnabled() {
        return maxAllowedWatermarkDrift < Long.MAX_VALUE;
    }

    public long getMaxAllowedWatermarkDrift() {
        return maxAllowedWatermarkDrift;
    }

    public String getWatermarkGroup() {
        return watermarkGroup;
    }

    public long getUpdateInterval() {
        return updateInterval;
    }
}
