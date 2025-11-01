/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A collection of simple metrics, around the triggering of a checkpoint. */
// 核心作用是收集和封装 Flink **任务子任务（Task Subtask）**启动或从检查点/保存点恢复时，初始化过程的性能指标
// 初始化阶段是 Flink 任务生命周期中一个重要的、可能耗时的阶段，尤其是在从大规模状态恢复时。这些指标对于理解以下方面至关重要：
// 恢复性能分析： 衡量从状态后端加载状态、恢复通道状态和执行用户自定义初始化逻辑所花费的时间。
    // 故障排查： 如果任务启动慢，这些指标能帮助定位是哪一个初始化步骤（例如，从远程存储下载状态文件）耗时过长。
public class SubTaskInitializationMetrics implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final long UNSET = -1L;

    /**
     * WARNING! When adding new fields make sure that the math to calculate various durations in
     * this class's getters is still correct.
     */
    // 初始化开始时间戳（Start Timestamp）。 子任务开始其初始化过程的绝对时间，通常是自 epoch 以来的毫秒数
    private final long startTs;
    // 初始化结束时间戳（End Timestamp）。 子任务完成其初始化过程的绝对时间。
    private final long endTs;
    // 持续时间指标映射。 一个映射表，存储了初始化阶段中各个子步骤的持续时间（例如："state_restore_time"）。键是指标名称，值是该子步骤花费的毫秒数。
    private final Map<String, Long> durationMetrics;
    // 初始化状态。 一个枚举值，表示子任务初始化是否成功或以何种方式结束。
    private final InitializationStatus status;

    public SubTaskInitializationMetrics(
            long startTs,
            long endTs,
            Map<String, Long> durationMetrics,
            InitializationStatus status) {
        checkArgument(startTs >= 0);
        checkArgument(endTs >= startTs);
        this.startTs = startTs;
        this.endTs = endTs;
        this.durationMetrics = durationMetrics;
        this.status = status;
    }

    public long getStartTs() {
        return startTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public Map<String, Long> getDurationMetrics() {
        return durationMetrics;
    }

    public InitializationStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubTaskInitializationMetrics that = (SubTaskInitializationMetrics) o;

        return startTs == that.startTs
                && endTs == that.endTs
                && Objects.equals(durationMetrics, that.durationMetrics)
                && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTs, endTs, durationMetrics, status);
    }

    @Override
    public String toString() {
        return SubTaskInitializationMetrics.class.getSimpleName()
                + "{"
                + "initializationStartTs="
                + startTs
                + "initializationEndTs="
                + endTs
                + "durationMetrics="
                + durationMetrics
                + "status="
                + status
                + '}';
    }
}
