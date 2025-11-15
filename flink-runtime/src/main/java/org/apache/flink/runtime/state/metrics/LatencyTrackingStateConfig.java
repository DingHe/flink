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

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateLatencyTrackOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Config to create latency tracking state metric. */

// 主要作用是封装和管理 Flink 状态后端（State Backend）进行状态操作延迟跟踪所需的所有配置参数和运行时依赖。
// 当 Flink 启用状态延迟监控功能时，它会在状态的读写操作中插入逻辑来测量耗时，并将这些延迟指标报告给 Flink 的指标系统。这个配置类就是为了：
// 保存配置： 存储是否启用延迟跟踪、采样频率、历史记录大小等核心配置项。
// 提供指标组： 存储用于注册延迟指标的 MetricGroup 实例。
// 支持构建者模式： 提供一个 Builder 类，方便从 Flink 的配置 (ReadableConfig) 中读取参数并创建配置对象。

@Internal
public class LatencyTrackingStateConfig {
    // 指标组。
    // 这是用于将状态延迟指标（如平均延迟、p95 延迟等）注册到 Flink 指标系统的组。
    private final MetricGroup metricGroup;
    // 启用标志。
    // 控制状态延迟跟踪功能是否开启。
    // 如果为 true，则状态后端将在读写状态时进行计时和采样。
    private final boolean enabled;
    // 采样间隔。
    // 指定每进行多少次状态操作（读或写）才进行一次延迟采样。
    // 例如，如果为 10，则每 10 次操作中只会测量一次延迟，以减少对作业性能的影响。
    private final int sampleInterval;
    // 历史记录大小。
    // 用于配置延迟直方图（Histogram）或类似结构中保留的延迟历史记录的数量。
    // 这决定了计算 p50/p95 等百分位延迟指标时的数据粒度或窗口大小。
    private final int historySize;
    // 状态名作为指标变量。
    // 如果为 true，则状态的名称会被作为指标标签（Tag/Variable）的一部分，
    // 这意味着每个独立的状态（ValueState、ListState 等）都会报告单独的延迟指标。
    private final boolean stateNameAsVariable;

    LatencyTrackingStateConfig(
            MetricGroup metricGroup,
            boolean enabled,
            int sampleInterval,
            int historySize,
            boolean stateNameAsVariable) {
        if (enabled) {
            Preconditions.checkNotNull(
                    metricGroup, "Metric group cannot be null if latency tracking is enabled.");
            Preconditions.checkArgument(sampleInterval >= 1);
        }
        this.metricGroup = metricGroup;
        this.enabled = enabled;
        this.sampleInterval = sampleInterval;
        this.historySize = historySize;
        this.stateNameAsVariable = stateNameAsVariable;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public int getHistorySize() {
        return historySize;
    }

    public int getSampleInterval() {
        return sampleInterval;
    }

    public boolean isStateNameAsVariable() {
        return stateNameAsVariable;
    }

    public static LatencyTrackingStateConfig disabled() {
        return newBuilder().setEnabled(false).build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements Serializable {
        private static final long serialVersionUID = 1L;

        private boolean enabled = StateLatencyTrackOptions.LATENCY_TRACK_ENABLED.defaultValue();
        private int sampleInterval =
                StateLatencyTrackOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue();
        private int historySize =
                StateLatencyTrackOptions.LATENCY_TRACK_HISTORY_SIZE.defaultValue();
        private boolean stateNameAsVariable =
                StateLatencyTrackOptions.LATENCY_TRACK_STATE_NAME_AS_VARIABLE.defaultValue();
        private MetricGroup metricGroup;

        public Builder setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder setSampleInterval(int sampleInterval) {
            this.sampleInterval = sampleInterval;
            return this;
        }

        public Builder setHistorySize(int historySize) {
            this.historySize = historySize;
            return this;
        }

        public Builder setStateNameAsVariable(boolean stateNameAsVariable) {
            this.stateNameAsVariable = stateNameAsVariable;
            return this;
        }

        public Builder setMetricGroup(MetricGroup metricGroup) {
            this.metricGroup = metricGroup;
            return this;
        }

        public Builder configure(ReadableConfig config) {
            this.setEnabled(config.get(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED))
                    .setSampleInterval(
                            config.get(StateLatencyTrackOptions.LATENCY_TRACK_SAMPLE_INTERVAL))
                    .setHistorySize(config.get(StateLatencyTrackOptions.LATENCY_TRACK_HISTORY_SIZE))
                    .setStateNameAsVariable(
                            config.get(
                                    StateLatencyTrackOptions.LATENCY_TRACK_STATE_NAME_AS_VARIABLE));
            return this;
        }

        public LatencyTrackingStateConfig build() {
            return new LatencyTrackingStateConfig(
                    metricGroup, enabled, sampleInterval, historySize, stateNameAsVariable);
        }
    }
}
