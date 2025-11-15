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

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CombinedWatermarkStatus} combines the watermark (and idleness) updates of multiple
 * partitions/shards/splits into one combined watermark.
 */
// CombinedWatermarkStatus 类在 Flink 事件时间处理中扮演着多路复用（Multiplexing）和同步协调的角色。
// 组合 Watermark (Minimum Logic): 在一个算子（通常是 StreamTask 或 Operator，
// 例如 TwoInputStreamOperator 双输入算子）接收来自多个输入源、多个分区或多个内部逻辑分片的 Watermark 时，
// 它负责根据 Flink 的 Watermark 语义计算出组合 Watermark。
// 组合 Watermark 总是所有非空闲（non-idle）分片 Watermark 中的最小值。
// 管理空闲状态 (Idleness): 它跟踪所有内部输入分片的空闲状态。只有当所有分片都处于空闲状态时，整个组合 Watermark 状态才被标记为空闲。
// 驱动 Watermark 前进： 通过定期调用 updateCombinedWatermark()，它驱动组合 Watermark 前进，并决定是否应该向更下游的算子发出新的 Watermark 消息。
// 确保了 Flink 的事件时间进展总是受限于最慢的那个非空闲输入分片。

@Internal
final class CombinedWatermarkStatus {

    /** List of all watermark outputs, for efficient access. */
    // 存储所有参与组合计算的 Watermark 分片状态（即每个输入源或内部逻辑分片的状态）
    private final List<PartialWatermark> partialWatermarks = new ArrayList<>();

    /** The combined watermark over the per-output watermarks. */
    // 组合 Watermark 的值。
    // 它是所有非空闲 PartialWatermark 的当前 Watermark 值中的最小值。初始值为 Long.MIN_VALUE
    private long combinedWatermark = Long.MIN_VALUE;
    // 组合空闲状态。
    // 表示整个组合 Watermark 状态是否处于空闲。仅当所有 partialWatermarks 都处于空闲状态时，此值为 true。
    private boolean idle = false;

    public long getCombinedWatermark() {
        return combinedWatermark;
    }

    public boolean isIdle() {
        return idle;
    }

    public boolean remove(PartialWatermark o) {
        return partialWatermarks.remove(o);
    }

    public void add(PartialWatermark element) {
        partialWatermarks.add(element);
    }

    /**
     * Checks whether we need to update the combined watermark.
     *
     * <p><b>NOTE:</b>It can update {@link #isIdle()} status.
     *
     * @return true, if the combined watermark changed
     */
    public boolean updateCombinedWatermark() {
        long minimumOverAllOutputs = Long.MAX_VALUE;

        // if we don't have any outputs minimumOverAllOutputs is not valid, it's still
        // at its initial Long.MAX_VALUE state and we must not emit that
        if (partialWatermarks.isEmpty()) {
            return false;
        }

        boolean allIdle = true;
        for (PartialWatermark partialWatermark : partialWatermarks) {
            if (!partialWatermark.isIdle()) {
                minimumOverAllOutputs =
                        Math.min(minimumOverAllOutputs, partialWatermark.getWatermark());
                allIdle = false;
            }
        }

        this.idle = allIdle;

        if (!allIdle && minimumOverAllOutputs > combinedWatermark) {
            combinedWatermark = minimumOverAllOutputs;
            return true;
        }

        return false;
    }

    /** Per-output watermark state. */
    // 代表一个单一的输入源、分片或输出的水位线状态
    static class PartialWatermark {
        // 当前分片的 Watermark 值。
        // 初始值为 Long.MIN_VALUE。
        private long watermark = Long.MIN_VALUE;
        // 当前分片的空闲状态。
        // true 表示该分片当前没有数据流经。
        private boolean idle = false;
        // 水位线更新监听器。
        // 一个回调函数，用于在 Watermark 发生变化时通知外部组件（如 CombinedWatermarkStatus 或 WatermarkOutputMultiplexer）
        private final WatermarkOutputMultiplexer.WatermarkUpdateListener onWatermarkUpdate;

        public PartialWatermark(
                WatermarkOutputMultiplexer.WatermarkUpdateListener onWatermarkUpdate) {
            this.onWatermarkUpdate = onWatermarkUpdate;
        }

        /**
         * Returns the current watermark timestamp. This will throw {@link IllegalStateException} if
         * the output is currently idle.
         */
        private long getWatermark() {
            checkState(!idle, "Output is idle.");
            return watermark;
        }

        /**
         * Returns true if the watermark was advanced, that is if the new watermark is larger than
         * the previous one.
         *
         * <p>Setting a watermark will clear the idleness flag.
         */
        public boolean setWatermark(long watermark) {
            this.idle = false;
            final boolean updated = watermark > this.watermark;
            if (updated) {
                this.onWatermarkUpdate.onWatermarkUpdate(watermark);
                this.watermark = Math.max(watermark, this.watermark);
            }
            return updated;
        }

        private boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
        }
    }
}
