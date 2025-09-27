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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.ThreadLocalRandom;
// 用于错开（staggering）窗口偏移量的一个枚举类型。它的主要作用是在 Flink 的并行任务中，
// 让每个并行的窗口算子（Window Operator）实例的窗口起始点不完全对齐。
// 这种设计可以有效分散在每个窗口边界处可能出现的高峰计算负载，
// 避免在同一时刻所有并行实例都进行窗口计算，从而提高系统的吞吐量和稳定性
/** A {@code WindowStagger} staggers offset in runtime for each window assignment. */
@PublicEvolving
public enum WindowStagger {
    /** Default mode, all panes fire at the same time across all partitions. */
    //对齐模式。这是默认模式，不进行任何错开
    ALIGNED {
        @Override
        public long getStaggerOffset(final long currentProcessingTime, final long size) {
            return 0L;
        }
    },

    /**
     * Stagger offset is sampled from uniform distribution U(0, WindowSize) when first event
     * ingested in the partitioned operator.
     */
    //随机模式。在每个窗口算子实例第一次处理事件时，会从一个均匀分布的随机区间 U(0, WindowSize) 中随机选择一个偏移量
    RANDOM {
        @Override
        public long getStaggerOffset(final long currentProcessingTime, final long size) {
            return (long) (ThreadLocalRandom.current().nextDouble() * size);
        }
    },

    /**
     * When the first event is received in the window operator, take the difference between the
     * start of the window and current procesing time as the offset. This way, windows are staggered
     * based on when each parallel operator receives the first event.
     */
    //自然模式。这种模式的偏移量是根据窗口算子实例第一次接收到事件时的处理时间来确定的
    NATURAL {
        @Override
        public long getStaggerOffset(final long currentProcessingTime, final long size) {
            final long currentProcessingWindowStart =
                    TimeWindow.getWindowStartWithOffset(currentProcessingTime, 0, size);
            return Math.max(0, currentProcessingTime - currentProcessingWindowStart);
        }
    };

    public abstract long getStaggerOffset(final long currentProcessingTime, final long size);
}
