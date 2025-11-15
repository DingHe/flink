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

import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Represents combined value and status of a watermark for a set number of input partial watermarks.
 */
// IndexedCombinedWatermarkStatus 类是 Flink 中用于处理多输入 Watermark 组合逻辑的工具类，特别适用于 TwoInputStreamOperator（双输入算子）或具有多个输入通道的场景。
// 基于索引的 Watermark 更新： 它将底层的 CombinedWatermarkStatus 包装起来，允许调用者通过一个索引 (index) 来标识和更新特定的输入分片（Partial Watermark）的水位线值或空闲状态。
// 维护最小 Watermark： 它确保了算子发出的组合 Watermark 是其所有输入分片 Watermark 值中的最小值（并忽略处于空闲状态的分片）
// 简化多路复用逻辑： 它将底层复杂的分片管理和最小值计算委托给 CombinedWatermarkStatus，对外提供了一个简单、基于索引的接口。

@Internal
public final class IndexedCombinedWatermarkStatus {
    // 核心组合逻辑。这是实际负责执行 Watermark 最小值计算和管理全局空闲状态的底层对象
    private final CombinedWatermarkStatus combinedWatermarkStatus;
    // 索引化的分片状态数组。
    // 这是一个数组，其索引（例如 0 和 1）对应于 Flink 算子的输入通道（Input 1 和 Input 2）。数组中的每个元素代表一个输入分片的 Watermark 状态。
    private final CombinedWatermarkStatus.PartialWatermark[] partialWatermarks;

    private IndexedCombinedWatermarkStatus(
            CombinedWatermarkStatus combinedWatermarkStatus,
            CombinedWatermarkStatus.PartialWatermark[] partialWatermarks) {
        this.combinedWatermarkStatus = combinedWatermarkStatus;
        this.partialWatermarks = partialWatermarks;
    }
    // 根据指定的输入数量 (inputsCount) 创建并初始化 IndexedCombinedWatermarkStatus 实例。
    //它会创建相应数量的 PartialWatermark 实例，并将它们添加到 CombinedWatermarkStatus 中，最终返回一个可以按索引访问的组合状态对象。
    public static IndexedCombinedWatermarkStatus forInputsCount(int inputsCount) {
        CombinedWatermarkStatus.PartialWatermark[] partialWatermarks =
                IntStream.range(0, inputsCount)
                        .mapToObj(
                                i -> new CombinedWatermarkStatus.PartialWatermark(watermark -> {}))
                        .toArray(CombinedWatermarkStatus.PartialWatermark[]::new);
        CombinedWatermarkStatus combinedWatermarkStatus = new CombinedWatermarkStatus();
        for (CombinedWatermarkStatus.PartialWatermark partialWatermark : partialWatermarks) {
            combinedWatermarkStatus.add(partialWatermark);
        }
        return new IndexedCombinedWatermarkStatus(combinedWatermarkStatus, partialWatermarks);
    }

    /**
     * Updates the value for the given partial watermark. Can update both the global idleness as
     * well as the combined watermark value.
     *
     * @return true, if the combined watermark value changed. The global idleness needs to be
     *     checked separately via {@link #isIdle()}
     */
    // 更新 Watermark 值。
    // 接收一个输入索引 (index) 和一个新的 Watermark 时间戳 (timestamp)。
    public boolean updateWatermark(int index, long timestamp) {
        checkArgument(index < partialWatermarks.length);
        partialWatermarks[index].setWatermark(timestamp);
        return combinedWatermarkStatus.updateCombinedWatermark();
    }

    public long getCombinedWatermark() {
        return combinedWatermarkStatus.getCombinedWatermark();
    }

    /**
     * Updates the idleness for the given partial watermark. Can update both the global idleness as
     * well as the combined watermark value.
     *
     * @return true, if the combined watermark value changed. The global idleness needs to be
     *     checked separately via {@link #isIdle()}
     */
    // 更新空闲状态。
    // 接收一个输入索引 (index) 和一个新的空闲状态 (idle)
    public boolean updateStatus(int index, boolean idle) {
        checkArgument(index < partialWatermarks.length);
        partialWatermarks[index].setIdle(idle);
        return combinedWatermarkStatus.updateCombinedWatermark();
    }

    public boolean isIdle() {
        return combinedWatermarkStatus.isIdle();
    }
}
