/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/** Describe the input selection that stream operators want to read records. */
// InputSelection 类是 Flink 内部用于描述和管理一个流式算子（Stream Operator）希望从其输入流中读取哪些数据的机制
// InputSelection 的核心作用是解决 Flink 多输入算子（如 TwoInputStreamOperator 或 N-ary Input Operator）在运行时需要决定接下来从哪条输入流中拉取数据的问题。
// 在 Flink 运行时，特别是在流处理任务的主循环中，算子通常会循环检查其输入队列并拉取数据。对于多输入算子，如果所有输入流都有可用数据，就必须选择一个输入。InputSelection 通过使用**位掩码（Bitmask）**来高效地表示这种选择策略：
@PublicEvolving
public final class InputSelection implements Serializable {
    // 常量，值为 -1。
    // 表示在尝试选择下一个输入时，没有可供读取的输入流。
    public static final int NONE_AVAILABLE = -1;

    private static final long serialVersionUID = 1L;

    /** The {@code InputSelection} instance which indicates to select all inputs. */
    // 预定义实例，表示选择所有输入。
    // 内部 inputMask 为 -1L（long 类型所有位都为 1）
    public static final InputSelection ALL = new InputSelection(-1);

    /** The {@code InputSelection} instance which indicates to select the first input. */
    // 预定义实例，表示只选择第一个输入。 内部 inputMask 为 1L（二进制 ...0001）
    public static final InputSelection FIRST = new Builder().select(1).build();

    /** The {@code InputSelection} instance which indicates to select the second input. */
    // 预定义实例，表示只选择第二个输入。 内部 inputMask 为 2L（二进制 ...0010）
    public static final InputSelection SECOND = new Builder().select(2).build();
    // 输入选择的位掩码。
    // 核心属性。
    // long 类型支持最多 64 个输入。其中，第 $i$ 位（从 0 开始）为 1 表示选择了第 $i+1$ 个输入。特殊值 -1L 表示选择了所有输入。
    private final long inputMask;

    /** @param inputMask -1 to mark if all inputs are selected. */
    private InputSelection(long inputMask) {
        this.inputMask = inputMask;
    }
    // 返回内部存储的 inputMask 位掩码。
    public long getInputMask() {
        return inputMask;
    }

    /**
     * Tests if the input specified by {@code inputId} is selected.
     *
     * @param inputId The input id, see the description of {@code inputId} in {@link
     *     Builder#select(int)}.
     * @return {@code true} if the input is selected, {@code false} otherwise.
     */
    // 检查特定输入是否被选择
    public boolean isInputSelected(int inputId) {
        return (inputMask & (1L << (inputId - 1))) != 0;
    }

    /**
     * Tests if all inputs are selected.
     *
     * @return {@code true} if the input mask equals -1, {@code false} otherwise.
     */
    // 检查 inputMask 是否等于 -1L，
    // 用于判断是否选择了所有可能的输入。
    public boolean areAllInputsSelected() {
        return inputMask == -1L;
    }

    /**
     * Fairly select one of the two inputs for reading. When {@code inputMask} includes two inputs
     * and both inputs are available, alternately select one of them. Otherwise, select the
     * available one of {@code inputMask}, or return {@link InputSelection#NONE_AVAILABLE} to
     * indicate no input is selected.
     *
     * <p>Note that this supports only two inputs for performance reasons.
     *
     * @param availableInputsMask The mask of all available inputs.
     * @param lastReadInputIndex The index of last read input.
     * @return the index of the input for reading or {@link InputSelection#NONE_AVAILABLE} (if
     *     {@code inputMask} is empty or the inputs in {@code inputMask} are unavailable).
     */
    // （双输入优化）公平选择下一个输入索引
    public int fairSelectNextIndexOutOf2(int availableInputsMask, int lastReadInputIndex) {
        return fairSelectNextIndexOutOf2((int) inputMask, availableInputsMask, lastReadInputIndex);
    }

    /**
     * Fairly select one of the two inputs for reading. When {@code inputMask} includes two inputs
     * and both inputs are available, alternately select one of them. Otherwise, select the
     * available one of {@code inputMask}, or return {@link InputSelection#NONE_AVAILABLE} to
     * indicate no input is selected.
     *
     * <p>Note that this supports only two inputs for performance reasons.
     *
     * @param selectionMask The mask of inputs that are selected. Note -1 for this is interpreted as
     *     all of the 32 inputs are available.
     * @param availableInputsMask The mask of all available inputs.
     * @param lastReadInputIndex The index of last read input.
     * @return the index of the input for reading or {@link InputSelection#NONE_AVAILABLE} (if
     *     {@code inputMask} is empty or the inputs in {@code inputMask} are unavailable).
     */
    public static int fairSelectNextIndexOutOf2(
            int selectionMask, int availableInputsMask, int lastReadInputIndex) {
        int combineMask = availableInputsMask & selectionMask;

        if (combineMask == 3) {
            return lastReadInputIndex == 0 ? 1 : 0;
        } else if (combineMask >= 0 && combineMask < 3) {
            return combineMask - 1;
        }

        throw new UnsupportedOperationException("Only two inputs are supported.");
    }

    /**
     * Fairly select one of the available inputs for reading.
     *
     * @param availableInputsMask The mask of all available inputs. Note -1 for this is interpreted
     *     as all of the 32 inputs are available.
     * @param lastReadInputIndex The index of last read input.
     * @return the index of the input for reading or {@link InputSelection#NONE_AVAILABLE} (if
     *     {@code inputMask} is empty or the inputs in {@code inputMask} are unavailable).
     */
    // （多输入通用）公平选择下一个输入索引。 适用于多于两个输入的情况。
    // 它结合了选择掩码 (inputMask) 和可用掩码 (availableInputsMask)，从上一次读取的输入之后开始，
    // 循环查找并返回下一个被选择且可用的输入索引。
    public int fairSelectNextIndex(long availableInputsMask, int lastReadInputIndex) {
        return fairSelectNextIndex(inputMask, availableInputsMask, lastReadInputIndex);
    }

    /**
     * Fairly select one of the available inputs for reading.
     *
     * @param inputMask The mask of inputs that are selected. Note -1 for this is interpreted as all
     *     of the 32 inputs are available.
     * @param availableInputsMask The mask of all available inputs. Note -1 for this is interpreted
     *     as all of the 32 inputs are available.
     * @param lastReadInputIndex The index of last read input.
     * @return the index of the input for reading or {@link InputSelection#NONE_AVAILABLE} (if
     *     {@code inputMask} is empty or the inputs in {@code inputMask} are unavailable).
     */
    public static int fairSelectNextIndex(
            long inputMask, long availableInputsMask, int lastReadInputIndex) {
        long combineMask = availableInputsMask & inputMask;

        if (combineMask == 0) {
            return NONE_AVAILABLE;
        }

        int nextReadInputIndex = selectFirstBitRightFromNext(combineMask, lastReadInputIndex + 1);
        if (nextReadInputIndex >= 0) {
            return nextReadInputIndex;
        }
        return selectFirstBitRightFromNext(combineMask, 0);
    }

    private static int selectFirstBitRightFromNext(long bits, int next) {
        if (next >= 64) {
            return NONE_AVAILABLE;
        }
        for (bits >>>= next; bits != 0 && (bits & 1) != 1; bits >>>= 1, next++) {}
        return bits != 0 ? next : NONE_AVAILABLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InputSelection that = (InputSelection) o;
        return inputMask == that.inputMask;
    }

    @Override
    public String toString() {
        return String.valueOf(inputMask);
    }

    /** Utility class for creating {@link InputSelection}. */
    public static final class Builder {

        private long inputMask = 0;

        /**
         * Returns a {@code Builder} that uses the input mask of the specified {@code selection} as
         * the initial mask.
         */
        public static Builder from(InputSelection selection) {
            Builder builder = new Builder();
            builder.inputMask = selection.inputMask;
            return builder;
        }

        /**
         * Selects an input identified by the given {@code inputId}.
         *
         * @param inputId the input id numbered starting from 1 to 64, and `1` indicates the first
         *     input. Specially, `-1` indicates all inputs.
         * @return a reference to this object.
         */
        public Builder select(int inputId) {
            if (inputId > 0 && inputId <= 64) {
                inputMask |= 1L << (inputId - 1);
            } else if (inputId == -1L) {
                inputMask = -1L;
            } else {
                throw new IllegalArgumentException(
                        "The inputId must be in the range of 1 to 64, or be -1.");
            }

            return this;
        }

        /**
         * Build normalized mask, if all inputs were manually selected, inputMask will be normalized
         * to -1.
         */
        public InputSelection build(int inputCount) {
            long allSelectedMask = (1L << inputCount) - 1;
            if (inputMask == allSelectedMask) {
                inputMask = -1;
            } else if (inputMask > allSelectedMask) {
                throw new IllegalArgumentException(
                        String.format(
                                "inputMask [%d] selects more than expected number of inputs [%d]",
                                inputMask, inputCount));
            }
            return build();
        }

        public InputSelection build() {
            return new InputSelection(inputMask);
        }
    }
}
