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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This handler is mainly used for selecting the next available input index in {@link
 * StreamMultipleInputProcessor}.
 */
// Flink 任务处理多个输入流时使用的核心调度和状态管理组件。它旨在高效地决定：在任何给定时刻，应该从哪个输入通道拉取下一个数据？
// 主要作用是为具有多个输入流（例如 CoProcessFunction 或 KeyedCoProcessFunction）的 Flink Task 提供输入选择逻辑。它通过位掩码（Bit Mask）高效地跟踪每个输入通道的三个关键状态：
// 选择状态（Selected）: Task 逻辑（如 InputSelectable 接口）希望从哪些输入拉取数据。
// 可用状态（Available）: 哪些输入通道当前有数据可读（DataInputStatus.MORE_AVAILABLE）
// 完成状态（Finished）: 哪些输入通道的数据流已经永久结束。
@Internal
public class MultipleInputSelectionHandler {
    // if we directly use Long.SIZE, calculation of allSelectedMask will overflow
    // 最大支持的输入通道数 (63)。 因为该类使用 Java 的 long 类型（64位）作为位掩码来跟踪输入状态，其中 1 位可能被保留，所以最多支持 63 个输入通道。
    public static final int MAX_SUPPORTED_INPUT_COUNT = Long.SIZE - 1;
    // 输入选择器接口。
    // 可选。如果 Task 实现了 InputSelectable，则使用它的 nextSelection() 方法来获取自定义的输入选择掩码。
    @Nullable private final InputSelectable inputSelectable;
    // 当前选择的输入掩码。 表示 Task 逻辑（由 inputSelectable 决定或默认为 ALL）希望拉取数据的输入集合。
    private long selectedInputsMask = InputSelection.ALL.getInputMask();
    // 所有输入都被选中的掩码。 值为 (1L << inputCount) - 1，用于表示所有输入通道的集合。
    private final long allSelectedMask;
    // 当前可用的输入掩码。 表示哪些输入通道（即 StreamTaskInput）当前报告有数据可读（MORE_AVAILABLE）
    private long availableInputsMask;
    // 尚未完成的输入掩码。 表示哪些输入通道还没有到达 END_OF_INPUT（即仍在运行）。
    private long notFinishedInputsMask;
    // 数据结束但分区未结束的输入掩码
    private long dataFinishedButNotPartition;
    // 在 END_OF_DATA 时是否应该耗尽（Drain）
    private boolean drainOnEndOfData = true;

    private enum OperatingMode {
        NO_INPUT_SELECTABLE, // 没有定义自定义输入选择逻辑。
        INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED, // 有自定义选择逻辑，且所有输入都未完成。
        INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED, // 有自定义选择逻辑，但部分输入已收到 END_OF_DATA
        ALL_DATA_INPUTS_FINISHED; // 所有输入都已收到 END_OF_DATA 或 END_OF_INPUT
    }
    // 操作模式枚举。
    // 定义了四种工作模式
    private OperatingMode operatingMode;

    public MultipleInputSelectionHandler(
            @Nullable InputSelectable inputSelectable, int inputCount) {
        checkSupportedInputCount(inputCount);
        this.inputSelectable = inputSelectable;
        this.allSelectedMask = (1L << inputCount) - 1;
        this.availableInputsMask = allSelectedMask;
        this.notFinishedInputsMask = allSelectedMask;
        this.dataFinishedButNotPartition = 0;
        if (inputSelectable != null) {
            this.operatingMode = OperatingMode.INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED;
        } else {
            this.operatingMode = OperatingMode.NO_INPUT_SELECTABLE;
        }
    }

    public static void checkSupportedInputCount(int inputCount) {
        checkArgument(
                inputCount <= MAX_SUPPORTED_INPUT_COUNT,
                "Only up to %s inputs are supported at once, while encountered %s",
                MAX_SUPPORTED_INPUT_COUNT,
                inputCount);
    }

    public DataInputStatus updateStatusAndSelection(DataInputStatus inputStatus, int inputIndex)
            throws IOException {
        switch (inputStatus) {
            case MORE_AVAILABLE:
                nextSelection();
                checkState(checkBitMask(availableInputsMask, inputIndex));
                return DataInputStatus.MORE_AVAILABLE;
            case NOTHING_AVAILABLE:
                availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
                break;
            case STOPPED:
                this.drainOnEndOfData = false;
                // fall through
            case END_OF_DATA:
                dataFinishedButNotPartition = setBitMask(dataFinishedButNotPartition, inputIndex);
                updateModeOnEndOfData();
                break;
            case END_OF_INPUT:
                dataFinishedButNotPartition = unsetBitMask(dataFinishedButNotPartition, inputIndex);
                notFinishedInputsMask = unsetBitMask(notFinishedInputsMask, inputIndex);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported inputStatus = " + inputStatus);
        }
        nextSelection();
        return calculateOverallStatus(inputStatus);
    }

    private void updateModeOnEndOfData() {
        boolean allDataInputsFinished =
                ((dataFinishedButNotPartition | ~notFinishedInputsMask) & allSelectedMask)
                        == allSelectedMask;
        if (allDataInputsFinished) {
            this.operatingMode = OperatingMode.ALL_DATA_INPUTS_FINISHED;
        } else if (this.operatingMode
                == OperatingMode.INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED) {
            this.operatingMode = OperatingMode.INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED;
        }
    }

    private DataInputStatus calculateOverallStatus(DataInputStatus updatedStatus)
            throws IOException {
        if (areAllInputsFinished()) {
            return DataInputStatus.END_OF_INPUT;
        }

        if (updatedStatus == DataInputStatus.END_OF_DATA
                && this.operatingMode == OperatingMode.ALL_DATA_INPUTS_FINISHED) {
            return drainOnEndOfData ? DataInputStatus.END_OF_DATA : DataInputStatus.STOPPED;
        }

        if (isAnyInputAvailable()) {
            return DataInputStatus.MORE_AVAILABLE;
        } else {
            long selectedNotFinishedInputMask = selectedInputsMask & notFinishedInputsMask;
            if (selectedNotFinishedInputMask == 0) {
                throw new IOException(
                        "Can not make a progress: all selected inputs are already finished");
            }
            return DataInputStatus.NOTHING_AVAILABLE;
        }
    }

    void nextSelection() {
        switch (operatingMode) {
            case NO_INPUT_SELECTABLE:
            case ALL_DATA_INPUTS_FINISHED:
                selectedInputsMask = InputSelection.ALL.getInputMask();
                break;
            case INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED:
                selectedInputsMask = inputSelectable.nextSelection().getInputMask();
                break;
            case INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED:
                selectedInputsMask =
                        (inputSelectable.nextSelection().getInputMask()
                                        | dataFinishedButNotPartition)
                                & allSelectedMask;
                break;
        }
    }

    int selectNextInputIndex(int lastReadInputIndex) {
        return InputSelection.fairSelectNextIndex(
                selectedInputsMask,
                availableInputsMask & notFinishedInputsMask,
                lastReadInputIndex);
    }

    boolean shouldSetAvailableForAnotherInput() {
        return (selectedInputsMask & allSelectedMask & ~availableInputsMask) != 0;
    }

    void setAvailableInput(int inputIndex) {
        availableInputsMask = setBitMask(availableInputsMask, inputIndex);
    }

    void setUnavailableInput(int inputIndex) {
        availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
    }

    boolean isAnyInputAvailable() {
        return (selectedInputsMask & availableInputsMask & notFinishedInputsMask) != 0;
    }

    boolean isInputSelected(int inputIndex) {
        return checkBitMask(selectedInputsMask, inputIndex);
    }

    public boolean isInputFinished(int inputIndex) {
        return !checkBitMask(notFinishedInputsMask, inputIndex);
    }

    public boolean areAllInputsFinished() {
        return notFinishedInputsMask == 0;
    }

    public boolean areAllDataInputsFinished() {
        return this.operatingMode == OperatingMode.ALL_DATA_INPUTS_FINISHED;
    }

    long setBitMask(long mask, int inputIndex) {
        return mask | 1L << inputIndex;
    }

    long unsetBitMask(long mask, int inputIndex) {
        return mask & ~(1L << inputIndex);
    }

    boolean checkBitMask(long mask, int inputIndex) {
        return (mask & (1L << inputIndex)) != 0;
    }
}
