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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifies {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel} in a given
 * subtask. Note that {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannelID
 * InputChannelID} can not be used because it is generated randomly.
 */
// 为特定 Flink 子任务中的输入通道（Input Channel）提供一个稳定、确定性的逻辑标识符。
// 在 Flink 的运行时网络栈中：
// 输入网关（Input Gate）： 一个子任务可能从多个上游任务接收数据，每组数据源于一个输入网关。
// 输入通道（Input Channel）： 每个输入网关内部包含多个输入通道，每个通道对应着一个特定的上游并行实例。
// 该类使用 输入网关的索引 和 输入通道在网关中的索引 来唯一定位一个通道。
// 它不使用 Flink 内部网络栈随机生成的 InputChannelID，确保了在作业重启或恢复时，通道的身份标识是稳定且可重现的。

@Internal
public class InputChannelInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    // 输入网关索引（Gate Index）。
    // 表示该通道所属的输入网关（Input Gate）在当前子任务中的序号。
    // 例如，一个任务可能有两个输入（两个上游），它们的网关索引可能是 0 和 1。
    private final int gateIdx;
    // 输入通道索引（Channel Index）。
    // 表示该通道在该输入网关内部的序号。例如，如果上游任务并行度为 4，这个网关可能有 4 个通道，索引从 0 到 3。
    private final int inputChannelIdx;

    public InputChannelInfo(int gateIdx, int inputChannelIdx) {
        this.gateIdx = gateIdx;
        this.inputChannelIdx = inputChannelIdx;
    }

    public int getGateIdx() {
        return gateIdx;
    }

    public int getInputChannelIdx() {
        return inputChannelIdx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final InputChannelInfo that = (InputChannelInfo) o;
        return gateIdx == that.gateIdx && inputChannelIdx == that.inputChannelIdx;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gateIdx, inputChannelIdx);
    }

    @Override
    public String toString() {
        return "InputChannelInfo{"
                + "gateIdx="
                + gateIdx
                + ", inputChannelIdx="
                + inputChannelIdx
                + '}';
    }
}
