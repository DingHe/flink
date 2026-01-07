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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;
import java.util.List;

// 为 Flink 任务中具有唯一索引的输入网关提供一个统一的基础，
// 集成数据拉取（InputGate 的能力）、检查点协调（CheckpointableInput 的能力）和管理多个输入通道
// 索引化： 它为输入网关引入了唯一的索引 (getGateIndex())，这在任务有多个输入（例如，处理来自不同算子的数据流或处理恢复状态的 InputGate 和新的数据流的 InputGate）时，是必不可少的身份标识。
/** An {@link InputGate} with a specific index. */
public abstract class IndexedInputGate extends InputGate implements CheckpointableInput {
    /** Returns the index of this input gate. Only supported on */
    // 获取网关索引（抽象）。
    // 返回该输入网关在当前任务所有输入网关中的唯一索引。这是识别网关的关键。
    public abstract int getGateIndex();

    // 获取未完成通道列表（抽象）。
    // 返回一个列表，包含所有尚未收到 EndOfPartitionEvent（分区结束事件）的输入通道的信息。
    // 这对于判断任务是否已耗尽所有输入数据至关重要。
    /** Returns the list of channels that have not received EndOfPartitionEvent. */
    public abstract List<InputChannelInfo> getUnfinishedChannels();

    // 检查点开始通知。
    // 遍历该网关下的所有 InputChannel，并调用它们的 checkpointStarted(barrier) 方法。
    // 将检查点开始的信号向下转发给每一个通道。
    @Override
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        for (int index = 0, numChannels = getNumberOfInputChannels();
                index < numChannels;
                index++) {
            getChannel(index).checkpointStarted(barrier);
        }
    }

    // 检查点停止通知。
    // 遍历该网关下的所有 InputChannel，并调用它们的 checkpointStopped(cancelledCheckpointId) 方法。
    // 将检查点取消的信号向下转发。
    @Override
    public void checkpointStopped(long cancelledCheckpointId) {
        for (int index = 0, numChannels = getNumberOfInputChannels();
                index < numChannels;
                index++) {
            getChannel(index).checkpointStopped(cancelledCheckpointId);
        }
    }
    // 获取输入网关索引
    @Override
    public int getInputGateIndex() {
        return getGateIndex();
    }

    // 该方法的实现为空（Unused）。
    // 注释说明这是因为 Flink 的网络栈通常通过**回收信用点（revoking credits）**机制自动实现消费阻塞，不需要显式调用此方法。
    @Override
    public void blockConsumption(InputChannelInfo channelInfo) {
        // Unused. Network stack is blocking consumption automatically by revoking credits.
    }

    // 转换为优先级事件。
    // 调用特定索引通道的 convertToPriorityEvent(sequenceNumber) 方法。
    // 这是在非对齐检查点中，将普通数据缓冲区转换为控制事件的关键步骤。
    @Override
    public void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException {
        getChannel(channelIndex).convertToPriorityEvent(sequenceNumber);
    }

    // 触发 Debloating 机制（抽象）。
    // 用于触发 Flink 的 Debloating（去膨胀）或自适应流量控制机制。
    // 这通常涉及到调整内部缓冲区或延迟，以优化端到端延迟和资源利用率。
    public abstract void triggerDebloating();
}
