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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;
import java.util.List;

/**
 * Input, with just basic methods for blocking and resuming consumption. It can be for example an
 * {@link InputGate} or a chained source.
 */
// 为 Flink 任务中所有能够接收上游数据的组件（无论是网络输入网关还是链式算子的数据源）定义一套统一的、与检查点协调相关的控制契约
// 在 Flink 的分布式检查点过程中，任务管理器（TaskManager）需要对所有数据输入进行控制，以实现**屏障对齐（Barrier Alignment）或非对齐检查点（Unaligned Checkpoint）**所需的流程。
// 这个接口就是为了抽象化所有这些输入组件，使其能够响应以下检查点相关的控制操作：
// 流控制： 阻塞和恢复特定通道的数据消费。
// 检查点生命周期： 响应检查点屏障的开始和停止事件。
// 元数据获取： 提供通道数量和通道信息，以便检查点协调器使用。
@Internal
public interface CheckpointableInput {
    // 阻塞通道消费。
    // 用于在对齐检查点期间（Aligned Checkpoint），当 Checkpoint Barrier 到达某个输入通道后，
    // 该方法会被调用，以停止消费指定通道 (channelInfo) 的数据。
    // 数据会被缓存，直到屏障在所有通道上都对齐。
    void blockConsumption(InputChannelInfo channelInfo);

    // 恢复通道消费。
    // 在屏障对齐完成（或非对齐检查点完成状态保存）后，调用此方法来恢复消费指定输入通道的数据。
    void resumeConsumption(InputChannelInfo channelInfo) throws IOException;

    // 获取所有通道信息。
    // 返回该输入组件所包含的所有输入通道的唯一标识符列表 (InputChannelInfo)
    List<InputChannelInfo> getChannelInfos();

    // 获取输入通道数量。
    // 返回该输入组件当前连接的输入通道总数。
    int getNumberOfInputChannels();

    // 检查点开始通知。
    // 通知该输入组件一个新的检查点已经开始，并传递检查点屏障 (CheckpointBarrier)
    void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException;

    void checkpointStopped(long cancelledCheckpointId);
    // 获取输入网关索引。
    // 返回该输入组件在其任务中的逻辑索引。这在任务包含多个输入网关时用于区分。
    int getInputGateIndex();

    void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException;
}
