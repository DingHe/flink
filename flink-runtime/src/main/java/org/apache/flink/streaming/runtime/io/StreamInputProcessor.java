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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.streaming.api.operators.InputSelectable;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for processing records by {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
 */
// 定义了流任务（StreamTask）如何从其输入通道读取和处理数据的契约
// 这个接口是 Flink 任务中实现数据摄入、背压（backpressure）、以及协调检查点（Checkpointing）的关键。
// StreamInputProcessor（流输入处理器）是 Task 内部负责管理所有输入流和输入数据的组件。
// 它封装了从网络栈 (InputGate) 中拉取数据、处理检查点屏障、以及将记录交付给用户算子进行处理的全部逻辑。
@Internal
public interface StreamInputProcessor extends AvailabilityProvider, Closeable {
    /**
     * In case of two and more input processors this method must call {@link
     * InputSelectable#nextSelection()} to choose which input to consume from next.
     *
     * @return input status to estimate whether more records can be processed immediately or not. If
     *     there are no more records available at the moment and the caller should check finished
     *     state and/or {@link #getAvailableFuture()}.
     */
    // 处理输入数据
    // 每次调用都尝试从输入通道读取并处理一个或一批数据记录，或者处理控制事件（如检查点屏障）
    DataInputStatus processInput() throws Exception;

    // 准备通道状态快照
    // 在检查点屏障成功对齐之后（即任务要进行本地状态快照之前），任务调用此方法来处理**通道状态（Channel State）**的快照准备工作。
    CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException;
}
