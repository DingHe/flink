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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/** Basic interface for inputs of stream operators. */
// 任务输入统一接口： 它代表了 Flink StreamTask（例如 SourceStreamTask、TwoInputTask 等）接收数据的入口。
// 它既可以代表从上游任务来的网络输入，也可以代表来自本地数据源（Source）的输入。
// 数据推送与可用性： 继承自 PushingAsyncDataInput，这意味着它采用推模式将数据异步交付给 Task 逻辑，并提供可用性通知（AvailabilityProvider）
@Internal
public interface StreamTaskInput<T> extends PushingAsyncDataInput<T>, Closeable {
    int UNSPECIFIED = -1;

    /** Returns the input index of this input. */
    // 获取输入索引。 *
    // 功能： 返回该输入在当前 Task 中的索引位置。
    // 用途： 对于多输入算子（如 CoProcessFunction 或 Join），索引通常从 0 开始。例如，第一个输入是 0，第二个输入是 1。
    // 这有助于 Task 逻辑区分数据来自哪个输入流。
    int getInputIndex();

    /** Prepares to spill the in-flight input buffers as checkpoint snapshot. */
    // 准备 Checkpoint 快照。
    // 功能： 这是 Task I/O Checkpoint 的核心方法。
    // 它启动将当前 I/O 通道中所有正在传输中但尚未被 Task 处理的数据（即飞行中数据）写入状态存储的过程
    CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException;
}
