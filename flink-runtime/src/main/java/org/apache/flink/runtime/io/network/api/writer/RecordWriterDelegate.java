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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;

import java.io.IOException;

/**
 * The record writer delegate provides the availability function for task processor, and it might
 * represent a single {@link RecordWriter} or multiple {@link RecordWriter} instances in specific
 * implementations.
 */
// RecordWriterDelegate<T> 的主要作用是统一和简化 Flink 任务向其下游（通过网络）发送数据的过程，并提供统一的**可用性（Availability）**检查机制。
// 统一输出管理： 它封装了实际的 RecordWriter 实例。在简单情况下，它可能只包装一个 RecordWriter；但在多输出或特殊调度场景下（例如 Flink 的 TwoOutputTask），它可能包装多个 RecordWriter
// 提供可用性信号： 继承自 AvailabilityProvider，允许 Flink 的 Mailbox 调度机制检查是否有足够的缓冲区来发送数据。这是实现背压和高效 I/O 调度的关键。
// 它是 Task 线程向网络 I/O 层写入数据和控制事件的通用入口，并且提供了重要的流控制信息。

public interface RecordWriterDelegate<T extends IOReadableWritable>
        extends AvailabilityProvider, AutoCloseable {

    /**
     * Broadcasts the provided event to all the internal record writer instances.
     *
     * @param event the event to be emitted to all the output channels.
     */
    // 将一个控制事件广播到所有输出通道。
    void broadcastEvent(AbstractEvent event) throws IOException;

    /**
     * Returns the internal actual record writer instance based on the output index.
     *
     * @param outputIndex the index respective to the record writer instance.
     */
    // 根据输出索引返回内部实际的 RecordWriter 实例。
    RecordWriter<T> getRecordWriter(int outputIndex);

    /** Sets the max overdraft buffer size of per gate. */
    // 设置每个 Gate（输入/输出门）允许的最大透支缓冲区大小。
    // 这是一种精细化的流控制机制，用于平衡低延迟和高吞吐量。透支缓冲区可以稍微提高 Task 的数据生产速率，减少等待时间，但需要小心控制，以避免过度消耗内存。
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);
}
