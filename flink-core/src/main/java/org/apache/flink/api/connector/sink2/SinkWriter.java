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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.Watermark;

import java.io.IOException;

/**
 * The {@code SinkWriter} is responsible for writing data.
 *
 * @param <InputT> The type of the sink writer's input
 */
// SinkWriter 接口是 Flink SinkV2 API 中的核心组件之一，它运行在 TaskManager 的并行子任务中，负责实际处理和写入数据到外部系统。
// 数据处理和缓冲： SinkWriter 从上游操作符接收数据记录（InputT 类型的元素），并根据其内部逻辑（如批处理、缓冲、序列化）准备写入外部系统。
// 与外部系统交互： 它负责与外部数据存储（如文件、数据库连接、消息队列生产者）进行直接通信。
// 保障一致性： 它通过实现 flush 方法来配合 Flink 的检查点机制，确保在检查点完成时，所有待处理的数据都已被成功发送或写入到外部系统，从而实现**至少一次（At-Least-Once）**或更高的一致性保证。
@Public
public interface SinkWriter<InputT> extends AutoCloseable {

    /**
     * Adds an element to the writer.
     *
     * @param element The input record
     * @param context The additional information about the input record
     * @throws IOException if fail to add an element.
     */
    // 写入/添加元素。
    // 这是 SinkWriter 接收和处理单个输入数据记录的核心方法。
    void write(InputT element, Context context) throws IOException, InterruptedException;

    /**
     * Called on checkpoint or end of input so that the writer to flush all pending data for
     * at-least-once.
     */
    // 刷新待处理数据。 此方法在两个关键时刻被调用：
    // 1. 检查点（Checkpoint）开始时。 确保所有待发送或缓冲的数据都被持久化到外部系统，以保障数据一致性。
    // 2. 输入流结束时（endOfInput 为 true）。 确保在任务关闭前清空所有缓冲区。
    void flush(boolean endOfInput) throws IOException, InterruptedException;

    /**
     * Adds a watermark to the writer.
     *
     * <p>This method is intended for advanced sinks that propagate watermarks.
     *
     * @param watermark The watermark.
     * @throws IOException if fail to add a watermark.
     */
    // 写入水位线。
    // 默认实现为空操作。
    // 此方法为高级 Sink 预留，允许它们将 Flink 内部的水位线信息传递或映射到外部系统（例如，用于基于时间的流处理引擎）
    default void writeWatermark(Watermark watermark) throws IOException, InterruptedException {}

    /** Context that {@link #write} can use for getting additional data about an input record. */
    @Public
    interface Context {

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /**
         * Returns the timestamp of the current input record or {@code null} if the element does not
         * have an assigned timestamp.
         */
        Long timestamp();
    }
}
