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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * The interface for Source. It acts like a factory class that helps construct the {@link
 * SplitEnumerator} and {@link SourceReader} and corresponding serializers.
 *
 * @param <T> The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 * @param <EnumChkT> The type of the enumerator checkpoints.
 */
// Source 接口是 Flink 统一 Source API（统一数据源 API）的核心组件，它定义了一个工厂模式（Factory），用于构建和配置一个完整数据源所需的所有运行时组件和元数据。
// 组件工厂： Source 自身并不读取数据，而是充当一个蓝图。它负责在 JobManager 和 TaskManager 上分别创建两个核心运行时组件：
// SplitEnumerator (枚举器/协调者)： 运行在 JobManager 上，负责发现和分配数据分片（Splits）。
// SourceReader (读取器)： 运行在 TaskManager 上，负责实际读取数据。
// 元数据序列化： 它必须提供序列化器，用于安全地传输和持久化关键的元数据，如数据分片（Splits）和枚举器状态检查点。
// 边界性声明： 它是声明数据源是**有界（Bounded）还是无界（Unbounded）**的权威接口。
// Source 接口是 Flink 数据源的配置和构建入口，它将数据源的协调逻辑和数据读取逻辑分离开来，以实现更好的弹性和容错性。
// <T>	记录类型	Source 生产的记录类型。 即数据流中元素的数据类型。
// <SplitT>	分片类型	Source 处理的数据分片类型。 例如，对于 Kafka Source，它可能是 Kafka 分区；对于文件 Source，它可能是文件块或文件路径。它必须继承 SourceSplit。
// <EnumChkT>	枚举器检查点类型	SplitEnumerator 状态的类型。 用于在 JobManager 上检查点保存和恢复枚举器的内部状态，以确保分片分配的容错性。
@Public
public interface Source<T, SplitT extends SourceSplit, EnumChkT>
        extends SourceReaderFactory<T, SplitT> {

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    // 获取数据源的边界性。
    Boundedness getBoundedness();

    /**
     * Creates a new SplitEnumerator for this source, starting a new input.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
     * @return A new SplitEnumerator.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause JobManager failure/recovery.
     */
    // 在 Flink Job 启动时（特别是 JobManager 上）被调用，
    // 用于首次创建一个 SplitEnumerator 实例，开始新的数据输入。它接收一个上下文对象，用于与 Flink 运行时交互。
    SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext)
            throws Exception;

    /**
     * Restores an enumerator from a checkpoint.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
     *     enumerator.
     * @param checkpoint The checkpoint to restore the SplitEnumerator from.
     * @return A SplitEnumerator restored from the given checkpoint.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause JobManager failure/recovery.
     */
    // 在 Flink Job 从检查点或保存点恢复时被调用。
    // 它接收上次检查点保存的枚举器状态 (checkpoint)，并重建一个 SplitEnumerator 实例，以确保分片分配进度不丢失。
    SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception;

    // ------------------------------------------------------------------------
    //  serializers for the metadata
    // ------------------------------------------------------------------------

    /**
     * Creates a serializer for the source splits. Splits are serialized when sending them from
     * enumerator to reader, and when checkpointing the reader's current state.
     *
     * @return The serializer for the split type.
     */
    // 返回一个 SimpleVersionedSerializer<SplitT>，用于序列化和反序列化数据分片。
    // 分片在**枚举器（JobManager）和读取器（TaskManager）**之间传输时需要序列化。
    SimpleVersionedSerializer<SplitT> getSplitSerializer();

    /**
     * Creates the serializer for the {@link SplitEnumerator} checkpoint. The serializer is used for
     * the result of the {@link SplitEnumerator#snapshotState(long)} method.
     *
     * @return The serializer for the SplitEnumerator checkpoint.
     */
    // 返回一个 SimpleVersionedSerializer<EnumChkT>，
    // 用于序列化和反序列化 SplitEnumerator 的内部状态，以便进行检查点保存和恢复。
    SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}
