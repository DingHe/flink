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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;

/**
 * A mixin interface for a {@link Sink} which supports exactly-once semantics using a two-phase
 * commit protocol. The {@link Sink} consists of a {@link CommittingSinkWriter} that performs the
 * precommits and a {@link Committer} that actually commits the data. To facilitate the separation
 * the {@link CommittingSinkWriter} creates <i>committables</i> on checkpoint or end of input and
 * the sends it to the {@link Committer}.
 *
 * <p>The {@link Sink} needs to be serializable. All configuration should be validated eagerly. The
 * respective sink writers and committers are transient and will only be created in the subtasks on
 * the taskmanagers.
 *
 * @param <CommittableT> The type of the committables.
 */
// SupportsCommitter 接口是 Flink SinkV2 API 的一个混入（Mixin）接口，用于扩展基础 Sink 接口，使其能够支持基于**两阶段提交（Two-Phase Commit, 2PC）协议的精确一次（Exactly-Once）**写入语义。
// 定义精确一次能力： 实现此接口的 Sink 明确表明它能够通过 2PC 协议保证数据在写入外部系统时的精确一次一致性。
// 创建 Committer： 它的主要职责是提供一个工厂方法 (createCommitter)，用于在 Flink 运行时创建 Committer 组件。Committer 运行在 JobManager 上（或 TaskManager 的协调任务中），负责执行 2PC 协议的第二阶段（最终提交）
// 提供序列化器： 它必须提供一个序列化器，用于安全地序列化和传输 Committable 对象，确保这些关键元数据能在 Flink 的检查点和 JobManager/Committer 之间正确传递
// CommittableT	提交类型	两阶段提交中用于封装提交所需元数据的类型。 这是 CommittingSinkWriter 在预提交阶段生成的对象类型，包含了提交数据所需的所有信息（如文件路径、事务 ID）。
@Public
public interface SupportsCommitter<CommittableT> {

    /**
     * Creates a {@link Committer} that permanently makes the previously written data visible
     * through {@link Committer#commit(Collection)}.
     *
     * @param context The context information for the committer initialization.
     * @return A committer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     */
    // Flink 在 TaskManager 上（或 JobManager 上的特殊任务中）调用此方法，使用 CommitterInitContext 提供的上下文信息来实例化 Committer。
    // Committer 将负责接收来自所有 SinkWriter 的 CommittableT 集合，并安全地将数据提交到外部存储。
    Committer<CommittableT> createCommitter(CommitterInitContext context) throws IOException;

    /** Returns the serializer of the committable type. */
    // 返回一个实现了 SimpleVersionedSerializer 接口的序列化器，用于将 CommittableT 对象序列化和反序列化。
    // 这对于在 SinkWriter（生成 Committable）和 Committer（使用 Committable）之间以及 Flink 检查点中安全地存储和传输这些元数据至关重要。
    SimpleVersionedSerializer<CommittableT> getCommittableSerializer();
}
