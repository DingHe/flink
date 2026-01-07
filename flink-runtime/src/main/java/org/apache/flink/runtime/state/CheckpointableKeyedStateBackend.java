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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.CheckpointListener;

import javax.annotation.Nonnull;

import java.io.Closeable;

/**
 * Interface that combines both, the {@link KeyedStateBackend} interface, which encapsulates methods
 * responsible for keyed state management and the {@link Snapshotable} which tells the system how to
 * snapshot the underlying state.
 *
 * <p><b>NOTE:</b> State backends that need to be notified of completed checkpoints can additionally
 * implement the {@link CheckpointListener} interface.
 *
 * @param <K> Type of the key by which state is keyed.
 */
// 定义了支持 Checkpoint 和 Savepoint 机制的键控状态后端所必须具备的能力。
// 整合核心能力： 它将 键控状态管理（KeyedStateBackend 的职责）与 容错快照能力（Snapshotable 的职责）整合在一起。
public interface CheckpointableKeyedStateBackend<K>
        extends KeyedStateBackend<K>, Snapshotable<SnapshotResult<KeyedStateHandle>>, Closeable {

    /** Returns the key groups which this state backend is responsible for. */
    // 获取当前后端负责的 Key Group 范围
    // 返回一个 KeyGroupRange 对象，它定义了当前状态后端实例负责管理哪些 Key Group 的状态
    // 背景： Flink 将状态逻辑上划分为 Key Group，然后将这些 Key Group 平均分配给并行任务实例。此方法告诉运行时，这个实例能够处理哪些 Key Group 的数据和快照。
    KeyGroupRange getKeyGroupRange();

    /**
     * Returns a {@link SavepointResources} that can be used by {@link SavepointSnapshotStrategy} to
     * write out a savepoint in the common/unified format.
     */
    // 创建 Savepoint 资源
    // 触发状态后端创建一个特殊的资源对象 (SavepointResources)，
    // 该对象包含将当前状态作为 Savepoint 写入统一格式所需的一切信息（通常用于 Flink 升级或版本间兼容）
    @Nonnull
    SavepointResources<K> savepoint() throws Exception;
}
