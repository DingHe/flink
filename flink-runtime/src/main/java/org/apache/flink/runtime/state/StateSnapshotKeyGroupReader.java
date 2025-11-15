/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.heap.StateTable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;

/** Interface for state de-serialization into {@link StateTable}s by key-group. */
// 目的是定义一个契约，用于将 Checkpoint 或 Savepoint 文件中按 Key Group 分区存储的状态数据，
// 反序列化并加载到 Flink 状态后端内部的数据结构中（特别是 Heap State Backend 中的 StateTable）
// 分 Key Group 恢复： 它是 Flink 实现并行和分布式状态恢复的核心组件。在恢复时，每个 TaskManager 只会读取并加载它自己负责的 Key Group 范围内的状态数据。
// 数据加载： 它负责从输入流（通常是 Checkpoint 文件）中读取 Key Group 的原始字节数据，并将其解析为 Flink 内部可操作的状态映射结构（如 StateTable）
// Heap 状态后端专用： 虽然接口本身是通用的，但 Javadoc 明确提到了它是用于将数据反序列化到 StateTables 中，这主要是针对 Flink 的 Heap State Backend（堆状态后端）。

@Internal
public interface StateSnapshotKeyGroupReader {

    /**
     * Read the data for the specified key-group from the input.
     *
     * @param div the input
     * @param keyGroupId the key-group to write
     * @throws IOException on write related problems
     */
    // 读取指定 Key Group 的状态映射数据。
    // 核心职责
    // 从给定的输入流中读取属于指定 keyGroupId 的所有状态数据，并将这些数据构建成 Flink 内部的状态映射结构（StateTable）
    // 调用时机
    // 在 Flink 任务启动或故障恢复时，KeyedStateBackend 会调用此方法来加载 Checkpoint/Savepoint 中的数据。
    void readMappingsInKeyGroup(@Nonnull DataInputView div, @Nonnegative int keyGroupId)
            throws IOException;
}
