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

import java.io.IOException;
import java.util.List;

/**
 * A {@link SinkWriter} whose state needs to be checkpointed.
 *
 * @param <InputT> The type of the sink writer's input
 * @param <WriterStateT> The type of the writer's state
 */
// StatefulSinkWriter 接口是 Flink SinkV2 API 的一个扩展，它定义了**支持状态快照（State Snapshotting）**的 SinkWriter
// 管理内部状态： 实现了此接口的 SinkWriter 可以维护需要在 Flink 检查点（Checkpoint）中保存和恢复的内部状态。
// 这种状态对于实现精确一次（Exactly-Once）或事务性写入至关重要。
// 参与检查点： 它通过 snapshotState 方法将当前的关键内部状态（如批次 ID、未提交的事务句柄、缓冲区内容等）序列化，作为 Flink 检查点的一部分。
// 保障容错： 当 Flink 任务失败时，它可以使用上一个成功检查点中保存的状态来恢复，确保数据写入过程可以从上次成功的状态继续，实现容错和一致性
// WriterStateT	写入器状态类型	SinkWriter 内部状态的数据类型。 这个类型定义了需要被检查点保存和恢复的信息的结构。例如，它可以是一个表示文件句柄、事务 ID 或未完成缓冲区的自定义类。
@Public
public interface StatefulSinkWriter<InputT, WriterStateT> extends SinkWriter<InputT> {
    /**
     * @return The writer's state.
     * @throws IOException if fail to snapshot writer's state.
     */
    // 创建状态快照。
    // 在 Flink 检查点触发时调用。此方法是有状态 Sink 参与检查点的核心。
    List<WriterStateT> snapshotState(long checkpointId) throws IOException;
}
