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

import org.apache.flink.annotation.Internal;

/**
 * Savepoint resources for a {@link KeyedStateBackend}. This is only a container for the {@link
 * FullSnapshotResources} that will be used by the {@link SavepointSnapshotStrategy} and gives the
 * backend a way to tell the {@link SnapshotStrategyRunner} whether it prefers asynchronous or
 * synchronous writing.
 *
 * @param <K> type of the backend keys.
 */
// Flink 键控状态后端（KeyedStateBackend） 在创建 Savepoint 时返回的一个容器对象。
// 资源打包： 它的主要职责是封装执行 Savepoint 所需的全量快照资源（FullSnapshotResources），这些资源包含了状态的元数据和所有数据迭代器。
// 执行偏好传递： 它允许状态后端向 Flink 的快照执行机制（SnapshotStrategyRunner）表明自己倾向于使用哪种写入模式——同步（SYNCHRONOUS）还是异步（ASYNCHRONOUS）
// 它是 Savepoint 机制中，状态后端向 Flink 运行时传递“我需要快照的数据”和“我希望如何写入快照”的载体。

@Internal
public class SavepointResources<K> {
    // 全量快照资源。 这是 Savepoint 的核心数据。它封装了将当前状态写入 Savepoint 文件所需的所有键值对迭代器、状态元信息快照和序列化器等底层资源。
    private final FullSnapshotResources<K> snapshotResources;
    private final SnapshotExecutionType preferredSnapshotExecutionType;

    public SavepointResources(
            FullSnapshotResources<K> snapshotResources,
            SnapshotExecutionType preferredSnapshotExecutionType) {
        this.snapshotResources = snapshotResources;
        this.preferredSnapshotExecutionType = preferredSnapshotExecutionType;
    }

    public FullSnapshotResources<K> getSnapshotResources() {
        return snapshotResources;
    }

    public SnapshotExecutionType getPreferredSnapshotExecutionType() {
        return preferredSnapshotExecutionType;
    }
}
