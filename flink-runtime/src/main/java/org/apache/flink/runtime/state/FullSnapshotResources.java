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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.IOException;
import java.util.List;

/**
 * A {@link SnapshotResources} to be used with the backend-independent {@link
 * FullSnapshotAsyncWriter}.
 *
 * @param <K> type of the backend keys.
 */
// 用于封装执行全量状态快照所需的所有数据和元信息资源。
// 它充当了 状态后端（State Backend） 的同步阶段（创建资源）与 异步写入器（FullSnapshotAsyncWriter） 之间的数据传输载体。
// 全量快照支持： 它是专门为全量快照策略设计的，要求能够提供所有状态的元数据和所有键值对的迭代器。
// 异步写入准备： 它将状态后端在同步阶段准备好的所有必要信息（如 Key 迭代器、序列化器、元信息等）打包，供 Checkpoint 的异步写入线程使用。
// 继承资源管理： 它继承了 SnapshotResources，意味着它也必须提供资源清理方法 release()。
@Internal
public interface FullSnapshotResources<K> extends SnapshotResources {

    /**
     * Returns the list of {@link StateMetaInfoSnapshot meta info snapshots} for this state
     * snapshot.
     */
    // 获取所有状态的元信息快照列表。
    List<StateMetaInfoSnapshot> getMetaInfoSnapshots();

    /**
     * Returns a {@link KeyValueStateIterator} for iterating over all key-value states for this
     * snapshot resources.
     */
    // 创建键值状态迭代器
    // 返回一个特殊的迭代器 (KeyValueStateIterator)，用于顺序遍历当前状态后端中所有 Key Group 的所有键值状态数据。
    KeyValueStateIterator createKVStateIterator() throws IOException;

    /** Returns the {@link KeyGroupRange} of this snapshot. */
    // 获取 Key Group 范围
    KeyGroupRange getKeyGroupRange();

    /** Returns key {@link TypeSerializer}. */
    // 获取 Key 的序列化器。
    TypeSerializer<K> getKeySerializer();

    /** Returns the {@link StreamCompressionDecorator} that should be used for writing. */
    // 获取流压缩装饰器。
    // 返回一个对象，该对象定义了在将状态数据写入目标输出流时应该使用哪种压缩算法（如果配置了压缩）
    StreamCompressionDecorator getStreamCompressionDecorator();
}
