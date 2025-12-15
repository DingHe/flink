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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.util.Optional;

/**
 * A placeholder state handle for shared state that will replaced by an original that was created in
 * a previous checkpoint. So we don't have to send a state handle twice, e.g. in case of {@link
 * ByteStreamStateHandle}. This class is used in the referenced states of {@link
 * IncrementalRemoteKeyedStateHandle}.
 */
// PlaceholderStreamStateHandle（占位符流状态句柄）是一种特殊的状态句柄，它的核心作用是为了优化增量检查点 (Incremental Checkpoint) 机制中的状态传输和冗余。
// 问题： 在增量检查点中，许多状态块（StateHandle）是在上一次检查点中创建并已上传到远程存储的（共享状态）。如果 TaskManager 每次都将完整的、包含实际存储位置信息的 StreamStateHandle（如 FileStateHandle）发送给 JobMaster，会造成元数据的冗余传输。
// 解决方案： 当 TaskManager 生成增量检查点时，对于那些未修改的、已经在上一个检查点中引用的状态块，它不发送完整的句柄，而是发送一个轻量级的 PlaceholderStreamStateHandle。
// 标识符： 它只携带状态的唯一标识符 (PhysicalStateHandleID) 和大小。
// 替换标记： 它告诉 JobMaster：“这个状态我已经有了，它的 ID 是 X。请在构建最终的 CompletedCheckpoint 元数据时，用上一个检查点中 ID 为 X 的那个真正的 StreamStateHandle 来替换我这个占位符。”
// 禁止操作： 它明确禁止了对流的读取 (openInputStream()) 或获取字节数组 (asBytesIfInMemory())，因为它本身不包含任何实际的 I/O 信息，只是一个元数据引用。

public class PlaceholderStreamStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;
    // 物理状态句柄 ID。
    // 这是该句柄引用的底层物理状态块的唯一标识符。JobMaster 使用这个 ID 来查找和替换为真正的 StreamStateHandle。
    private final PhysicalStateHandleID physicalID;
    // 状态大小。
    // 占位符携带的状态数据的字节大小，用于 JobMaster 在不读取实际句柄的情况下计算总检查点大小。
    private final long stateSize;
    // 文件是否已合并标志。
    // 在 RocksDB 状态后端中，用于标记该状态块是否已经是完全合并的 SST 文件（与增量状态的管理相关）
    private final boolean fileMerged;

    public PlaceholderStreamStateHandle(
            PhysicalStateHandleID physicalID, long stateSize, boolean fileMerged) {
        this.physicalID = physicalID;
        this.stateSize = stateSize;
        this.fileMerged = fileMerged;
    }

    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException(
                "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        throw new UnsupportedOperationException(
                "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return physicalID;
    }

    @Override
    public void discardState() throws Exception {
        // nothing to do.
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    public boolean isFileMerged() {
        return fileMerged;
    }
}
