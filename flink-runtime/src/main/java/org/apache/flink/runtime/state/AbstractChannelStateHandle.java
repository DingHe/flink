/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract channel state handle.
 *
 * @param <Info> type of channel info (e.g. {@link
 *     org.apache.flink.runtime.checkpoint.channel.InputChannelInfo InputChannelInfo}).
 */
// 作为 Flink 非对齐检查点（Unaligned Checkpointing）中，用于持久化和恢复输入/输出通道数据状态的抽象父类。
    // 数据引用（delegate）： 实际数据存储在哪里（通常是一个文件）。
    // 数据分块信息（offsets）： 如何从存储中精确读取该通道的状态数据。
    // 通道标识（info）： 标识这个状态属于哪个具体的输入或输出通道
    // 弹性伸缩信息（subtaskIndex）： 记录状态所属的原始任务索引，用于恢复和并行度调整
@Internal
public abstract class AbstractChannelStateHandle<Info> implements StateObject {

    private static final long serialVersionUID = 1L;
    // 通道信息。
    // 用于唯一标识该状态所属的通道。对于输入通道，Info 类型通常是 InputChannelInfo
    private final Info info;
    // 指向实际存储了通道数据的底层流句柄。
    // 由于多个通道的状态可能被写入同一个物理文件（即委托句柄可以共享），因此它被称为“委托”（Delegate）
    private final StreamStateHandle delegate;
    /**
     * Start offsets in a {@link org.apache.flink.core.fs.FSDataInputStream stream} {@link
     * StreamStateHandle#openInputStream obtained} from {@link #delegate}.
     */
    // 流中数据起始偏移量列表。
    // 定义了该通道的数据在 delegate 所引用的物理流（文件）中的多个分段的起始位置（字节偏移量）
    private final List<Long> offsets;
    // 通道状态的总大小（字节）。 记录该特定通道的状态数据占用的总存储空间大小
    private final long size;

    /** The original subtask index before rescaling recovery. */
    // 原始子任务索引。
    // 在发生并行度调整（Rescaling）时，这个索引记录了生成该通道状态的原始子任务的 ID。这对于状态的正确分配至关重要。
    private final int subtaskIndex;

    AbstractChannelStateHandle(
            StreamStateHandle delegate,
            List<Long> offsets,
            int subtaskIndex,
            Info info,
            long size) {
        this.subtaskIndex = subtaskIndex;
        this.info = checkNotNull(info);
        this.delegate = checkNotNull(delegate);
        this.offsets = checkNotNull(offsets);
        this.size = size;
    }

    public static Stream<StreamStateHandle> collectUniqueDelegates(
            Stream<StateObjectCollection<? extends AbstractChannelStateHandle<?>>> collections) {
        return collections
                .flatMap(Collection::stream)
                .map(AbstractChannelStateHandle::getDelegate)
                .distinct();
    }

    @Override
    public void discardState() throws Exception {
        delegate.discardState();
    }

    @Override
    public long getStateSize() {
        return size; // can not rely on delegate.getStateSize because it can be shared
    }

    public List<Long> getOffsets() {
        return offsets;
    }

    public StreamStateHandle getDelegate() {
        return delegate;
    }

    public Info getInfo() {
        return info;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractChannelStateHandle<?> that = (AbstractChannelStateHandle<?>) o;
        return subtaskIndex == that.subtaskIndex
                && info.equals(that.info)
                && delegate.equals(that.delegate)
                && offsets.equals(that.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskIndex, info, delegate, offsets);
    }

    @Override
    public String toString() {
        return "AbstractChannelStateHandle{"
                + "info="
                + info
                + ", delegate="
                + delegate
                + ", offsets="
                + offsets
                + ", size="
                + size
                + '}';
    }

    /** Describes the underlying content. */
    public static class StateContentMetaInfo {
        private final List<Long> offsets;
        private long size = 0;

        public StateContentMetaInfo() {
            this(new ArrayList<>(), 0);
        }

        public StateContentMetaInfo(List<Long> offsets, long size) {
            this.offsets = offsets;
            this.size = size;
        }

        public void withDataAdded(long offset, long size) {
            this.offsets.add(offset);
            this.size += size;
        }

        public List<Long> getOffsets() {
            return Collections.unmodifiableList(offsets);
        }

        public long getSize() {
            return size;
        }
    }
}
