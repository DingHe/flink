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

package org.apache.flink.connector.file.src.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * The {@code FileSplitAssigner} is responsible for deciding what split should be processed next by
 * which node. It determines split processing order and locality.
 */
// FileSplitAssigner 接口是 Flink 文件 Source 中，位于 SplitEnumerator 内部的一个关键组件。它专门负责处理文件分片 (FileSourceSplit) 的分配策略和顺序。
// 决定处理顺序： 它维护了所有待处理的文件分片集合，并决定下一个应该被分配给 SourceReader 的分片是哪一个。
// 实现数据本地性： 它通过接收请求分片节点的 主机名 (hostname)，可以应用**数据本地性（Data Locality）**策略，即优先将分片分配给存储该分片数据副本的 TaskManager，从而减少网络 I/O。
// 处理分片恢复： 它提供了将失败或新发现的分片重新添加回分配池的机制。
// FileSplitAssigner 是一个智能分片调度器，它负责从待分配的分片池中，按最优顺序（考虑本地性）挑选下一个工作单元，并追踪剩余工作量。
@PublicEvolving
public interface FileSplitAssigner {

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    // 获取下一个分片。
    // 这是分配器的核心方法。
    // 它接收请求分片的 SourceReader 所在的主机名。
    // 分配器应该利用此 hostname 优先选择具有数据本地性的分片。
    // 如果所有分片都已分配，且 Source 是有界的，则返回 Optional.empty()，信号通知 Source 即将完成。
    Optional<FileSourceSplit> getNext(@Nullable String hostname);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    // 添加分片。
    // 用于将一组新的分片添加到待分配池中。这发生在两种主要情况：
    // 1. 分片发现： SplitEnumerator 发现新文件或新分区。
    // 2. 分片恢复： 某个 SourceReader 失败，它尚未完成的分片被退还给分配器，等待重新分配。
    void addSplits(Collection<FileSourceSplit> splits);

    /** Gets the remaining splits that this assigner has pending. */
    // 获取剩余分片。
    // 返回当前分配器中所有尚未分配的 FileSourceSplit 集合。
    // 这主要用于 SplitEnumerator 在触发检查点时，获取需要保存到状态中的待分配工作。
    Collection<FileSourceSplit> remainingSplits();

    // ------------------------------------------------------------------------

    /**
     * Factory for the {@code FileSplitAssigner}, to allow the {@code FileSplitAssigner} to be
     * eagerly initialized and to not be serializable.
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        /**
         * Creates a new {@code FileSplitAssigner} that starts with the given set of initial splits.
         */
        FileSplitAssigner create(Collection<FileSourceSplit> initialSplits);
    }
}
