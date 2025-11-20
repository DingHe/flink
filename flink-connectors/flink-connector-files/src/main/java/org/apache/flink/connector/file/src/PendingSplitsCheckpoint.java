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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet
 * assigned.
 */
// PendingSplitsCheckpoint 类是 Flink 文件 Source (FileSource) 专用的枚举器检查点状态 (EnumChkT)。
// 它封装了 SplitEnumerator 在检查点时需要持久化的所有关键信息。
// 容错： 它保存了所有尚未分配给任何 SourceReader 的待处理分片（Pending Splits）列表。
// 当 Flink Job 失败并恢复时，SplitEnumerator 可以从这个检查点状态中重建其待分配分片池，确保所有文件分片最终都会被处理，即使 Job 发生重启。
// 持续监控（Continuous Monitoring）支持： 对于配置为持续监控模式的文件 Source（即不断检查新文件），它还保存了已处理文件的路径。
// 这用于在重启或故障恢复后，避免重新处理那些在上次检查点之前已完成的文件。

@PublicEvolving
public class PendingSplitsCheckpoint<SplitT extends FileSourceSplit> {

    /** The splits in the checkpoint. */
    // 待分配的分片集合。
    // 包含了 SplitEnumerator 在检查点创建时，尚未分配给任何 SourceReader 的所有 FileSourceSplit。
    private final Collection<SplitT> splits;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    // 已处理的路径集合。
    // 仅在文件 Source 运行在持续监控模式下相关。它包含已完成处理且在当前检查点中不再出现的文件的路径，用于在恢复时忽略这些文件。
    private final Collection<Path> alreadyProcessedPaths;

    /**
     * The cached byte representation from the last serialization step. This helps to avoid paying
     * repeated serialization cost for the same checkpoint object. This field is used by {@link
     * PendingSplitsCheckpointSerializer}.
     */
    // 序列化缓存。
    // 用于缓存此检查点对象的字节表示。
    // 使用 transient 关键字表示它不需要通过 Java 默认序列化机制序列化，而是由专门的 PendingSplitsCheckpointSerializer 使用，以提高序列化效率。
    @Nullable byte[] serializedFormCache;

    protected PendingSplitsCheckpoint(
            Collection<SplitT> splits, Collection<Path> alreadyProcessedPaths) {
        this.splits = Collections.unmodifiableCollection(splits);
        this.alreadyProcessedPaths = Collections.unmodifiableCollection(alreadyProcessedPaths);
    }

    // ------------------------------------------------------------------------

    public Collection<SplitT> getSplits() {
        return splits;
    }

    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "PendingSplitsCheckpoint:\n"
                + "\t\t Pending Splits: "
                + splits
                + '\n'
                + "\t\t Processed Paths: "
                + alreadyProcessedPaths
                + '\n';
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    public static <T extends FileSourceSplit> PendingSplitsCheckpoint<T> fromCollectionSnapshot(
            final Collection<T> splits) {
        checkNotNull(splits);

        // create a copy of the collection to make sure this checkpoint is immutable
        final Collection<T> copy = new ArrayList<>(splits);
        return new PendingSplitsCheckpoint<>(copy, Collections.emptySet());
    }

    public static <T extends FileSourceSplit> PendingSplitsCheckpoint<T> fromCollectionSnapshot(
            final Collection<T> splits, final Collection<Path> alreadyProcessedPaths) {
        checkNotNull(splits);

        // create a copy of the collection to make sure this checkpoint is immutable
        final Collection<T> splitsCopy = new ArrayList<>(splits);
        final Collection<Path> pathsCopy = new ArrayList<>(alreadyProcessedPaths);

        return new PendingSplitsCheckpoint<>(splitsCopy, pathsCopy);
    }

    static <T extends FileSourceSplit> PendingSplitsCheckpoint<T> reusingCollection(
            final Collection<T> splits, final Collection<Path> alreadyProcessedPaths) {
        return new PendingSplitsCheckpoint<>(splits, alreadyProcessedPaths);
    }
}
