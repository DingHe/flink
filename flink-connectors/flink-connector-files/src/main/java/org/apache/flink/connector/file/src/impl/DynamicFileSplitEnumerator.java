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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A SplitEnumerator implementation that supports dynamic filtering.
 *
 * <p>This enumerator handles {@link DynamicFilteringEvent} and filter out the desired input splits
 * with the support of the {@link DynamicFileEnumerator}.
 *
 * <p>If the enumerator receives the first split request before any dynamic filtering data is
 * received, it will enumerate all splits. If a DynamicFilterEvent is received during the fully
 * enumerating, the remaining splits will be filtered accordingly.
 */
// DynamicFileSplitEnumerator 是 Flink 文件 Source (FileSource) 的 SplitEnumerator 实现，
// 专门设计用于支持 Table/SQL 层面的**动态过滤（Dynamic Filtering）**优化。
// 处理动态过滤事件： 它能够接收并处理来自 SourceReader 的 DynamicFilteringEvent，该事件携带着用于过滤的文件分区信息 (DynamicFilteringData)
// 动态裁剪 Splits： 在接收到过滤数据后，它会使用实现了 DynamicFileEnumerator 的组件来重新枚举和切割文件，确保只生成与过滤条件匹配的文件分片。
// 管理分片状态与恢复： 它管理着一个 FileSplitAssigner 来分配 splits，并维护一个 assignedSplits 集合来追踪哪些分片已经被分配，以防止在动态过滤导致的重新分配时重复分配 splits。
// 批处理限定： 当前的实现（通过 snapshotState 抛出异常）表明它主要设计用于批处理执行场景。
// 智能文件分片协调者，能够根据运行时从小表接收到的过滤数据，动态地优化大表的读取，避免扫描不必要的数据。
@Internal
public class DynamicFileSplitEnumerator<SplitT extends FileSourceSplit>
        implements SplitEnumerator<SplitT, PendingSplitsCheckpoint<SplitT>>,
                SupportsHandleExecutionAttemptSourceEvent {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicFileSplitEnumerator.class);
    // 枚举器上下文。
    // 用于与 Flink 运行时（JobManager）进行通信，例如分配 splits 或发送信号。
    private final SplitEnumeratorContext<SplitT> context;
    // 动态文件枚举器工厂。
    // 用于创建实际执行文件发现和动态过滤逻辑的 DynamicFileEnumerator 实例。
    private final DynamicFileEnumerator.Provider fileEnumeratorFactory;
    // 分片分配器工厂。
    // 用于创建负责管理 splits 分配顺序和本地性逻辑的 FileSplitAssigner 实例。
    private final FileSplitAssigner.Provider splitAssignerFactory;

    /**
     * Stores the id of splits that has been assigned. The split assigner may be rebuilt when a
     * DynamicFilteringEvent is received. After that, the splits that are already assigned can be
     * assigned for the second time. We have to retain the state and filter out the splits that has
     * been assigned with this set.
     */
    // 已分配分片 ID 集合。
    // 存储所有已经分配给 SourceReader 的分片 ID。
    // 这是为了防止在重新创建 splitAssigner 后，已分配的分片被二次分配。
    private final Set<String> assignedSplits;
    // 所有被枚举的分片 ID 集合。
    // 存储最近一次调用 enumerateSplits 发现的所有分片 ID。
    // 用于在 addSplitsBack 时过滤掉那些因动态过滤而被裁剪的分片。
    private transient Set<String> allEnumeratingSplits;
    // 当前的分片分配器实例。
    // 负责管理待分配 splits 的集合和分配逻辑。
    // 它可以在收到动态过滤事件后被重建。
    private transient FileSplitAssigner splitAssigner;

    // ------------------------------------------------------------------------

    public DynamicFileSplitEnumerator(
            SplitEnumeratorContext<SplitT> context,
            DynamicFileEnumerator.Provider fileEnumeratorFactory,
            FileSplitAssigner.Provider splitAssignerFactory) {
        this.context = checkNotNull(context);
        this.splitAssignerFactory = checkNotNull(splitAssignerFactory);
        this.fileEnumeratorFactory = checkNotNull(fileEnumeratorFactory);
        this.assignedSplits = new HashSet<>();
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        if (splitAssigner == null) {
            // No DynamicFilteringData is received before the first split request,
            // create a split assigner that handles all splits
            createSplitAssigner(null);
        }

        if (LOG.isDebugEnabled()) {
            final String hostInfo =
                    hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
            LOG.debug("Subtask {} {} is requesting a file source split", subtask, hostInfo);
        }

        final Optional<FileSourceSplit> nextSplit = getNextUnassignedSplit(hostname);
        if (nextSplit.isPresent()) {
            final FileSourceSplit split = nextSplit.get();
            context.assignSplit((SplitT) split, subtask);
            assignedSplits.add(split.splitId());
            LOG.debug("Assigned split to subtask {} : {}", subtask, split);
        } else {
            context.signalNoMoreSplits(subtask);
            LOG.info("No more splits available for subtask {}", subtask);
        }
    }

    private Optional<FileSourceSplit> getNextUnassignedSplit(String hostname) {
        Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
        while (nextSplit.isPresent()) {
            FileSourceSplit split = nextSplit.get();
            // ignore the split if it has been assigned
            if (!assignedSplits.contains(split.splitId())) {
                return nextSplit;
            }
            nextSplit = splitAssigner.getNext(hostname);
        }
        return nextSplit;
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof DynamicFilteringEvent) {
            LOG.warn("Received DynamicFilteringEvent: {}", subtaskId);
            createSplitAssigner(((DynamicFilteringEvent) sourceEvent).getData());
        } else {
            LOG.error("Received unrecognized event: {}", sourceEvent);
        }
    }

    private void createSplitAssigner(@Nullable DynamicFilteringData dynamicFilteringData) {
        DynamicFileEnumerator fileEnumerator = fileEnumeratorFactory.create();
        if (dynamicFilteringData != null) {
            fileEnumerator.setDynamicFilteringData(dynamicFilteringData);
        }
        Collection<FileSourceSplit> splits;
        try {
            splits = fileEnumerator.enumerateSplits(new Path[1], context.currentParallelism());
            allEnumeratingSplits =
                    splits.stream().map(FileSourceSplit::splitId).collect(Collectors.toSet());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not enumerate file splits", e);
        }
        splitAssigner = splitAssignerFactory.create(splits);
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        LOG.debug("Dynamic File Source Enumerator adds splits back: {}", splits);
        if (splitAssigner != null) {
            List<FileSourceSplit> fileSplits = new ArrayList<>(splits);
            // Only add back splits enumerating. A split may be filtered after it is assigned.
            fileSplits.removeIf(s -> !allEnumeratingSplits.contains(s.splitId()));
            // Added splits should be removed from assignedSplits for re-assignment
            fileSplits.forEach(s -> assignedSplits.remove(s.splitId()));
            splitAssigner.addSplits(fileSplits);
        }
    }

    @Override
    public PendingSplitsCheckpoint<SplitT> snapshotState(long checkpointId) {
        throw new UnsupportedOperationException(
                "DynamicFileSplitEnumerator only supports batch execution.");
    }

    @Override
    public void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent) {
        // Only recognize events that don't care attemptNumber
        handleSourceEvent(subtaskId, sourceEvent);
    }
}
