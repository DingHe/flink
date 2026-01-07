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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for a source reader which is responsible for reading the records from the source
 * splits assigned by {@link SplitEnumerator}.
 *
 * <p>For most non-trivial source reader, it is recommended to use {@link
 * org.apache.flink.connector.base.source.reader.SourceReaderBase SourceReaderBase} which provides
 * an efficient hand-over protocol to avoid blocking I/O inside the task thread and supports various
 * split-threading models.
 *
 * <p>Implementations can provide the following metrics:
 *
 * <ul>
 *   <li>{@link OperatorIOMetricGroup#getNumRecordsInCounter()} (highly recommended)
 *   <li>{@link OperatorIOMetricGroup#getNumBytesInCounter()} (recommended)
 *   <li>{@link SourceReaderMetricGroup#getNumRecordsInErrorsCounter()} (recommended)
 *   <li>{@link SourceReaderMetricGroup#setPendingRecordsGauge(Gauge)}
 *   <li>{@link SourceReaderMetricGroup#setPendingBytesGauge(Gauge)}
 * </ul>
 *
 * @param <T> The type of the record emitted by this source reader.
 * @param <SplitT> The type of the source splits.
 */
// SourceReader 接口是 Flink 统一 Source API 的核心执行组件，它运行在 Flink 的 TaskManager 上，负责实际读取分配给它的数据分片 (SourceSplit) 并将记录发送到下游算子。
// 数据读取和记录发射： 负责从外部数据源（如文件、Kafka、数据库）读取数据，并将记录 (T) 发射到 Flink 的数据流 (ReaderOutput) 中。
// 分片管理： 接收来自 SplitEnumerator 分配给它的 SourceSplit，并管理这些分片的读取进度。
// 非阻塞 I/O： 必须确保其核心读取方法 (pollNext) 是非阻塞的，以避免阻塞 TaskManager 的主 I/O 线程，这对于高性能和容错至关重要。
// 容错状态： 参与 Flink 的检查点机制，通过 snapshotState 保存其当前正在读取的分片状态。
// 对于大多数复杂的实现，官方推荐使用抽象基类 SourceReaderBase，因为它提供了高效的 I/O 握手协议，避免了在主线程中进行阻塞 I/O。
// <T>	记录类型	Source Reader 最终发射给下游算子的记录类型。
// <SplitT extends SourceSplit>	分片类型	Source Reader 处理的数据分片类型。
@Public
public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable, CheckpointListener {

    /** Start the reader. */
    // 启动读取器。
    // 在读取器初始化后被调用，用于启动任何必要的内部资源（如后台线程、连接等）。
    void start();

    /**
     * Poll the next available record into the {@link ReaderOutput}.
     *
     * <p>The implementation must make sure this method is non-blocking.
     *
     * <p>Although the implementation can emit multiple records into the given ReaderOutput, it is
     * recommended not doing so. Instead, emit one record into the ReaderOutput and return a {@link
     * InputStatus#MORE_AVAILABLE} to let the caller thread know there are more records available.
     *
     * @return The InputStatus of the SourceReader after the method invocation.
     */
    // 拉取下一条记录。
    // Flink 运行时线程反复调用此方法以获取数据。它必须是非阻塞的。
    // 它将读取到的记录通过 ReaderOutput 发射出去。返回值 (InputStatus) 告知运行时当前读取器状态：
    InputStatus pollNext(ReaderOutput<T> output) throws Exception;

    /**
     * Checkpoint on the state of the source.
     *
     * @return the state of the source.
     */
    // 创建状态快照。
    // 在 Flink 检查点触发时被调用。它必须返回一个列表，包含当前 Reader 正在处理的所有 Splits 及其最新的读取进度状态。这个状态用于 Job 恢复。
    List<SplitT> snapshotState(long checkpointId);

    /**
     * Returns a future that signals that data is available from the reader.
     *
     * <p>Once the future completes, the runtime will keep calling the {@link
     * #pollNext(ReaderOutput)} method until that method returns a status other than {@link
     * InputStatus#MORE_AVAILABLE}. After that, the runtime will again call this method to obtain
     * the next future. Once that completes, it will again call {@link #pollNext(ReaderOutput)} and
     * so on.
     *
     * <p>The contract is the following: If the reader has data available, then all futures
     * previously returned by this method must eventually complete. Otherwise the source might stall
     * indefinitely.
     *
     * <p>It is not a problem to have occasional "false positives", meaning to complete a future
     * even if no data is available. However, one should not use an "always complete" future in
     * cases no data is available, because that will result in busy waiting loops calling {@code
     * pollNext(...)} even though no data is available.
     *
     * @return a future that will be completed once there is a record available to poll.
     */
    // 数据可用性信号。
    // 返回一个 CompletableFuture<Void>。
    // 当此 Future 完成时，表示读取器可能有新的记录可供 pollNext 读取。
    // 运行时将等待此 Future 完成，然后再次调用 pollNext。
    // 这是实现非阻塞 I/O 的关键机制。
    CompletableFuture<Void> isAvailable();

    /**
     * Adds a list of splits for this reader to read. This method is called when the enumerator
     * assigns a split via {@link SplitEnumeratorContext#assignSplit(SourceSplit, int)} or {@link
     * SplitEnumeratorContext#assignSplits(SplitsAssignment)}.
     *
     * @param splits The splits assigned by the split enumerator.
     */
    // 添加分片。
    // 当 SplitEnumerator 将新的分片分配给此 Reader 时调用。
    // Reader 必须将这些 Splits 加入到其工作队列中。
    void addSplits(List<SplitT> splits);

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SplitEnumeratorContext#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    // 通知不再有分片。
    // 当 SplitEnumerator 确认此 Reader 不会再接收到任何新的 Splits（例如，有界 Source 的所有 Splits 都已分配完毕）时调用。
    // Reader 可以利用此信息准备结束输入。
    void notifyNoMoreSplits();

    /**
     * Handle a custom source event sent by the {@link SplitEnumerator}. This method is called when
     * the enumerator sends an event via {@link SplitEnumeratorContext#sendEventToSourceReader(int,
     * SourceEvent)}.
     *
     * <p>This method has a default implementation that does nothing, because most sources do not
     * require any custom events.
     *
     * @param sourceEvent the event sent by the {@link SplitEnumerator}.
     */
    // 处理 Source 事件。
    // 用于处理由 SplitEnumerator 发送的自定义 Source 事件。默认实现为空。
    default void handleSourceEvents(SourceEvent sourceEvent) {}

    /**
     * We have an empty default implementation here because most source readers do not have to
     * implement the method.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    // 通知检查点完成。
    // 默认实现为空。如果 Reader 需要知道特定的检查点已成功完成（例如，用于清理外部系统的临时状态），则可重写此方法。
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * Pauses or resumes reading of individual source splits.
     *
     * <p>Note that no other methods can be called in parallel, so updating subscriptions can be
     * done atomically. This method is simply providing connectors with more expressive APIs the
     * opportunity to update all subscriptions at once.
     *
     * <p>This is currently used to align the watermarks of splits, if watermark alignment is used
     * and the source reads from more than one split.
     *
     * <p>The default implementation throws an {@link UnsupportedOperationException} where the
     * default implementation will be removed in future releases. To be compatible with future
     * releases, it is recommended to implement this method and override the default implementation.
     *
     * @param splitsToPause the splits to pause
     * @param splitsToResume the splits to resume
     */
    // 暂停/恢复分片。
    // 用于水位线对齐（Watermark Alignment）。当 Flink 发现不同 splits 的水位线差异过大时，会调用此方法来暂停或恢复特定 splits 的读取，以帮助水位线对齐。
    // 默认实现抛出异常（即不支持此功能），并强烈建议实现者重写它以兼容未来的 Flink 版本。
    @PublicEvolving
    default void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        throw new UnsupportedOperationException(
                "This source reader does not support pausing or resuming splits which can lead to unaligned splits.\n"
                        + "Unaligned splits are splits where the output watermarks of the splits have diverged more than the allowed limit.\n"
                        + "It is highly discouraged to use unaligned source splits, as this leads to unpredictable\n"
                        + "watermark alignment if there is more than a single split per reader. It is recommended to implement pausing splits\n"
                        + "for this source. At your own risk, you can allow unaligned source splits by setting the\n"
                        + "configuration parameter `pipeline.watermark-alignment.allow-unaligned-source-splits' to true.\n"
                        + "Beware that this configuration parameter will be dropped in a future Flink release.");
    }
}
