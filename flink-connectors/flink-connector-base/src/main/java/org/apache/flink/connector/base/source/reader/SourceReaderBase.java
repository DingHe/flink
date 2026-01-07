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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract implementation of {@link SourceReader} which provides some synchronization between
 * the mail box main thread and the SourceReader internal threads. This class allows user to just
 * provide a {@link SplitReader} and snapshot the split state.
 *
 * <p>This implementation provides the following metrics out of the box:
 *
 * <ul>
 *   <li>{@link OperatorIOMetricGroup#getNumRecordsInCounter()}
 * </ul>
 *
 * @param <E> The rich element type that contains information for split state update or timestamp
 *     extraction.
 * @param <T> The final element type to emit.
 * @param <SplitT> the immutable split type.
 * @param <SplitStateT> the mutable type of split state.
 */
// 这个类解决了 Source 读取中最棘手的 “多线程同步” 和 “状态管理” 问题：
// 多线程解耦：它通过一个内部队列（Handover Queue），将“从外部系统拉取数据（Fetcher 线程）”和“向下游发送数据（Task 主线程/Mailbox 线程）”完全解耦。
// 通用生命周期管理：实现了分片的添加（addSplits）、快照（snapshotState）、处理 Source 事件等标准流程。
// 简化实现：开发者只需要实现极少量的抽象方法（如状态初始化、类型转换），并提供一个 SplitReader 即可工作。
// 内置指标与容错：自动处理了 Records In 计数等指标，并确保在 Checkpoint 时能正确保存分片状态。
// SourceReaderBase 就像是一个中央调度室：
//Fetcher 线程在外面（外部系统）采煤。
//采好的煤放入传送带（elementsQueue）。
//SourceReaderBase 负责在传送带末端捡煤，对照账本（splitStates），贴上标签（转换类型），发给工厂（算子下游）。

@PublicEvolving
public abstract class SourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
        implements SourceReader<T, SplitT> {
    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderBase.class);

    /** A queue to buffer the elements fetched by the fetcher thread. */
    // 核心缓冲队列。
    // 存放 Fetcher 线程读取到的数据批次。
    // 主线程从这里拿数据发往下游。
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

    /** The state of the splits. */
    // 分片状态跟踪。
    // 记录当前 Reader 负责的所有分片及其对应的可变状态（Mutable State）。
    private final Map<String, SplitContext<T, SplitStateT>> splitStates;

    /** The record emitter to handle the records read by the SplitReaders. */
    // 数据发射器。
    // 定义如何将读取到的原始记录 E 转换为输出类型 T 并发送。
    protected final RecordEmitter<E, T, SplitStateT> recordEmitter;

    /** The split fetcher manager to run split fetchers. */
    // 线程管理器。
    // 负责管理和启动实际跑在后台的 Fetcher 线程（数据拉取线程）。
    protected final SplitFetcherManager<E, SplitT> splitFetcherManager;

    /** The configuration for the reader. */
    // 读取配置。
    // 包含队列容量、超时时间等参数。
    protected final SourceReaderOptions options;

    /** The raw configurations that may be used by subclasses. */
    protected final Configuration config;
    // 性能指标计数器。
    // 统计输入记录的总数。
    private final Counter numRecordsInCounter;

    /** The context of this source reader. */
    // 上下文。
    // 提供 Metrics、配置信息以及向 Enumerator 发送事件的能力。
    protected SourceReaderContext context;

    /** The latest fetched batch of records-by-split from the split reader. */
    // 当前处理中的数据批次。
    // 保存了当前正在遍历的数据块，避免频繁去队列 poll。
    @Nullable private RecordsWithSplitIds<E> currentFetch;

    @Nullable private SplitContext<T, SplitStateT> currentSplitContext;
    @Nullable private SourceOutput<T> currentSplitOutput;

    /** Indicating whether the SourceReader will be assigned more splits or not. */
    // 分片分配结束标识。
    // 当 Enumerator 通知不再有新分片时设为 true。
    private boolean noMoreSplitsAssignment;

    @Nullable protected final RecordEvaluator<T> eofRecordEvaluator;

    /**
     * @deprecated Please use {@link #SourceReaderBase(SplitFetcherManager, RecordEmitter,
     *     Configuration, SourceReaderContext)} instead.
     */
    @Deprecated
    public SourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this(elementsQueue, splitFetcherManager, recordEmitter, null, config, context);
    }

    /**
     * @deprecated Please use {@link #SourceReaderBase(SplitFetcherManager, RecordEmitter,
     *     RecordEvaluator, Configuration, SourceReaderContext)} instead.
     */
    @Deprecated
    public SourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        this.elementsQueue = elementsQueue;
        this.splitFetcherManager = splitFetcherManager;
        this.recordEmitter = recordEmitter;
        this.splitStates = new HashMap<>();
        this.options = new SourceReaderOptions(config);
        this.config = config;
        this.context = context;
        this.noMoreSplitsAssignment = false;
        this.eofRecordEvaluator = eofRecordEvaluator;

        numRecordsInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    /**
     * The primary constructor for the source reader.
     *
     * <p>The reader will use a handover queue sized as configured via {@link
     * SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}.
     */
    public SourceReaderBase(
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this(splitFetcherManager, recordEmitter, null, config, context);
    }

    public SourceReaderBase(
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        this.elementsQueue = splitFetcherManager.getQueue();
        this.splitFetcherManager = splitFetcherManager;
        this.recordEmitter = recordEmitter;
        this.splitStates = new HashMap<>();
        this.options = new SourceReaderOptions(config);
        this.config = config;
        this.context = context;
        this.noMoreSplitsAssignment = false;
        this.eofRecordEvaluator = eofRecordEvaluator;

        numRecordsInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    @Override
    public void start() {}
    // 由 Flink 框架循环调用的最核心方法，负责向算子下游发送数据。
    // 检查 currentFetch 是否还有数据，如果没有则从 elementsQueue 拿新批次。
    // 调用 recordEmitter 将数据发往下游。返回 MORE_AVAILABLE 或 NOTHING_AVAILABLE 告诉框架下次何时再来。
    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        // make sure we have a fetch we are working on, or move to the next
        RecordsWithSplitIds<E> recordsWithSplitId = this.currentFetch;
        if (recordsWithSplitId == null) {
            recordsWithSplitId = getNextFetch(output);
            if (recordsWithSplitId == null) {
                return trace(finishedOrAvailableLater());
            }
        }

        // we need to loop here, because we may have to go across splits
        while (true) {
            // Process one record.
            final E record = recordsWithSplitId.nextRecordFromSplit();
            if (record != null) {
                // emit the record.
                numRecordsInCounter.inc(1);
                recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
                LOG.trace("Emitted record: {}", record);

                // We always emit MORE_AVAILABLE here, even though we do not strictly know whether
                // more is available. If nothing more is available, the next invocation will find
                // this out and return the correct status.
                // That means we emit the occasional 'false positive' for availability, but this
                // saves us doing checks for every record. Ultimately, this is cheaper.
                return trace(InputStatus.MORE_AVAILABLE);
            } else if (!moveToNextSplit(recordsWithSplitId, output)) {
                // The fetch is done and we just discovered that and have not emitted anything, yet.
                // We need to move to the next fetch. As a shortcut, we call pollNext() here again,
                // rather than emitting nothing and waiting for the caller to call us again.
                return pollNext(output);
            }
        }
    }

    private InputStatus trace(InputStatus status) {
        LOG.trace("Source reader status: {}", status);
        return status;
    }
    // 从 elementsQueue 尝试获取下一个数据批次
    @Nullable
    private RecordsWithSplitIds<E> getNextFetch(final ReaderOutput<T> output) {
        splitFetcherManager.checkErrors();

        LOG.trace("Getting next source data batch from queue");
        final RecordsWithSplitIds<E> recordsWithSplitId = elementsQueue.poll();
        if (recordsWithSplitId == null || !moveToNextSplit(recordsWithSplitId, output)) {
            // No element available, set to available later if needed.
            return null;
        }

        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }
    // 当一个批次处理完时，清理当前上下文，
    // 并处理那些在批次中标识为“已读取完成”的分片（调用 onSplitFinished）
    private void finishCurrentFetch(
            final RecordsWithSplitIds<E> fetch, final ReaderOutput<T> output) {
        currentFetch = null;
        currentSplitContext = null;
        currentSplitOutput = null;

        final Set<String> finishedSplits = fetch.finishedSplits();
        if (!finishedSplits.isEmpty()) {
            LOG.info("Finished reading split(s) {}", finishedSplits);
            Map<String, SplitStateT> stateOfFinishedSplits = new HashMap<>();
            for (String finishedSplitId : finishedSplits) {
                stateOfFinishedSplits.put(
                        finishedSplitId, splitStates.remove(finishedSplitId).state);
                output.releaseOutputForSplit(finishedSplitId);
            }
            onSplitFinished(stateOfFinishedSplits);
        }

        fetch.recycle();
    }
    // 在一个数据批次内，切换到下一个分片（因为一个 Batch 可能包含多个分片的数据）。
    private boolean moveToNextSplit(
            RecordsWithSplitIds<E> recordsWithSplitIds, ReaderOutput<T> output) {
        final String nextSplitId = recordsWithSplitIds.nextSplit();
        if (nextSplitId == null) {
            LOG.trace("Current fetch is finished.");
            finishCurrentFetch(recordsWithSplitIds, output);
            return false;
        }

        currentSplitContext = splitStates.get(nextSplitId);
        checkState(currentSplitContext != null, "Have records for a split that was not registered");

        Function<T, Boolean> eofRecordHandler = null;
        if (eofRecordEvaluator != null) {
            eofRecordHandler =
                    record -> {
                        if (!eofRecordEvaluator.isEndOfStream(record)) {
                            return false;
                        }
                        SplitT split =
                                toSplitType(currentSplitContext.splitId, currentSplitContext.state);
                        splitFetcherManager.removeSplits(Collections.singletonList(split));
                        return true;
                    };
        }
        currentSplitOutput = currentSplitContext.getOrCreateSplitOutput(output, eofRecordHandler);
        LOG.trace("Emitting records from fetch for split {}", nextSplitId);
        return true;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return currentFetch != null
                ? FutureCompletingBlockingQueue.AVAILABLE
                : elementsQueue.getAvailabilityFuture();
    }
    // Checkpoint 触发时执行。
    // 遍历 splitStates，调用 toSplitType 将可变状态转为不可变的 Split 对象返回，存入状态后端。
    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        List<SplitT> splits = new ArrayList<>();
        splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
        return splits;
    }
    // 当 Enumerator 分配了新分片时调用。
    @Override
    public void addSplits(List<SplitT> splits) {
        LOG.info("Adding split(s) to reader: {}", splits);
        // Initialize the state for each split.
        splits.forEach(
                s ->
                        splitStates.put(
                                s.splitId(), new SplitContext<>(s.splitId(), initializedState(s))));
        // Hand over the splits to the split fetcher to start fetch.
        splitFetcherManager.addSplits(splits);
    }
    // 标记此 Reader 以后不会再收到新分片了，是判断 END_OF_INPUT 的前提条件。
    @Override
    public void notifyNoMoreSplits() {
        LOG.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
        elementsQueue.notifyAvailable();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.info("Received unhandled source event: {}", sourceEvent);
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        splitFetcherManager.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Source Reader.");
        splitFetcherManager.close(options.sourceReaderCloseTimeout);
    }

    /**
     * Gets the number of splits the reads has currently assigned.
     *
     * <p>These are the splits that have been added via {@link #addSplits(List)} and have not yet
     * been finished by returning them from the {@link SplitReader#fetch()} as part of {@link
     * RecordsWithSplitIds#finishedSplits()}.
     */
    public int getNumberOfCurrentlyAssignedSplits() {
        return splitStates.size();
    }

    // -------------------- Abstract method to allow different implementations ------------------

    /** Handles the finished splits to clean the state if needed. */
    protected abstract void onSplitFinished(Map<String, SplitStateT> finishedSplitIds);

    /**
     * When new splits are added to the reader. The initialize the state of the new splits.
     *
     * @param split a newly added split.
     */
    protected abstract SplitStateT initializedState(SplitT split);

    /**
     * Convert a mutable SplitStateT to immutable SplitT.
     *
     * @param splitState splitState.
     * @return an immutable Split state.
     */
    protected abstract SplitT toSplitType(String splitId, SplitStateT splitState);

    // ------------------ private helper methods ---------------------

    private InputStatus finishedOrAvailableLater() {
        final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (elementsQueue.isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            splitFetcherManager.checkErrors();
            return InputStatus.END_OF_INPUT;
        } else {
            // We can reach this case if we just processed all data from the queue and finished a
            // split,
            // and concurrently the fetcher finished another split, whose data is then in the queue.
            return InputStatus.MORE_AVAILABLE;
        }
    }

    // ------------------ private helper classes ---------------------

    private static final class SplitContext<T, SplitStateT> {

        final String splitId;
        final SplitStateT state;
        @Nullable SourceOutput<T> sourceOutput;

        private SplitContext(String splitId, SplitStateT state) {
            this.state = state;
            this.splitId = splitId;
        }

        SourceOutput<T> getOrCreateSplitOutput(
                ReaderOutput<T> mainOutput, @Nullable Function<T, Boolean> eofRecordHandler) {
            if (sourceOutput == null) {
                // The split output should have been created when AddSplitsEvent was processed in
                // SourceOperator. Here we just use this method to get the previously created
                // output.
                sourceOutput = mainOutput.createOutputForSplit(splitId);
                if (eofRecordHandler != null) {
                    sourceOutput = new SourceOutputWrapper<>(sourceOutput, eofRecordHandler);
                }
            }
            return sourceOutput;
        }
    }

    /** This output will stop sending records after receiving the eof record. */
    private static final class SourceOutputWrapper<T> implements SourceOutput<T> {
        final SourceOutput<T> sourceOutput;
        final Function<T, Boolean> eofRecordHandler;

        private boolean isStreamEnd = false;

        public SourceOutputWrapper(
                SourceOutput<T> sourceOutput, Function<T, Boolean> eofRecordHandler) {
            this.sourceOutput = sourceOutput;
            this.eofRecordHandler = eofRecordHandler;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            sourceOutput.emitWatermark(watermark);
        }

        @Override
        public void markIdle() {
            sourceOutput.markIdle();
        }

        @Override
        public void markActive() {
            sourceOutput.markActive();
        }

        @Override
        public void collect(T record) {
            if (!isEndOfStreamReached(record)) {
                sourceOutput.collect(record);
            }
        }

        @Override
        public void collect(T record, long timestamp) {
            if (!isEndOfStreamReached(record)) {
                sourceOutput.collect(record, timestamp);
            }
        }

        /**
         * Judge and handle the eof record.
         *
         * @return whether the record is the eof record.
         */
        private boolean isEndOfStreamReached(T record) {
            if (isStreamEnd) {
                return true;
            }
            if (eofRecordHandler.apply(record)) {
                isStreamEnd = true;
            }
            return isStreamEnd;
        }
    }
}
