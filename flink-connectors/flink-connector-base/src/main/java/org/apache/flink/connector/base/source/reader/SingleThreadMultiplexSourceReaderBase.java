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
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * A base for {@link SourceReader}s that read splits with one thread using one {@link SplitReader}.
 * The splits can be read either one after the other (like in a file source) or concurrently by
 * changing the subscription in the split reader (like in the Kafka Source).
 *
 * <p>To implement a source reader based on this class, implementors need to supply the following:
 *
 * <ul>
 *   <li>A {@link SplitReader}, which connects to the source and reads/polls data. The split reader
 *       gets notified whenever there is a new split. The split reader would read files, contain a
 *       Kafka or other source client, etc.
 *   <li>A {@link RecordEmitter} that takes a record from the Split Reader and updates the
 *       checkpointing state and converts it into the final form. For example for Kafka, the Record
 *       Emitter takes a {@code ConsumerRecord}, puts the offset information into state, transforms
 *       the records with the deserializers into the final type, and emits the record.
 *   <li>The class must override the methods to convert back and forth between the immutable splits
 *       ({@code SplitT}) and the mutable split state representation ({@code SplitStateT}).
 *   <li>Finally, the reader must decide what to do when it starts ({@link #start()}) or when a
 *       split is finished ({@link #onSplitFinished(java.util.Map)}).
 * </ul>
 *
 * @param <E> The type of the records (the raw type that typically contains checkpointing
 *     information).
 * @param <T> The final type of the records emitted by the source.
 * @param <SplitT> The type of the splits processed by the source.
 * @param <SplitStateT> The type of the mutable state per split.
 */
// 这个类的核心作用是：定义了一种“单线程多路复用”的读取模式。
// 在 Flink 的新 Source 架构中，数据读取通常分为“主线程（处理数据）”和“Fetcher 线程（拉取数据）”。
//单线程 (Single Thread)：意味着无论该 Reader 被分配了多少个分片（Splits），在后台只会运行一个 Fetcher 线程。
//多路复用 (Multiplex)：意味着这唯一的一个 Fetcher 线程能够同时处理或轮询多个分片的数据。
// 适用场景：
//Kafka Source：一个 KafkaConsumer 实例（在一个线程内）可以同时消费多个 Topic Partition。
//MySQL CDC：增量阶段通常只有一个 Binlog 读取线程，但它需要处理来自多个表（在分片算法中体现）的变更。
//文件系统：虽然文件通常一个接一个读，但也可以通过这个类来统一管理多个文件的读取序列。
// 如果你直接使用 SourceReaderBase，你需要自己决定是用一个线程还是多个线程去读。而通过继承 SingleThreadMultiplexSourceReaderBase，Flink 为你做好了以下封装：
//线程安全保证：它内部使用的 SingleThreadFetcherManager 确保了所有对外部系统的访问（如 Kafka Consumer 调用）都发生在同一个后台线程中，避免了多线程并发访问非线程安全客户端的问题。
//降低开销：对于很多数据源，开启大量线程并没有意义（如磁盘 I/O 或单连接网络），单线程多路复用能显著降低 TaskManager 的线程上下文切换开销。
//统一状态转换：它强制你通过继承来处理 SplitT（不可变分片）和 SplitStateT（可变状态）的转换，确保 Checkpoint 机制的正确性。
@PublicEvolving
public abstract class SingleThreadMultiplexSourceReaderBase<
                E, T, SplitT extends SourceSplit, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    /**
     * The primary constructor for the source reader.
     *
     * <p>The reader will use a handover queue sized as configured via {@link
     * SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                new SingleThreadFetcherManager<>(splitReaderSupplier, config),
                recordEmitter,
                config,
                context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue}.
     *
     * @deprecated Please use {@link #SingleThreadMultiplexSourceReaderBase(Supplier, RecordEmitter,
     *     Configuration, SourceReaderContext)} instead.
     */
    @Deprecated
    public SingleThreadMultiplexSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier, config),
                recordEmitter,
                config,
                context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue} and {@link SingleThreadFetcherManager}.
     *
     * @deprecated Please use {@link
     *     #SingleThreadMultiplexSourceReaderBase(SingleThreadFetcherManager, RecordEmitter,
     *     Configuration, SourceReaderContext)} instead.
     */
    @Deprecated
    public SingleThreadMultiplexSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue}, {@link SingleThreadFetcherManager} and {@link
     * RecordEvaluator}.
     *
     * @deprecated Please use {@link
     *     #SingleThreadMultiplexSourceReaderBase(SingleThreadFetcherManager, RecordEmitter,
     *     RecordEvaluator, Configuration, SourceReaderContext)} instead.
     */
    @Deprecated
    public SingleThreadMultiplexSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                splitFetcherManager,
                recordEmitter,
                eofRecordEvaluator,
                config,
                context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue} and {@link SingleThreadFetcherManager}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(splitFetcherManager, recordEmitter, config, context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue}, {@link SingleThreadFetcherManager} and {@link
     * RecordEvaluator}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        super(splitFetcherManager, recordEmitter, eofRecordEvaluator, config, context);
    }
}
