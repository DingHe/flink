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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
// 管理、调度、和实现从所有输入通道异步拉取数据和事件的整个流程，是 Flink 任务真正从上游接收数据的网关
// 资源管理： 管理用于接收数据的 BufferPool 和 MemorySegment。
// 通道管理与动态更新： 维护所有 InputChannel 实例，处理它们的初始化、从恢复通道（RecoveredInputChannel）到真实通道（Local/Remote）的转换，以及处理上游生产者的状态更新。
// 调度与拉取： 通过 PrioritizedDeque 队列和位图（BitSet）高效地追踪哪些通道有数据可用，并提供 pollNext() 和 getNext() 方法来消费 BufferOrEvent。
public class SingleInputGate extends IndexedInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

    /** Lock object to guard partition requests and runtime channel updates. */
    // 请求锁。
    // 用于同步对分区请求和运行时通道更新操作的访问，确保线程安全。
    private final Object requestLock = new Object();

    /** The name of the owning task, for logging purposes. */
    // 所属任务名称。
    // 用于日志记录和调试，标识该网关属于哪个 Flink Task。
    private final String owningTaskName;

    // 网关索引。
    // 该网关在其所属任务中的唯一索引。
    private final int gateIndex;

    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    // 消费结果 ID。
    // 标识该网关消费的中间结果数据集的 ID。
    private final IntermediateDataSetID consumedResultId;

    /** The type of the partition the input gate is consuming. */
    // 消费分区类型。 指定该网关消费的分区类型（如 Pipelined 或 Blocking）。
    private final ResultPartitionType consumedPartitionType;

    /** The number of input channels (equivalent to the number of consumed partitions). */
    // 输入通道数量。
    // 该网关期望连接和管理的输入通道总数。
    private final int numberOfInputChannels;

    /** Input channels. We store this in a map for runtime updates of single channels. */
    // 按分区 ID 组织的通道集合。
    // 用于运行时通道查找和更新，支持一个分区对应多个输入通道（在某些恢复场景中）
    private final Map<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
            inputChannels;
    // 通道数组。
    // 按索引顺序存储所有 InputChannel 实例。这是最常用的通道查找结构。
    @GuardedBy("requestLock")
    private final InputChannel[] channels;

    /** Channels, which notified this input gate about available data. */
    // 有数据通道队列。
    // 一个双端队列，用于存放报告有数据可用的 InputChannel，并根据优先级（例如是否有优先级事件）进行调度。
    private final PrioritizedDeque<InputChannel> inputChannelsWithData = new PrioritizedDeque<>();

    /**
     * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be
     * unified onto one.
     */
    // 已入队通道位图。
    // 用于快速检查某个通道是否已存在于 inputChannelsWithData 队列中，以确保通道不会被重复排队。
    @GuardedBy("inputChannelsWithData")
    private final BitSet enqueuedInputChannelsWithData;

    // 已经收到 EndOfPartitionEvent 的通道数量（即已完成通道数）。
    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfPartitionEvents;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfUserRecords;

    @GuardedBy("inputChannelsWithData")
    private int[] lastPrioritySequenceNumber;



    /** The partition producer state listener. */
    private final PartitionProducerStateProvider partitionProducerStateProvider;

    /**
     * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
     * from this pool.
     */
    // 缓冲区池。
    // 用于从远程通道接收数据时，分配内存 Segment 以存储传入的数据。
    private BufferPool bufferPool;
    // 全部分区结束标志。
    // 当所有输入通道都收到 EndOfPartitionEvent 时，设置为 true，表示输入已完成（isFinished()）
    private boolean hasReceivedAllEndOfPartitionEvents;
    // 数据结束标志。
    // 当所有输入通道都收到 EndOfData 事件时，设置为 true
    private boolean hasReceivedEndOfData;

    /** Flag indicating whether partitions have been requested. */
    // 如果为 false，则表明这是第一次请求数据
    private boolean requestedPartitionsFlag;

    private final List<TaskEvent> pendingEvents = new ArrayList<>();

    private int numberOfUninitializedChannels;

    /** A timer to retrigger local partition requests. Only initialized if actually needed. */
    private Timer retriggerLocalRequestTimer;
    // BufferPool构建工厂
    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    // 关闭 Future。
    // 当该网关被关闭 (close()) 时，该 Future 完成，用于通知依赖于网关关闭的外部组件。
    private final CompletableFuture<Void> closeFuture;

    @Nullable private final BufferDecompressor bufferDecompressor;

    private final MemorySegmentProvider memorySegmentProvider;

    /**
     * The segment to read data from file region of bounded blocking partition by local input
     * channel.
     */
    private final MemorySegment unpooledSegment;
    // 吞吐量计算器。
    // 用于测量当前的输入数据吞吐量，为 BufferDebloater 提供输入。
    private final ThroughputCalculator throughputCalculator;
    // 缓冲区去膨胀器。
    // 流量控制组件，用于根据吞吐量和延迟动态调整输入缓冲区的大小，优化性能
    private final BufferDebloater bufferDebloater;

    // 是否应耗尽数据。
    // 与 EndOfData 状态配合使用，指示在收到结束信号后是否应该耗尽（drain）剩余的在途数据。
    private boolean shouldDrainOnEndOfData = true;

    // The consumer client will be null if the tiered storage is not enabled.
    @Nullable private TieredStorageConsumerClient tieredStorageConsumerClient;

    // The consumer specs in tiered storage will be null if the tiered storage is not enabled.
    @Nullable private List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs;

    // The availability notifier will be null if the tiered storage is not enabled.
    @Nullable private AvailabilityNotifier availabilityNotifier;

    /**
     * A map containing the status of the last consumed buffer in each input channel. The status
     * contains the following information: 1) whether the buffer contains partial record, and 2) the
     * index of the subpartition where the buffer comes from.
     */
    private final Map<Integer, Tuple2<Boolean, Integer>> lastBufferStatusMapInTieredStore =
            new HashMap<>();

    /** A map of counters for the number of {@link EndOfData}s received from each input channel. */
    // 用于记录每个通道收到的结束标志次数
    private final int[] endOfDatas;

    /**
     * A map of counters for the number of {@link EndOfPartitionEvent}s received from each input
     * channel.
     */
    private final int[] endOfPartitions;

    public SingleInputGate(
            String owningTaskName,
            int gateIndex,
            IntermediateDataSetID consumedResultId,
            final ResultPartitionType consumedPartitionType,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= gateIndex, "The gate index must be positive.");
        this.gateIndex = gateIndex;

        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.bufferPoolFactory = checkNotNull(bufferPoolFactory);

        checkArgument(numberOfInputChannels > 0);
        this.numberOfInputChannels = numberOfInputChannels;

        this.inputChannels = CollectionUtil.newHashMapWithExpectedSize(numberOfInputChannels);
        this.channels = new InputChannel[numberOfInputChannels];
        this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
        this.channelsWithEndOfUserRecords = new BitSet(numberOfInputChannels);
        this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);
        this.lastPrioritySequenceNumber = new int[numberOfInputChannels];
        Arrays.fill(lastPrioritySequenceNumber, Integer.MIN_VALUE);

        this.partitionProducerStateProvider = checkNotNull(partitionProducerStateProvider);

        this.bufferDecompressor = bufferDecompressor;
        this.memorySegmentProvider = checkNotNull(memorySegmentProvider);

        this.closeFuture = new CompletableFuture<>();

        this.unpooledSegment = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
        this.bufferDebloater = bufferDebloater;
        this.throughputCalculator = checkNotNull(throughputCalculator);

        this.tieredStorageConsumerClient = null;
        this.tieredStorageConsumerSpecs = null;
        this.availabilityNotifier = null;

        this.endOfDatas = new int[numberOfInputChannels];
        Arrays.fill(endOfDatas, 0);
        this.endOfPartitions = new int[numberOfInputChannels];
        Arrays.fill(endOfPartitions, 0);
    }

    protected PrioritizedDeque<InputChannel> getInputChannelsWithData() {
        return inputChannelsWithData;
    }
    // 负责初始化和设置所有接收数据所需的资源，例如缓冲区池和底层通道
    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: Already registered buffer pool.");

        BufferPool bufferPool = bufferPoolFactory.get();
        setBufferPool(bufferPool);
        // 判断 Flink 是否启用了分层存储（Tiered Storage）或外部存储的消费功能。
        if (tieredStorageConsumerClient != null) {
            tieredStorageConsumerClient.setup(bufferPool);
        }

        setupChannels();
    }
    // 等待所有输入通道的状态数据（Channel State）被完全消费和处理完毕。
    // 这确保了在任务开始处理新的数据记录之前，所有必需的通道状态都已就绪。
    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        synchronized (requestLock) {
            // 用于存储所有需要等待完成的 CompletableFuture 实例
            List<CompletableFuture<?>> futures = new ArrayList<>(numberOfInputChannels);
            for (InputChannel inputChannel : inputChannels()) {
                if (inputChannel instanceof RecoveredInputChannel) {
                    // RecoveredInputChannel 是一个特殊的通道类型，它代表了从检查点恢复状态的输入通道。只有需要恢复状态的通道才会有等待 Future。
                    futures.add(((RecoveredInputChannel) inputChannel).getStateConsumedFuture());
                }
            }
            // 将 futures 列表中的所有 Future 组合成一个单一的 CompletableFuture<Void>
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }
    }
    // Flink 数据流启动的顶层入口点。
    // 它是任务开始接收和处理来自上游任务的数据之前，所执行的协调和请求工作
    @Override
    public void requestPartitions() {
        synchronized (requestLock) {
            // requestedPartitionsFlag 标志。如果为 false，则表明这是第一次请求数据，允许进入同步逻辑。
            if (!requestedPartitionsFlag) {
                if (closeFuture.isDone()) {
                    throw new IllegalStateException("Already released.");
                }

                // Sanity checks
                // 检查通道数量一致性
                long numInputChannels =
                        inputChannels.values().stream().mapToLong(x -> x.values().size()).sum();
                if (numberOfInputChannels != numInputChannels) {
                    throw new IllegalStateException(
                            String.format(
                                    "Bug in input gate setup logic: mismatch between "
                                            + "number of total input channels [%s] and the currently set number of input "
                                            + "channels [%s].",
                                    numInputChannels, numberOfInputChannels));
                }

                convertRecoveredInputChannels();
                internalRequestPartitions();
            }

            requestedPartitionsFlag = true;
            // Start the reader only when all InputChannels have been converted to either
            // LocalInputChannel or RemoteInputChannel, as this will prevent RecoveredInputChannels
            // from being queued again.
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.start();
            }
        }
    }
    // 在 Flink 任务从检查点或保存点恢复时，输入通道（Input Channels）最初会被特殊的 RecoveredInputChannel 对象替代。
    // 这些对象负责从状态句柄（State Handle）中读取和恢复通道状态数据（即在检查点时刻被截断的流式数据）
    // 当所有的通道状态数据（来自检查点）都被成功读取和消费完毕后，convertRecoveredInputChannels 方法就会被调用
    // 核心作用是：
    // 将临时的 RecoveredInputChannel 实例，替换为其对应的、真实的、能够从上游接收数据的 InputChannel（如 RemoteInputChannel 或 LocalInputChannel）实例
    @VisibleForTesting
    public void convertRecoveredInputChannels() {
        LOG.debug("Converting recovered input channels ({} channels)", getNumberOfInputChannels());
        for (Map<InputChannelInfo, InputChannel> inputChannelsForCurrentPartition :
                inputChannels.values()) {
            // 获取旧通道信息
            Set<InputChannelInfo> oldInputChannelInfos =
                    new HashSet<>(inputChannelsForCurrentPartition.keySet());
            for (InputChannelInfo inputChannelInfo : oldInputChannelInfos) {
                InputChannel inputChannel = inputChannelsForCurrentPartition.get(inputChannelInfo);
                // 判断当前通道是否是临时的 RecoveredInputChannel。只有这种通道才需要被转换为“真实”的通道。
                if (inputChannel instanceof RecoveredInputChannel) {
                    try {
                        // 负责创建并返回该通道对应的真实 InputChannel 实例（例如 RemoteInputChannel），同时会清理 RecoveredInputChannel 自身的状态
                        InputChannel realInputChannel =
                                ((RecoveredInputChannel) inputChannel).toInputChannel();
                        inputChannel.releaseAllResources();
                        inputChannelsForCurrentPartition.remove(inputChannelInfo);
                        inputChannelsForCurrentPartition.put(
                                realInputChannel.getChannelInfo(), realInputChannel);
                        channels[inputChannel.getChannelIndex()] = realInputChannel;
                    } catch (Throwable t) {
                        inputChannel.setError(t);
                        return;
                    }
                }
            }
        }
    }
    // 核心作用是启动数据流，即向所有上游任务对应的输入通道（Input Channels）发出请求，要求它们开始发送数据。
    // 在 Flink 的数据流模型中，下游任务不会自动接收数据，而是由下游的 InputGate 主动向上游请求数据分区（Subpartitions）
    // 目标： 确保 SingleInputGate 管理的每个输入通道都向上游的 ResultPartition 发送请求，从而建立数据传输连接并开始接收流数据。
    private void internalRequestPartitions() {
        for (InputChannel inputChannel : inputChannels()) {
            try {
                // requestSubpartitions() 方法。
                // 对于 RemoteInputChannel：它会通过网络连接向远程的 TaskManager 发送一个请求消息，要求上游任务的 ResultPartition 开始发送数据。
                // 对于 LocalInputChannel：它会向本地的 TaskManager 发送一个请求，要求本地的 ResultPartition 开始发送数据
                inputChannel.requestSubpartitions();
            } catch (Throwable t) {
                inputChannel.setError(t);
                return;
            }
        }
    }
    // 完成状态恢复
    @Override
    public void finishReadRecoveredState() throws IOException {
        for (final InputChannel channel : channels) {
            if (channel instanceof RecoveredInputChannel) {
                ((RecoveredInputChannel) channel).finishReadRecoveredState();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    @Override
    public int getGateIndex() {
        return gateIndex;
    }
    // 用于在 Flink 任务执行的数据消费阶段（通常是 Task 运行结束前或需要检查数据完整性时），
    // 找出哪些输入通道还没有接收到 “分区结束事件”（EndOfPartitionEvent）
    // “分区结束事件” 标志着一个上游任务的数据分区已经完全发送完毕。因此，这个方法返回的列表就是当前仍在等待上游数据到达的通道集合。
    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        // 初始化列表
        List<InputChannelInfo> unfinishedChannels =
                new ArrayList<>(
                        numberOfInputChannels - channelsWithEndOfPartitionEvents.cardinality());
        synchronized (inputChannelsWithData) {
            // 从索引 0 开始查找第一个未设置（即 false）的位
            for (int i = channelsWithEndOfPartitionEvents.nextClearBit(0);
                    i < numberOfInputChannels;
                    i = channelsWithEndOfPartitionEvents.nextClearBit(i + 1)) {
                unfinishedChannels.add(getChannel(i).getChannelInfo());
            }
        }

        return unfinishedChannels;
    }
    // 计算并返回当前整个输入网关（Input Gate）正在使用的网络缓冲区（Network Buffers）的总数量
    // 反压（Backpressure）机制： 缓冲区数量是实现反压的关键指标之一。
    // 如果占用的缓冲区过多，InputGate 可能会停止向上游请求数据，从而减缓数据流入，防止内存耗尽
    @VisibleForTesting
    int getBuffersInUseCount() {
        int total = 0;
        for (InputChannel channel : channels) {
            total += channel.getBuffersInUseCount();
        }
        return total;
    }
    // 将一个新的网络缓冲区大小 (newBufferSize) 通知给 InputGate 所管理的所有活动（未释放）的输入通道。
    // 核心功能： 遍历所有输入通道，并将新的缓冲区大小参数传递给它们。
    @VisibleForTesting
    public void announceBufferSize(int newBufferSize) {
        for (InputChannel channel : channels) {
            if (!channel.isReleased()) {
                channel.announceBufferSize(newBufferSize);
            }
        }
    }
    // Flink 网络栈中实现**运行时动态调整内存缓冲区大小（Buffer Size Debloating）**的关键逻辑
    // 基于当前的任务吞吐量（Throughput）和内存使用情况，智能地计算并应用一个优化的网络缓冲区大小。
    @Override
    public void triggerDebloating() {
        if (isFinished() || closeFuture.isDone()) {
            return;
        }

        checkState(bufferDebloater != null, "Buffer debloater should not be null");
        final long currentThroughput = throughputCalculator.calculateThroughput();
        bufferDebloater
                .recalculateBufferSize(currentThroughput, getBuffersInUseCount())
                .ifPresent(this::announceBufferSize);
    }

    public Duration getLastEstimatedTimeToConsume() {
        return bufferDebloater.getLastEstimatedTimeToConsumeBuffers();
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    BufferProvider getBufferProvider() {
        return bufferPool;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    MemorySegmentProvider getMemorySegmentProvider() {
        return memorySegmentProvider;
    }

    public String getOwningTaskName() {
        return owningTaskName;
    }

    // 计算并返回当前 SingleInputGate 所有输入通道中排队等待被消费的网络缓冲区（Network Buffer）的总数量
    // 排队的缓冲区（Queued Buffers）： 指的是已经被 InputChannel 从上游接收到，但尚未被下游任务逻辑（例如算子）从 InputGate 中拉取出来并处理的内存缓冲区。
    // 重要性： 这个指标是衡量 Flink 网络延迟和**数据积压（Backlog）**的重要指标。
    // 如果这个数字很大，通常意味着下游任务处理速度跟不上上游生产速度，可能存在反压（Backpressure）
    public int getNumberOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                int totalBuffers = 0;

                for (InputChannel channel : inputChannels()) {
                    totalBuffers += channel.unsynchronizedGetNumberOfQueuedBuffers();
                }

                return totalBuffers;
            } catch (Exception ex) {
                LOG.debug("Fail to get number of queued buffers :", ex);
            }
        }

        return 0;
    }
    // 计算并返回当前 SingleInputGate 所有输入通道中排队等待被消费的网络缓冲区的总内存大小**（以字节为单位）
    // 排队的缓冲区： 是指已从上游接收，但尚未被下游任务拉取和处理的内存块
    // 重要性： 这个指标直接反映了当前 InputGate 占用了多少网络内存来存储等待处理的数据。
    // 它是衡量数据积压（Backlog）和潜在反压（Backpressure）的更精确指标，因为它考虑了每个缓冲区的实际大小（而非仅仅是数量）
    public long getSizeOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                long totalSize = 0;

                for (InputChannel channel : inputChannels()) {
                    totalSize += channel.unsynchronizedGetSizeOfQueuedBuffers();
                }

                return totalSize;
            } catch (Exception ex) {
                LOG.debug("Fail to get size of queued buffers :", ex);
            }
        }

        return 0;
    }

    public CompletableFuture<Void> getCloseFuture() {
        return closeFuture;
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        return channels[channelIndex];
    }

    // ------------------------------------------------------------------------
    // Setup/Life-cycle
    // ------------------------------------------------------------------------
    // 设置该输入网关使用的缓冲区池（BufferPool）
    public void setBufferPool(BufferPool bufferPool) {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: buffer pool has"
                        + "already been set for this input gate.");

        this.bufferPool = checkNotNull(bufferPool);
    }

    /** Assign the exclusive buffers to all remote input channels directly for credit-based mode. */
    // 主要负责资源预分配和通道初始化
    @VisibleForTesting
    public void setupChannels() throws IOException {
        // Allocate enough exclusive and floating buffers to guarantee that job can make progress.
        // Note: An exception will be thrown if there is no buffer available in the given timeout.

        // First allocate a single floating buffer to avoid potential deadlock when the exclusive
        // buffer is 0. See FLINK-24035 for more information.
        bufferPool.reserveSegments(1);

        // Next allocate the exclusive buffers per channel when the number of exclusive buffer is
        // larger than 0.
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.setup();
            }
        }
    }

    // 将一组新的输入通道（InputChannel）分配给这个 InputGate，并更新其内部的通道管理结构

    public void setInputChannels(InputChannel... channels) {
        if (channels.length != numberOfInputChannels) {
            throw new IllegalArgumentException(
                    "Expected "
                            + numberOfInputChannels
                            + " channels, "
                            + "but got "
                            + channels.length);
        }
        synchronized (requestLock) {
            System.arraycopy(channels, 0, this.channels, 0, numberOfInputChannels);
            for (InputChannel inputChannel : channels) {
                if (inputChannels
                                        .computeIfAbsent(
                                                inputChannel.getPartitionId().getPartitionId(),
                                                ignored -> new HashMap<>())
                                        .put(inputChannel.getChannelInfo(), inputChannel)
                                == null
                        && inputChannel instanceof UnknownInputChannel) {

                    numberOfUninitializedChannels++;
                }
            }
        }
    }

    public void setTieredStorageService(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageConsumerClient client,
            TieredStorageNettyServiceImpl nettyService) {
        this.tieredStorageConsumerSpecs = tieredStorageConsumerSpecs;
        this.tieredStorageConsumerClient = client;
        if (client != null) {
            this.availabilityNotifier = new AvailabilityNotifierImpl();
            setupTieredStorageNettyService(nettyService, tieredStorageConsumerSpecs);
            client.registerAvailabilityNotifier(availabilityNotifier);
        }
    }
    // 处理了一个关键的运行时场景：将临时的“未知通道” (UnknownInputChannel) 替换为真正的本地或远程通道
    // 在 Flink 任务启动时，下游 TaskManager 可能并不知道上游任务（即数据源）最终会被调度到哪个 TaskManager 上。
    // 因此，它会为这些上游任务的数据流创建临时的 UnknownInputChannel 占位符。
    // updateInputChannel 方法在 JobManager 告诉下游 TaskManager 上游任务的位置信息（通过 NettyShuffleDescriptor）时被调用。
    public void updateInputChannel(
            ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor)
            throws IOException, InterruptedException {
        synchronized (requestLock) {
            if (closeFuture.isDone()) {
                // There was a race with a task failure/cancel
                return;
            }

            IntermediateResultPartitionID partitionId =
                    shuffleDescriptor.getResultPartitionID().getPartitionId();

            Map<InputChannelInfo, InputChannel> newInputChannels = new HashMap<>();
            for (InputChannel current : inputChannels.get(partitionId).values()) {
                if (current instanceof UnknownInputChannel) {
                    UnknownInputChannel unknownChannel = (UnknownInputChannel) current;
                    boolean isLocal = shuffleDescriptor.isLocalTo(localLocation);
                    InputChannel newChannel;
                    if (isLocal) {
                        newChannel =
                                unknownChannel.toLocalInputChannel(
                                        shuffleDescriptor.getResultPartitionID());
                    } else {
                        RemoteInputChannel remoteInputChannel =
                                unknownChannel.toRemoteInputChannel(
                                        shuffleDescriptor.getConnectionId(),
                                        shuffleDescriptor.getResultPartitionID());
                        remoteInputChannel.setup();
                        newChannel = remoteInputChannel;
                    }
                    LOG.debug(
                            "{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

                    newInputChannels.put(newChannel.getChannelInfo(), newChannel);
                    channels[current.getChannelIndex()] = newChannel;

                    if (requestedPartitionsFlag) {
                        newChannel.requestSubpartitions();
                    }

                    for (TaskEvent event : pendingEvents) {
                        newChannel.sendTaskEvent(event);
                    }

                    if (--numberOfUninitializedChannels == 0) {
                        pendingEvents.clear();
                    }
                    if (enabledTieredStorage()) {
                        TieredStoragePartitionId tieredStoragePartitionId =
                                TieredStorageIdMappingUtils.convertId(
                                        shuffleDescriptor.getResultPartitionID());
                        TieredStorageConsumerSpec spec =
                                checkNotNull(tieredStorageConsumerSpecs)
                                        .get(current.getChannelIndex());
                        for (int subpartitionId : spec.getSubpartitionIds().values()) {
                            tieredStorageConsumerClient.updateTierShuffleDescriptors(
                                    tieredStoragePartitionId,
                                    spec.getInputChannelId(),
                                    new TieredStorageSubpartitionId(subpartitionId),
                                    checkNotNull(shuffleDescriptor.getTierShuffleDescriptors()));
                        }
                        queueChannel(newChannel, null, false);
                    }
                }
            }

            inputChannels.put(partitionId, newInputChannels);
        }
    }
    // 专门用于处理数据流中断后，重新请求上游数据分区的操作
    // 此方法在 Flink 检测到上游数据分区请求失败或连接丢失时被调用。其核心目的是重新激活一个特定的、先前已请求过的输入通道，使其再次向上游任务请求所需的数据子分区（Subpartitions）
    /** Retriggers a partition request. */
    public void retriggerPartitionRequest(
            IntermediateResultPartitionID partitionId, InputChannelInfo inputChannelInfo)
            throws IOException {
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                final InputChannel ch = inputChannels.get(partitionId).get(inputChannelInfo);

                checkNotNull(ch, "Unknown input channel with ID " + partitionId);

                LOG.debug(
                        "{}: Retriggering partition request {}:{}.",
                        owningTaskName,
                        ch.partitionId,
                        ch.getConsumedSubpartitionIndexSet());

                if (ch.getClass() == RemoteInputChannel.class) {
                    final RemoteInputChannel rch = (RemoteInputChannel) ch;
                    rch.retriggerSubpartitionRequest();
                } else if (ch.getClass() == LocalInputChannel.class) {
                    final LocalInputChannel ich = (LocalInputChannel) ch;

                    if (retriggerLocalRequestTimer == null) {
                        retriggerLocalRequestTimer = new Timer(true);
                    }

                    ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer);
                } else {
                    throw new IllegalStateException(
                            "Unexpected type of channel to retrigger partition: " + ch.getClass());
                }
            }
        }
    }

    @VisibleForTesting
    Timer getRetriggerLocalRequestTimer() {
        return retriggerLocalRequestTimer;
    }

    MemorySegment getUnpooledSegment() {
        return unpooledSegment;
    }

    @Override
    public void close() throws IOException {
        boolean released = false;
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                try {
                    LOG.debug("{}: Releasing {}.", owningTaskName, this);

                    if (retriggerLocalRequestTimer != null) {
                        retriggerLocalRequestTimer.cancel();
                    }

                    for (InputChannel inputChannel : inputChannels()) {
                        try {
                            inputChannel.releaseAllResources();
                        } catch (IOException e) {
                            LOG.warn(
                                    "{}: Error during release of channel resources: {}.",
                                    owningTaskName,
                                    e.getMessage(),
                                    e);
                        }
                    }

                    // The buffer pool can actually be destroyed immediately after the
                    // reader received all of the data from the input channels.
                    if (bufferPool != null) {
                        bufferPool.lazyDestroy();
                    }
                } finally {
                    released = true;
                    closeFuture.complete(null);
                }
            }
        }

        if (released) {
            synchronized (inputChannelsWithData) {
                inputChannelsWithData.notifyAll();
            }
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.close();
            }
        }
    }

    @Override
    public boolean isFinished() {
        return hasReceivedAllEndOfPartitionEvents;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        if (!hasReceivedEndOfData) {
            return EndOfDataStatus.NOT_END_OF_DATA;
        } else if (shouldDrainOnEndOfData) {
            return EndOfDataStatus.DRAINED;
        } else {
            return EndOfDataStatus.STOPPED;
        }
    }

    @Override
    public String toString() {
        return "SingleInputGate{"
                + "owningTaskName='"
                + owningTaskName
                + '\''
                + ", gateIndex="
                + gateIndex
                + '}';
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(true);
    }

    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(false);
    }
    // Flink **数据处理循环（Processing Loop）的核心，它是下游任务（Task）从网络层拉取（Pull）**数据的唯一入口。
    // 负责从排队等待的输入通道中获取下一个可用的数据缓冲区 (Buffer) 或控制事件 (Event)，并处理与任务生命周期和性能监控相关的逻辑。
    private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking)
            throws IOException, InterruptedException {
        // 是否已接收到所有上游输入通道的 EndOfPartitionEvent（分区结束事件）
        if (hasReceivedAllEndOfPartitionEvents) {
            return Optional.empty();
        }

        if (closeFuture.isDone()) {
            throw new CancelTaskException("Input gate is already closed.");
        }
        // 获取下一个数据/事件
        Optional<InputWithData<InputChannel, Buffer>> next = waitAndGetNextData(blocking);
        // 检查是否获得数据
        if (!next.isPresent()) {
            throughputCalculator.pauseMeasurement();
            return Optional.empty();
        }
        // 恢复吞吐量计算
        throughputCalculator.resumeMeasurement();

        InputWithData<InputChannel, Buffer> inputWithData = next.get();
        final BufferOrEvent bufferOrEvent =
                transformToBufferOrEvent(
                        inputWithData.data,
                        inputWithData.moreAvailable,
                        inputWithData.input,
                        inputWithData.morePriorityEvents);
        throughputCalculator.incomingDataSize(bufferOrEvent.getSize());
        return Optional.of(bufferOrEvent);
    }

    // 主要作用是从一个或多个输入通道中等待并获取下一个可用的数据或事件（Buffer）
    // 处理了阻塞/非阻塞模式、数据恢复、多子分区情况下的结束标志（END_OF_DATA/END_OF_PARTITION）以及优先级事件的处理。
    private Optional<InputWithData<InputChannel, Buffer>> waitAndGetNextData(boolean blocking)
            throws IOException, InterruptedException {

        // 无限循环：持续尝试获取数据，直到成功返回一个数据或在非阻塞模式下确认没有数据。
        while (true) {
            synchronized (inputChannelsWithData) {
                // 获取有数据的通道
                // blocking 参数决定了如果暂时没有数据是否阻塞等待。
                Optional<InputChannel> inputChannelOpt = getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    return Optional.empty();
                }

                final InputChannel inputChannel = inputChannelOpt.get();
                // 读取数据/Buffer：调用内部方法，尝试从该通道读取一个 Buffer。
                // 这个方法可能会优先读取恢复数据（如果有）或读取正常数据。
                Optional<Buffer> buffer = readRecoveredOrNormalBuffer(inputChannel);
                if (!buffer.isPresent()) {
                    checkUnavailability();
                    continue;
                }
                // 获取子分区数量：获取当前 InputChannel 所消费的子分区（Subpartition）的数量。
                // 在某些情况下（如 Bounded Stream/Batch），一个输入可能对应多个子分区。
                int numSubpartitions = inputChannel.getConsumedSubpartitionIndexSet().size();
                if (numSubpartitions > 1) {
                    switch (buffer.get().getDataType()) {
                        // 针对当前通道，增加已收到的 END_OF_DATA 计数。
                        // endOfDatas 是一个数组，用于记录每个通道收到的结束标志次数。
                        case END_OF_DATA:
                            endOfDatas[inputChannel.getChannelIndex()]++;
                            if (endOfDatas[inputChannel.getChannelIndex()] < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                            // 处理单个分区结束事件（例如 Batch 作业中的分区结束）。
                        case END_OF_PARTITION:
                            endOfPartitions[inputChannel.getChannelIndex()]++;
                            // 检查是否收到全部分区结束标志：如果收到的 END_OF_PARTITION 数量少于子分区的总数
                            if (endOfPartitions[inputChannel.getChannelIndex()]
                                    < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                        default:
                            break;
                    }
                }
                // 检查是否有更多优先级事件：判断队列中除了当前取出的 Buffer 外，是否还有其他高优先级事件等待处理。
                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                // 当前 Buffer 是优先级事件
                // 如果当前获取的 Buffer 自身具有高优先级（例如 Watermark 或 Barrier）
                if (buffer.get().getDataType().hasPriority()) {
                    if (!morePriorityEvents) {
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                checkUnavailability();
                return Optional.of(
                        new InputWithData<>(
                                inputChannel,
                                buffer.get(),
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    private Optional<Buffer> readRecoveredOrNormalBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        // Firstly, read the buffers from the recovered channel
        if (inputChannel instanceof RecoveredInputChannel && !inputChannel.isReleased()) {
            Optional<Buffer> buffer = readBufferFromInputChannel(inputChannel);
            if (!((RecoveredInputChannel) inputChannel).getStateConsumedFuture().isDone()) {
                return buffer;
            }
        }

        //  After the recovered buffers are read, read the normal buffers
        return enabledTieredStorage()
                ? readBufferFromTieredStore(inputChannel)
                : readBufferFromInputChannel(inputChannel);
    }
    // Flink 在处理单个输入通道数据时的内部辅助方法。它负责从一个特定的 InputChannel 拉取下一个缓冲区，
    // 并处理与调度（Scheduling）、**优先级（Priority）以及分层存储元数据（Tiered Storage Metadata）**相关的逻辑。
    private Optional<Buffer> readBufferFromInputChannel(InputChannel inputChannel)
            throws IOException, InterruptedException {
        // 获取下一个缓冲区
        Optional<BufferAndAvailability> bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
        if (!bufferAndAvailabilityOpt.isPresent()) {
            return Optional.empty();
        }
        final BufferAndAvailability bufferAndAvailability = bufferAndAvailabilityOpt.get();
        // 检查 BufferAndAvailability 中的 moreAvailable 标志。
        // 如果为 true，表示该通道在返回当前缓冲区后，其内部队列中仍有更多数据或事件等待处理。
        if (bufferAndAvailability.moreAvailable()) {
            // enqueue the inputChannel at the end to avoid starvation
            // 将当前的 inputChannel 实例重新加入到就绪队列中。这允许 Gate 稍后再次从该通道读取数据，直到其队列完全耗尽。
            queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
        }
        // 如果是优先级事件，将其序列号记录在 lastPrioritySequenceNumber 数组中。
        // 这对于 Flink 内部处理精确一次（Exactly-Once）语义和优先级调度非常重要。
        if (bufferAndAvailability.hasPriority()) {
            lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                    bufferAndAvailability.getSequenceNumber();
        }

        Buffer buffer = bufferAndAvailability.buffer();
        // 如果是 RECOVERY_METADATA，这表明它包含了用于**分层存储（Tiered Storage）**恢复或数据一致性所需的元数据。
        if (buffer.getDataType() == Buffer.DataType.RECOVERY_METADATA) {
            RecoveryMetadata recoveryMetadata =
                    (RecoveryMetadata)
                            EventSerializer.fromSerializedEvent(
                                    buffer.getNioBufferReadable(), getClass().getClassLoader());
            lastBufferStatusMapInTieredStore.put(
                    inputChannel.getChannelIndex(),
                    Tuple2.of(
                            buffer.getDataType().isPartialRecord(),
                            recoveryMetadata.getFinalBufferSubpartitionId()));
        }
        return Optional.of(bufferAndAvailability.buffer());
    }

    private Optional<Buffer> readBufferFromTieredStore(InputChannel inputChannel)
            throws IOException {
        TieredStorageConsumerSpec tieredStorageConsumerSpec =
                checkNotNull(tieredStorageConsumerSpecs).get(inputChannel.getChannelIndex());
        Tuple2<Boolean, Integer> lastBufferStatus =
                lastBufferStatusMapInTieredStore.computeIfAbsent(
                        inputChannel.getChannelIndex(), key -> Tuple2.of(false, -1));
        boolean isLastBufferPartialRecord = lastBufferStatus.f0;
        int lastSubpartitionId = lastBufferStatus.f1;

        while (true) {
            int subpartitionId;
            if (isLastBufferPartialRecord) {
                subpartitionId = lastSubpartitionId;
            } else {
                subpartitionId =
                        checkNotNull(tieredStorageConsumerClient)
                                .peekNextBufferSubpartitionId(
                                        tieredStorageConsumerSpec.getPartitionId(),
                                        tieredStorageConsumerSpec.getSubpartitionIds());
            }

            if (subpartitionId < 0) {
                return Optional.empty();
            }

            // If the data is available in the specific partition and subpartition, read buffer
            // through consumer client.
            Optional<Buffer> buffer =
                    checkNotNull(tieredStorageConsumerClient)
                            .getNextBuffer(
                                    tieredStorageConsumerSpec.getPartitionId(),
                                    new TieredStorageSubpartitionId(subpartitionId));

            if (buffer.isPresent()) {
                if (!(inputChannel instanceof RecoveredInputChannel)) {
                    queueChannel(checkNotNull(inputChannel), null, false);
                }
                lastBufferStatusMapInTieredStore.put(
                        inputChannel.getChannelIndex(),
                        Tuple2.of(buffer.get().getDataType().isPartialRecord(), subpartitionId));
            } else {
                if (!isLastBufferPartialRecord
                        && inputChannel.getConsumedSubpartitionIndexSet().size() > 1) {
                    // Continue to check other subpartitions that have been marked as
                    // available.
                    continue;
                }
            }

            return buffer;
        }
    }
    // 是否启用分层存储
    private boolean enabledTieredStorage() {
        return tieredStorageConsumerClient != null;
    }

    private void checkUnavailability() {
        assert Thread.holdsLock(inputChannelsWithData);

        if (inputChannelsWithData.isEmpty()) {
            availabilityHelper.resetUnavailable();
        }
    }

    private BufferOrEvent transformToBufferOrEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        if (buffer.isBuffer()) {
            return transformBuffer(buffer, moreAvailable, currentChannel, morePriorityEvents);
        } else {
            return transformEvent(buffer, moreAvailable, currentChannel, morePriorityEvents);
        }
    }

    private BufferOrEvent transformBuffer(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents) {
        return new BufferOrEvent(
                decompressBufferIfNeeded(buffer),
                currentChannel.getChannelInfo(),
                moreAvailable,
                morePriorityEvents);
    }

    private BufferOrEvent transformEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        final AbstractEvent event;
        try {
            event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        } finally {
            buffer.recycleBuffer();
        }

        if (event.getClass() == EndOfPartitionEvent.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfPartitionEvents.get(currentChannel.getChannelIndex()));
                channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());
                hasReceivedAllEndOfPartitionEvents =
                        channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels;

                enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
                if (inputChannelsWithData.contains(currentChannel)) {
                    inputChannelsWithData.getAndRemove(channel -> channel == currentChannel);
                }
            }
            if (hasReceivedAllEndOfPartitionEvents) {
                // Because of race condition between:
                // 1. releasing inputChannelsWithData lock in this method and reaching this place
                // 2. empty data notification that re-enqueues a channel we can end up with
                // moreAvailable flag set to true, while we expect no more data.
                checkState(!moreAvailable || !pollNext().isPresent());
                moreAvailable = false;
                markAvailable();
            }

            currentChannel.releaseAllResources();
        } else if (event.getClass() == EndOfData.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfUserRecords.get(currentChannel.getChannelIndex()));
                channelsWithEndOfUserRecords.set(currentChannel.getChannelIndex());
                hasReceivedEndOfData =
                        channelsWithEndOfUserRecords.cardinality() == numberOfInputChannels;
                shouldDrainOnEndOfData &= ((EndOfData) event).getStopMode() == StopMode.DRAIN;
            }
        }

        return new BufferOrEvent(
                event,
                buffer.getDataType().hasPriority(),
                currentChannel.getChannelInfo(),
                moreAvailable,
                buffer.getSize(),
                morePriorityEvents);
    }

    private Buffer decompressBufferIfNeeded(Buffer buffer) {
        if (buffer.isCompressed()) {
            try {
                checkNotNull(bufferDecompressor, "Buffer decompressor not set.");
                return bufferDecompressor.decompressToIntermediateBuffer(buffer);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return buffer;
    }

    private void markAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (inputChannelsWithData) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        }
        toNotify.complete(null);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.sendTaskEvent(event);
            }

            if (numberOfUninitializedChannels > 0) {
                pendingEvents.add(event);
            }
        }
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        // BEWARE: consumption resumption only happens for streaming jobs in which all slots
        // are allocated together so there should be no UnknownInputChannel. As a result, it
        // is safe to not synchronize the requestLock here. We will refactor the code to not
        // rely on this assumption in the future.
        channels[channelInfo.getInputChannelIdx()].resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        if (!enabledTieredStorage()) {
            channels[channelInfo.getInputChannelIdx()].acknowledgeAllRecordsProcessed();
        }
    }

    // ------------------------------------------------------------------------
    // Channel notifications
    // ------------------------------------------------------------------------
    // 通知通道有新的数据到达
    void notifyChannelNonEmpty(InputChannel channel) {
        if (enabledTieredStorage()) {
            TieredStorageConsumerSpec tieredStorageConsumerSpec =
                    checkNotNull(tieredStorageConsumerSpecs).get(channel.getChannelIndex());
            checkNotNull(availabilityNotifier)
                    .notifyAvailable(
                            tieredStorageConsumerSpec.getPartitionId(),
                            tieredStorageConsumerSpec.getInputChannelId());
        } else {
            queueChannel(checkNotNull(channel), null, false);
        }
    }

    /**
     * Notifies that the respective channel has a priority event at the head for the given buffer
     * number.
     *
     * <p>The buffer number limits the notification to the respective buffer and voids the whole
     * notification in case that the buffer has been polled in the meantime. That is, if task thread
     * polls the enqueued priority buffer before this notification occurs (notification is not
     * performed under lock), this buffer number allows {@link #queueChannel(InputChannel, Integer,
     * boolean)} to avoid spurious priority wake-ups.
     */
    void notifyPriorityEvent(InputChannel inputChannel, int prioritySequenceNumber) {
        queueChannel(checkNotNull(inputChannel), prioritySequenceNumber, false);
    }

    void notifyPriorityEventForce(InputChannel inputChannel) {
        queueChannel(checkNotNull(inputChannel), null, true);
    }

    void triggerPartitionStateCheck(
            ResultPartitionID partitionId, InputChannelInfo inputChannelInfo) {
        partitionProducerStateProvider.requestPartitionProducerState(
                consumedResultId,
                partitionId,
                ((PartitionProducerStateProvider.ResponseHandle responseHandle) -> {
                    boolean isProducingState =
                            new RemoteChannelStateChecker(partitionId, owningTaskName)
                                    .isProducerReadyOrAbortConsumption(responseHandle);
                    if (isProducingState) {
                        try {
                            retriggerPartitionRequest(
                                    partitionId.getPartitionId(), inputChannelInfo);
                        } catch (IOException t) {
                            responseHandle.failConsumption(t);
                        }
                    }
                }));
    }

    private void queueChannel(
            InputChannel channel, @Nullable Integer prioritySequenceNumber, boolean forcePriority) {
        try (GateNotificationHelper notification =
                new GateNotificationHelper(this, inputChannelsWithData)) {
            synchronized (inputChannelsWithData) {
                boolean priority = prioritySequenceNumber != null || forcePriority;

                if (!forcePriority
                        && priority
                        && isOutdated(
                                prioritySequenceNumber,
                                lastPrioritySequenceNumber[channel.getChannelIndex()])) {
                    // priority event at the given offset already polled (notification is not atomic
                    // in respect to
                    // buffer enqueuing), so just ignore the notification
                    return;
                }

                if (!queueChannelUnsafe(channel, priority)) {
                    return;
                }

                if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
                    notification.notifyPriority();
                }
                if (inputChannelsWithData.size() == 1) {
                    notification.notifyDataAvailable();
                }
            }
        }
    }

    private boolean isOutdated(int sequenceNumber, int lastSequenceNumber) {
        if ((lastSequenceNumber < 0) != (sequenceNumber < 0)
                && Math.max(lastSequenceNumber, sequenceNumber) > Integer.MAX_VALUE / 2) {
            // probably overflow of one of the two numbers, the negative one is greater then
            return lastSequenceNumber < 0;
        }
        return lastSequenceNumber >= sequenceNumber;
    }

    /**
     * Queues the channel if not already enqueued and not received EndOfPartition, potentially
     * raising the priority.
     *
     * @return true iff it has been enqueued/prioritized = some change to {@link
     *     #inputChannelsWithData} happened
     */
    private boolean queueChannelUnsafe(InputChannel channel, boolean priority) {
        assert Thread.holdsLock(inputChannelsWithData);
        if (channelsWithEndOfPartitionEvents.get(channel.getChannelIndex())) {
            return false;
        }

        final boolean alreadyEnqueued =
                enqueuedInputChannelsWithData.get(channel.getChannelIndex());
        if (alreadyEnqueued
                && (!priority || inputChannelsWithData.containsPriorityElement(channel))) {
            // already notified / prioritized (double notification), ignore
            return false;
        }

        inputChannelsWithData.add(channel, priority, alreadyEnqueued);
        if (!alreadyEnqueued) {
            enqueuedInputChannelsWithData.set(channel.getChannelIndex());
        }
        return true;
    }

    private Optional<InputChannel> getChannel(boolean blocking) throws InterruptedException {
        assert Thread.holdsLock(inputChannelsWithData);

        while (inputChannelsWithData.isEmpty()) {
            if (closeFuture.isDone()) {
                throw new IllegalStateException("Released");
            }

            if (blocking) {
                inputChannelsWithData.wait();
            } else {
                availabilityHelper.resetUnavailable();
                return Optional.empty();
            }
        }

        InputChannel inputChannel = inputChannelsWithData.poll();
        enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());

        return Optional.of(inputChannel);
    }

    private void setupTieredStorageNettyService(
            TieredStorageNettyServiceImpl nettyService,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {
        List<Supplier<InputChannel>> channelSuppliers = new ArrayList<>();
        for (int index = 0; index < channels.length; ++index) {
            int channelIndex = index;
            channelSuppliers.add(() -> channels[channelIndex]);
        }
        nettyService.setupInputChannels(tieredStorageConsumerSpecs, channelSuppliers);
    }

    /** The default implementation of {@link AvailabilityNotifier}. */
    private class AvailabilityNotifierImpl implements AvailabilityNotifier {

        private AvailabilityNotifierImpl() {}

        @Override
        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
            Map<InputChannelInfo, InputChannel> channels =
                    inputChannels.get(partitionId.getPartitionID().getPartitionId());
            if (channels == null) {
                return;
            }
            InputChannelInfo inputChannelInfo =
                    new InputChannelInfo(gateIndex, inputChannelId.getInputChannelId());
            InputChannel inputChannel = channels.get(inputChannelInfo);
            if (inputChannel != null) {
                queueChannel(inputChannel, null, false);
            }
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel>
            getInputChannels() {
        Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel> result =
                new HashMap<>();
        for (Map.Entry<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
                mapEntry : inputChannels.entrySet()) {
            for (Map.Entry<InputChannelInfo, InputChannel> entry : mapEntry.getValue().entrySet()) {
                result.put(Tuple2.of(mapEntry.getKey(), entry.getKey()), entry.getValue());
            }
        }
        return result;
    }
    // 由于 SingleInputGate 内部的通道存储结构是分层的（通常是 Map<Partition, Map<ChannelInfo, Channel>>），
    // 这个方法实现了一个**扁平化（Flattening）**的迭代器，使得调用者可以简单地、顺序地遍历所有的 InputChannel，而无需关心它们是如何分组存储的
    public Iterable<InputChannel> inputChannels() {
        return () ->
                new Iterator<InputChannel>() {
                    private final Iterator<Map<InputChannelInfo, InputChannel>> mapIterator =
                            inputChannels.values().iterator();

                    private Iterator<InputChannel> iterator = null;

                    @Override
                    public boolean hasNext() {
                        return (iterator != null && iterator.hasNext()) || mapIterator.hasNext();
                    }

                    @Override
                    public InputChannel next() {
                        if ((iterator == null || !iterator.hasNext()) && mapIterator.hasNext()) {
                            iterator = mapIterator.next().values().iterator();
                        }

                        if (iterator == null || !iterator.hasNext()) {
                            return null;
                        }

                        return iterator.next();
                    }
                };
    }
}
