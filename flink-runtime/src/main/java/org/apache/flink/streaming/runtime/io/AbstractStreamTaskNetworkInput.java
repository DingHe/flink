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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
// 核心作用是将网络传输过来的字节流（Buffers）转换为 Flink 流处理所需的逻辑元素（StreamElements），并在此过程中处理 Flink 容错和时间语义相关的控制流：
// 网络 I/O 抽象： 它封装了与 CheckpointedInputGate 的交互，负责从网络拉取原始的 BufferOrEvent。
// 数据反序列化： 它管理每个输入通道的 RecordDeserializer，负责将跨越网络 Buffer 边界的记录（Record）重新组装和反序列化为 StreamElement（数据、水位线、控制事件等）。
// 水位线/时间处理： 它集成了 StatusWatermarkValve，负责对齐来自不同输入通道的水位线（Watermark）和水位线状态（Watermark Status），确保事件时间语义的正确性。
public abstract class AbstractStreamTaskNetworkInput<
                T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>>
        implements StreamTaskInput<T> {
    // 带 Checkpoint 的输入门（核心）。
    // 这是实际从网络拉取 Buffer 或 Event 的组件，并负责处理 Checkpoint Barrier 对齐和通道状态的写入/恢复。
    protected final CheckpointedInputGate checkpointedInputGate;
    // 反序列化委托。
    // 用于将字节流反序列化成 StreamElement（包括数据记录、水位线、延迟标记等）
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;
    // 负责序列化 Task 接收到的实际用户数据（类型 T）
    protected final TypeSerializer<T> inputSerializer;
    // 存储每个输入通道（InputChannelInfo）对应的 RecordDeserializer 实例。
    // R 是一个泛型，表示特定类型的反序列化器。
    protected final Map<InputChannelInfo, R> recordDeserializers;
    // 扁平化通道索引映射。
    // 将每个输入通道映射到一个从 0 开始的扁平索引。
    // 这个索引主要供 StatusWatermarkValve 使用，用于识别来自哪个通道的水位线。
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();
    /** Valve that controls how watermarks and watermark statuses are forwarded. */
    // 状态水位线控制阀。
    // 负责对齐来自所有输入通道的水位线（Watermark）和水位线状态（Watermark Status）。
    // 只有当控制阀允许时，水位线才会被向下游 Task 发送。
    protected final StatusWatermarkValve statusWatermarkValve;
    // 输入索引。
    // 标识该网络输入在整个 Task 所有输入中的逻辑索引位置（例如，对于双输入 Task，可能是 0 或 1）。
    protected final int inputIndex;
    // 记录属性合并器。
    // 用于处理和组合来自不同通道的 RecordAttributes 事件，例如用于动态分区调整。
    private final RecordAttributesCombiner recordAttributesCombiner;
    // 上次处理的通道。
    // 记录最近一次拉取到数据的通道信息。用于将反序列化的元素归属到正确的通道，特别是用于水位线处理。
    private InputChannelInfo lastChannel = null;
    // 当前活动的反序列化器。 指向 lastChannel 对应的 RecordDeserializer 实例
    private R currentRecordDeserializer = null;
    // 批量发送检查器。
    // 一个回调接口，用于检查 Task 是否可以连续发送多个记录（即进行批量发送优化）
    protected final CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        super();
        this.checkpointedInputGate = checkpointedInputGate;
        deserializationDelegate =
                new NonReusingDeserializationDelegate<>(
                        new StreamElementSerializer<>(inputSerializer));
        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
        this.canEmitBatchOfRecords = checkNotNull(canEmitBatchOfRecords);
        this.recordAttributesCombiner =
                new RecordAttributesCombiner(checkpointedInputGate.getNumberOfInputChannels());
    }
    // 该方法实现了 Flink 的拉取式数据读取和事件处理循环，是数据从网络接收、反序列化到发送给操作符的关键路径。
    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

        while (true) {
            // get the stream element from the deserializer
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {
                    // 尝试从当前缓冲区中反序列化下一个完整的 Flink 流元素（StreamElement，可以是用户数据、Watermark等）
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                } catch (IOException e) {
                    throw new IOException(
                            String.format("Can't get next record for channel %s", lastChannel), e);
                }
                // 检查当前缓冲区是否已完全反序列化完毕（耗尽）
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }
                // 检查反序列化器是否成功获得了一个完整的流元素（用户数据或控制事件）
                if (result.isFullRecord()) {
                    // 对反序列化出的流元素进行具体处理，并将其发送给下游操作符。
                    // 该方法返回一个布尔值，指示是否应该中断当前批次的发射（例如，如果处理的是控制事件，通常会中断）
                    final boolean breakBatchEmitting =
                            processElement(deserializationDelegate.getInstance(), output);
                    if (canEmitBatchOfRecords.check() && !breakBatchEmitting) {
                        continue;
                    }
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }
            // 从输入网关获取新的缓冲区或事件
            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
            // 检查是否成功获取到了下一个 BufferOrEvent
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                // 如果获取到的是一个数据缓冲区
                if (bufferOrEvent.get().isBuffer()) {
                    processBuffer(bufferOrEvent.get());
                } else {
                    DataInputStatus status = processEvent(bufferOrEvent.get());
                    if (status == DataInputStatus.MORE_AVAILABLE && canEmitBatchOfRecords.check()) {
                        continue;
                    }
                    return status;
                }
            } else {
                // 检查输入网关是否已永久关闭（所有输入都已结束）
                if (checkpointedInputGate.isFinished()) {
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    return DataInputStatus.END_OF_INPUT;
                }
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    /**
     * Process the given stream element and returns whether to stop processing and return from the
     * emitNext method so that the emitNext is invoked again right after processing the element to
     * allow behavior change in emitNext method. For example, the behavior of emitNext may need to
     * change right after process a RecordAttributes.
     */
    // Flink 运行时在接收到网络数据后，对反序列化出的 流元素（StreamElement） 进行分派和处理的核心逻辑。
    // 它负责将用户数据转发给操作符，并将控制事件（如 Watermark、WatermarkStatus 等）路由给相应的协调组件。
    // 该方法返回一个布尔值，指示外部调用者（即 emitNext 方法）是否应该在处理完这个元素后立即中断当前的批量发射循环 (while(true))，
    // 并将控制权返回给 Mailbox。返回 true 表示中断并返回，返回 false 表示可以继续批量处理下一条记录。
    private boolean processElement(StreamElement streamElement, DataOutput<T> output)
            throws Exception {
        // 检查流元素是否是用户数据记录
        if (streamElement.isRecord()) {
            output.emitRecord(streamElement.asRecord());
            return false;
        // 处理水位线（Watermark）
        } else if (streamElement.isWatermark()) {
            statusWatermarkValve.inputWatermark(
                    streamElement.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
            return false;
        } else if (streamElement.isLatencyMarker()) {
            output.emitLatencyMarker(streamElement.asLatencyMarker());
            return false;
        } else if (streamElement.isWatermarkStatus()) {
            statusWatermarkValve.inputWatermarkStatus(
                    streamElement.asWatermarkStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
            return false;
        } else if (streamElement.isRecordAttributes()) {
            recordAttributesCombiner.inputRecordAttributes(
                    streamElement.asRecordAttributes(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
            return true;
        } else {
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }
    // Flink 运行时处理来自网络输入流的**控制事件（Control Events）**的核心逻辑，而不是用户数据或 Watermark 等流元素。
    // 这些控制事件通常与数据流的边界、状态恢复或生命周期管理有关。
    protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        // EndOfData 事件标志着上游 Source Task 已经停止生产数据。它主要在流批一体模式或任务停止时使用。
        if (event.getClass() == EndOfData.class) {
            switch (checkpointedInputGate.hasReceivedEndOfData()) {
                case NOT_END_OF_DATA:
                    // skip
                    break;
                case DRAINED:
                    return DataInputStatus.END_OF_DATA;
                case STOPPED:
                    return DataInputStatus.STOPPED;
            }
        // EndOfPartitionEvent 标志着一个特定的上游分区的数据已耗尽，通常在有界流或批处理模式中出现。
        } else if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
            if (checkpointedInputGate.isFinished()) {
                return DataInputStatus.END_OF_INPUT;
            }
        // EndOfChannelStateEvent 是在任务状态恢复过程中使用的事件，标志着一个通道的状态数据已经恢复完毕。
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return DataInputStatus.END_OF_RECOVERY;
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }
    // Flink 任务在从网络输入中获取到一个原始数据缓冲区（Buffer）后，负责进行初始化和准备反序列化的关键步骤。
    // 它将获取到的缓冲区与正确的反序列化器关联起来，为后续的数据处理做准备。
    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);
        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(
                currentRecordDeserializer != null,
                "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
