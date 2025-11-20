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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.SortingDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInputFactory;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.curator5.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link StreamTask} for executing a {@link OneInputStreamOperator}. */
// Flink 流处理中最基础且最常见的任务类型之一。它的作用是执行只接收一个输入流的流操作符 (OneInputStreamOperator)
// 管理单输入： 负责从网络中接收数据，并处理所有与单个输入流相关的逻辑（例如反序列化、事件时间水位线对齐、检查点屏障处理）
// 封装操作符： 作为 OneInputStreamOperator 的容器，它将接收到的数据和控制事件（如 Watermark、Checkpoint 屏障）转发给底层的操作符实例进行业务逻辑处理。
// 支持 Checkpointing： 负责设置和集成检查点屏障处理器 (CheckpointBarrierHandler)，以保证数据流和状态快照的一致性。
// 支持排序输入： 在配置需要时，可以为输入数据添加排序功能（Shuffle后排序）
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {
    // 检查点屏障处理器。
    // 负责处理输入流中接收到的检查点屏障（Checkpoint Barrier）。
    // 它用于实现 Flink 的分布式快照机制，确保状态的一致性。如果 Job 未启用 Checkpointing，则可能为 null
    @Nullable private CheckpointBarrierHandler checkpointBarrierHandler;
    // 输入水位线指标。
    // 一个 Flink 指标（Metric），用于记录和暴露当前任务接收到的聚合输入水位线（即输入 Watermark 的最小值）
    private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    public OneInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the time provider) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param env The task environment for this task.
     * @param timeProvider Optionally, a specific time provider to use.
     */
    @VisibleForTesting
    public OneInputStreamTask(Environment env, @Nullable TimerService timeProvider)
            throws Exception {
        super(env, timeProvider);
    }
    // 任务初始化。
    // 这是任务启动后执行的核心初始化逻辑，主要负责设置输入处理器和指标。
    @Override
    public void init() throws Exception {
        StreamConfig configuration = getConfiguration();
        // 获取配置： 获取任务配置 (StreamConfig) 并检查输入数量 (numberOfInputs)
        int numberOfInputs = configuration.getNumberOfNetworkInputs();

        if (numberOfInputs > 0) {
            // 创建输入组件： 如果有输入 (numberOfInputs > 0)，
            // 则依次创建和配置 CheckpointedInputGate、记录计数器 (numRecordsIn)、数据输出适配器 (output) 和 StreamTaskInput (input)
            CheckpointedInputGate inputGate = createCheckpointedInputGate();
            Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
            DataOutput<IN> output = createDataOutput(numRecordsIn);
            StreamTaskInput<IN> input = createTaskInput(inputGate);

            StreamConfig.InputConfig[] inputConfigs =
                    configuration.getInputs(getUserCodeClassLoader());
            StreamConfig.InputConfig inputConfig = inputConfigs[0];
            if (requiresSorting(inputConfig)) {
                // 处理排序输入： 检查配置是否需要排序 (requiresSorting)。如果需要，则使用 wrapWithSorted(input) 将输入封装在一个 SortingDataInput 中，
                // 并断言 Checkpointing 未启用（排序输入和 Checkpointing 不兼容）
                checkState(
                        !configuration.isCheckpointingEnabled(),
                        "Checkpointing is not allowed with sorted inputs.");
                input = wrapWithSorted(input);
            }

            getEnvironment()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .reuseRecordsInputCounter(numRecordsIn);

            inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);
        }
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge::getValue);
    }

    @Override
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(checkpointBarrierHandler);
    }

    private StreamTaskInput<IN> wrapWithSorted(StreamTaskInput<IN> input) {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        return new SortingDataInput<>(
                input,
                configuration.getTypeSerializerIn(input.getInputIndex(), userCodeClassLoader),
                configuration.getStateKeySerializer(userCodeClassLoader),
                configuration.getStatePartitioner(input.getInputIndex(), userCodeClassLoader),
                getEnvironment().getMemoryManager(),
                getEnvironment().getIOManager(),
                getExecutionConfig().isObjectReuseEnabled(),
                configuration.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        getJobConfiguration(),
                        getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                this,
                getExecutionConfig());
    }

    @SuppressWarnings("unchecked")
    private CheckpointedInputGate createCheckpointedInputGate() {
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[] {Arrays.asList(inputGates)},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService);

        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[] {Arrays.asList(inputGates)},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        return Iterables.getOnlyElement(Arrays.asList(checkpointedInputGates));
    }

    private DataOutput<IN> createDataOutput(Counter numRecordsIn) {
        return new StreamTaskNetworkOutput<>(
                operatorChain.getFinishedOnRestoreInputOrDefault(mainOperator),
                inputWatermarkGauge,
                numRecordsIn);
    }

    private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate) {
        StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(inputGate);

        TypeSerializer<IN> inSerializer =
                configuration.getTypeSerializerIn1(getUserCodeClassLoader());

        return StreamTaskNetworkInputFactory.create(
                inputGate,
                inSerializer,
                getEnvironment().getIOManager(),
                statusWatermarkValve,
                0,
                getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                gateIndex ->
                        configuration
                                .getInPhysicalEdges(getUserCodeClassLoader())
                                .get(gateIndex)
                                .getPartitioner(),
                getEnvironment().getTaskInfo(),
                getCanEmitBatchOfRecords());
    }

    /**
     * The network data output implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in one input processor.
     */
    private static class StreamTaskNetworkOutput<IN> implements DataOutput<IN> {

        private final Input<IN> operator;

        private final WatermarkGauge watermarkGauge;
        private final Counter numRecordsIn;
        private final ThrowingConsumer<StreamRecord<IN>, Exception> recordProcessor;

        private StreamTaskNetworkOutput(
                Input<IN> operator, WatermarkGauge watermarkGauge, Counter numRecordsIn) {

            this.operator = checkNotNull(operator);
            this.watermarkGauge = checkNotNull(watermarkGauge);
            this.numRecordsIn = checkNotNull(numRecordsIn);
            this.recordProcessor = RecordProcessorUtils.getRecordProcessor(operator);
        }

        @Override
        public void emitRecord(StreamRecord<IN> record) throws Exception {
            numRecordsIn.inc();
            recordProcessor.accept(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
            operator.processWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            operator.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            operator.processLatencyMarker(latencyMarker);
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            operator.processRecordAttributes(recordAttributes);
        }
    }
}
