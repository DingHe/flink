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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput.SelectableSortingInputs;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.tasks.WatermarkGaugeExposingOutput;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A factory for {@link StreamMultipleInputProcessor}. */
@Internal
public class StreamMultipleInputProcessorFactory {

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static StreamMultipleInputProcessor create(
            TaskInvokable ownerTask,
            CheckpointedInputGate[] checkpointedInputGates,
            StreamConfig.InputConfig[] configuredInputs,
            IOManager ioManager,
            MemoryManager memoryManager,
            TaskIOMetricGroup ioMetricGroup,
            Counter mainOperatorRecordsIn,
            MultipleInputStreamOperator<?> mainOperator,
            WatermarkGauge[] inputWatermarkGauges,
            StreamConfig streamConfig,
            Configuration taskManagerConfig,
            Configuration jobConfig,
            ExecutionConfig executionConfig,
            ClassLoader userClassloader,
            OperatorChain<?, ?> operatorChain,
            InflightDataRescalingDescriptor inflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        checkNotNull(operatorChain);

        List<Input> operatorInputs = mainOperator.getInputs();
        int inputsCount = operatorInputs.size();

        StreamOneInputProcessor<?>[] inputProcessors = new StreamOneInputProcessor[inputsCount];
        Counter networkRecordsIn = new SimpleCounter();
        ioMetricGroup.reuseRecordsInputCounter(networkRecordsIn);

        checkState(
                configuredInputs.length == inputsCount,
                "Number of configured inputs in StreamConfig [%s] doesn't match the main operator's number of inputs [%s]",
                configuredInputs.length,
                inputsCount);
        StreamTaskInput[] inputs = new StreamTaskInput[inputsCount];
        for (int i = 0; i < inputsCount; i++) {
            StreamConfig.InputConfig configuredInput = configuredInputs[i];
            if (configuredInput instanceof StreamConfig.NetworkInputConfig) {
                StreamConfig.NetworkInputConfig networkInput =
                        (StreamConfig.NetworkInputConfig) configuredInput;
                inputs[i] =
                        StreamTaskNetworkInputFactory.create(
                                checkpointedInputGates[networkInput.getInputGateIndex()],
                                networkInput.getTypeSerializer(),
                                ioManager,
                                new StatusWatermarkValve(
                                        checkpointedInputGates[networkInput.getInputGateIndex()]),
                                i,
                                inflightDataRescalingDescriptor,
                                gatePartitioners,
                                taskInfo,
                                canEmitBatchOfRecords);
            } else if (configuredInput instanceof StreamConfig.SourceInputConfig) {
                StreamConfig.SourceInputConfig sourceInput =
                        (StreamConfig.SourceInputConfig) configuredInput;
                inputs[i] = operatorChain.getSourceTaskInput(sourceInput);
            } else {
                throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
            }
        }

        InputSelectable inputSelectable =
                mainOperator instanceof InputSelectable ? (InputSelectable) mainOperator : null;

        StreamConfig.InputConfig[] inputConfigs = streamConfig.getInputs(userClassloader);
        boolean anyRequiresSorting =
                Arrays.stream(inputConfigs).anyMatch(StreamConfig::requiresSorting);

        if (anyRequiresSorting) {

            if (inputSelectable != null) {
                throw new IllegalStateException(
                        "The InputSelectable interface is not supported with sorting inputs");
            }

            StreamTaskInput[] sortingInputs =
                    IntStream.range(0, inputsCount)
                            .filter(idx -> requiresSorting(inputConfigs[idx]))
                            .mapToObj(idx -> inputs[idx])
                            .toArray(StreamTaskInput[]::new);
            KeySelector[] sortingInputKeySelectors =
                    IntStream.range(0, inputsCount)
                            .filter(idx -> requiresSorting(inputConfigs[idx]))
                            .mapToObj(idx -> streamConfig.getStatePartitioner(idx, userClassloader))
                            .toArray(KeySelector[]::new);
            TypeSerializer[] sortingInputKeySerializers =
                    IntStream.range(0, inputsCount)
                            .filter(idx -> requiresSorting(inputConfigs[idx]))
                            .mapToObj(idx -> streamConfig.getTypeSerializerIn(idx, userClassloader))
                            .toArray(TypeSerializer[]::new);

            StreamTaskInput[] passThroughInputs =
                    IntStream.range(0, inputsCount)
                            .filter(idx -> !requiresSorting(inputConfigs[idx]))
                            .mapToObj(idx -> inputs[idx])
                            .toArray(StreamTaskInput[]::new);

            SelectableSortingInputs selectableSortingInputs =
                    MultiInputSortingDataInput.wrapInputs(
                            ownerTask,
                            sortingInputs,
                            sortingInputKeySelectors,
                            sortingInputKeySerializers,
                            streamConfig.getStateKeySerializer(userClassloader),
                            passThroughInputs,
                            memoryManager,
                            ioManager,
                            executionConfig.isObjectReuseEnabled(),
                            streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                    ManagedMemoryUseCase.OPERATOR,
                                    jobConfig,
                                    taskManagerConfig,
                                    userClassloader),
                            taskManagerConfig,
                            executionConfig);

            StreamTaskInput<?>[] sortedInputs = selectableSortingInputs.getSortedInputs();
            StreamTaskInput<?>[] passedThroughInputs =
                    selectableSortingInputs.getPassThroughInputs();
            int sortedIndex = 0;
            int passThroughIndex = 0;
            for (int i = 0; i < inputs.length; i++) {
                if (requiresSorting(inputConfigs[i])) {
                    inputs[i] = sortedInputs[sortedIndex];
                    sortedIndex++;
                } else {
                    inputs[i] = passedThroughInputs[passThroughIndex];
                    passThroughIndex++;
                }
            }
            inputSelectable = selectableSortingInputs.getInputSelectable();
        }

        for (int i = 0; i < inputsCount; i++) {
            StreamConfig.InputConfig configuredInput = configuredInputs[i];
            if (configuredInput instanceof StreamConfig.NetworkInputConfig) {
                StreamTaskNetworkOutput dataOutput =
                        new StreamTaskNetworkOutput<>(
                                operatorChain.getFinishedOnRestoreInputOrDefault(
                                        operatorInputs.get(i)),
                                inputWatermarkGauges[i],
                                mainOperatorRecordsIn,
                                networkRecordsIn);

                inputProcessors[i] =
                        new StreamOneInputProcessor(inputs[i], dataOutput, operatorChain);
            } else if (configuredInput instanceof StreamConfig.SourceInputConfig) {
                StreamConfig.SourceInputConfig sourceInput =
                        (StreamConfig.SourceInputConfig) configuredInput;
                OperatorChain.ChainedSource chainedSource =
                        operatorChain.getChainedSource(sourceInput);

                inputProcessors[i] =
                        new StreamOneInputProcessor(
                                inputs[i],
                                new StreamTaskSourceOutput(
                                        chainedSource.getSourceOutput(),
                                        inputWatermarkGauges[i],
                                        chainedSource
                                                .getSourceTaskInput()
                                                .getOperator()
                                                .getSourceMetricGroup()),
                                operatorChain);
            } else {
                throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
            }
        }

        return new StreamMultipleInputProcessor(
                new MultipleInputSelectionHandler(inputSelectable, inputsCount), inputProcessors);
    }

    /**
     * The network data output implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in two input selective processor.
     */
    // 接收从网络输入端（StreamTaskNetworkInput）反序列化出来的流元素，并将它们分发给算子的处理逻辑。
    // 输入端（StreamTaskNetworkInput）负责从网络 Buffer 中解析出对象。
    // 中间人（StreamTaskNetworkOutput）负责记录监控指标（如输入速率、水位线），并执行 JIT 优化的分发逻辑。
    // 目的地（Input<T> / Operator）负责执行真正的业务逻辑。
    private static class StreamTaskNetworkOutput<T> implements PushingAsyncDataInput.DataOutput<T> {
        // 这是数据的下游目的地。
        // 通常指向算子（Operator）的包装对象。所有的元素（记录、水位线等）最终都会调用这个对象的 processXXX 方法进行业务处理。
        private final Input<T> input;
        // 水位线度量指标
        // 用于在 Flink UI 或监控系统中展示当前算子收到的最大水位线。每当有新的水位线通过时，它会更新内部的时间戳。
        private final WatermarkGauge inputWatermarkGauge;
        // 算子级别的输入记录计数器。
        // 记录该算子接收到的总记录数，用于计算算子的吞吐量（TPS）。
        private final Counter mainOperatorRecordsIn;
        // 网络层级别的输入记录计数器。
        // 通常用于统计从特定网络通道进入的数据量，帮助分析是否存在背压或网络倾斜。
        private final Counter networkRecordsIn;

        /** The function way is only used for frequent record processing as for JIT optimization. */
        // 高度优化的记录处理函数。
        private final ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer;

        private StreamTaskNetworkOutput(
                Input<T> input,
                WatermarkGauge inputWatermarkGauge,
                Counter mainOperatorRecordsIn,
                Counter networkRecordsIn) {
            this.input = checkNotNull(input);
            this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
            this.mainOperatorRecordsIn = mainOperatorRecordsIn;
            this.networkRecordsIn = networkRecordsIn;
            this.recordConsumer = RecordProcessorUtils.getRecordProcessor(input);
        }

        @Override
        public void emitRecord(StreamRecord<T> record) throws Exception {
            recordConsumer.accept(record);
            mainOperatorRecordsIn.inc();
            networkRecordsIn.inc();
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
            input.processWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            input.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            input.processLatencyMarker(latencyMarker);
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            input.processRecordAttributes(recordAttributes);
        }
    }
    // StreamTaskSourceOutput 是 AsyncDataOutputToOutput 的一个特定子类。
    // 它的核心作用是：专门为 Source 算子（SourceOperator）定制的输出适配器，用于将 SourceReader 产生的数据直接对接到 Flink 的算子链（Operator Chain）中。
    // 然父类 AsyncDataOutputToOutput 已经处理了大部分记录和指标的转换，但在 SourceOperatorStreamTask（即新版 Source 的执行任务）中，输出端通常是一个更复杂的 WatermarkGaugeExposingOutput。
    // 这个子类通过显式持有这个特殊的输出接口，确保了水位线状态（WatermarkStatus）能够正确地在算子链中传播，而不仅仅是作为普通事件处理。
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static class StreamTaskSourceOutput
            extends SourceOperatorStreamTask.AsyncDataOutputToOutput {
        private final WatermarkGaugeExposingOutput<StreamRecord<?>> chainedOutput;

        public StreamTaskSourceOutput(
                WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput,
                WatermarkGauge inputWatermarkGauge,
                InternalSourceReaderMetricGroup metricGroup) {
            super(chainedSourceOutput, metricGroup, inputWatermarkGauge);
            this.chainedOutput = chainedSourceOutput;
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            chainedOutput.emitWatermarkStatus(watermarkStatus);
        }
    }
}
