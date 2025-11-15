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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
// StreamOneInputProcessor<IN> 是 Flink 流处理中处理只有一个输入流的算子任务（如 MapFunction、FilterFunction 等）输入逻辑的组件
// 单输入数据拉取： 负责从唯一的输入源 (StreamTaskInput) 持续读取数据，并将数据发送给算子的输出目标 (DataOutput)。
@Internal
public final class StreamOneInputProcessor<IN> implements StreamInputProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamOneInputProcessor.class);
    // 任务输入抽象。这是实际负责从底层 I/O 层（如网络 InputGate）读取数据的核心组件。对于单输入任务，它只有一个。
    private StreamTaskInput<IN> input;
    // 数据输出目标。
    // 这是接收输入记录的组件，通常是任务的算子链中的第一个算子实例。
    // 它负责将记录交付给用户定义的函数进行处理。
    private DataOutput<IN> output;
    // 输入结束感知接口。
    // 用于通知有界多输入任务（在 StreamOneInputProcessor 的上下文中，通常是实现 BoundedOneInput 的算子）某个输入流已经结束。
    private final BoundedMultiInput endOfInputAware;

    public StreamOneInputProcessor(
            StreamTaskInput<IN> input, DataOutput<IN> output, BoundedMultiInput endOfInputAware) {

        this.input = checkNotNull(input);
        this.output = checkNotNull(output);
        this.endOfInputAware = checkNotNull(endOfInputAware);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return input.getAvailableFuture();
    }

    @Override
    public DataInputStatus processInput() throws Exception {
        DataInputStatus status = input.emitNext(output);

        if (status == DataInputStatus.END_OF_DATA) {
            endOfInputAware.endInput(input.getInputIndex() + 1);
            output = new FinishedDataOutput<>();
        } else if (status == DataInputStatus.END_OF_RECOVERY) {
            if (input instanceof RecoverableStreamTaskInput) {
                input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
            }
            return DataInputStatus.MORE_AVAILABLE;
        }

        return status;
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return input.prepareSnapshot(channelStateWriter, checkpointId);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
