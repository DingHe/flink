/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Transformation} for {@link Sink}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}
 * @param <OutputT> The output type of the {@link Sink}
 */
// SinkTransformation 是 Flink 中用于描述数据汇（Sink）操作的专用 Transformation。它表示数据流图的终点，即数据离开 Flink 运行时并写入到外部系统（如数据库、消息队列、文件系统）的逻辑操作。
// 定义数据流终点： 它是图中的最后一个节点，将上游的数据流 (inputStream) 绑定到一个具体的 Sink 实例（基于 Flink 的 SinkV2 API）
// 封装 Sink 逻辑： 它持有用户定义的 Sink 对象，该对象包含了与外部系统交互的所有逻辑（如序列化、写入机制、事务保证）。
// 携带血缘信息： 它继承自 TransformationWithLineage，确保了数据汇操作能够携带血缘元数据，用于追踪数据被写入了哪些外部数据集。
// SinkTransformation 专门负责将一个 DataStream 转化为一个数据流图的输出节点，并封装了写入外部系统所需的一切配置和逻辑。
@Internal
public class SinkTransformation<InputT, OutputT> extends TransformationWithLineage<OutputT> {
    // 输入数据流对象。 封装了上游的 DataStream 实例
    private final DataStream<InputT> inputStream;
    // 数据汇逻辑实例。
    // 这是用户提供的、实现了 Flink SinkV2 API 的核心 Sink 对象，包含了与外部系统交互的逻辑。
    private final Sink<InputT> sink;
    // 游 Transformation。
    // 指向 inputStream 所对应的上游 Transformation 实例。这是用于构建逻辑图和血缘关系的基础
    private final Transformation<InputT> input;
    // 自定义 Sink 操作符 UID 哈希。
    // 存储了用户为 Sink 操作符（及其潜在的内部操作符）提供的自定义 UID 哈希，用于确保状态恢复的稳定性
    private final CustomSinkOperatorUidHashes customSinkOperatorUidHashes;
    // 接策略。 定义此 Sink 操作符在物理执行图中的链接方式（是作为链的头部、尾部还是独立节点）。
    private ChainingStrategy chainingStrategy;

    public SinkTransformation(
            DataStream<InputT> inputStream,
            Sink<InputT> sink,
            TypeInformation<OutputT> outputType,
            String name,
            int parallelism,
            boolean parallelismConfigured,
            CustomSinkOperatorUidHashes customSinkOperatorUidHashes) {
        super(name, outputType, parallelism, parallelismConfigured);
        this.inputStream = checkNotNull(inputStream);
        this.sink = checkNotNull(sink);
        this.input = inputStream.getTransformation();
        this.customSinkOperatorUidHashes = checkNotNull(customSinkOperatorUidHashes);
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        chainingStrategy = checkNotNull(strategy);
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        final List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }

    @Nullable
    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    public DataStream<InputT> getInputStream() {
        return inputStream;
    }

    public Sink<InputT> getSink() {
        return sink;
    }

    public CustomSinkOperatorUidHashes getSinkOperatorsUidHashes() {
        return customSinkOperatorUidHashes;
    }
}
