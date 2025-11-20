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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PhysicalTransformation} for a {@link
 * DataStream#assignTimestampsAndWatermarks(WatermarkStrategy)}.
 *
 * @param <IN> The input and output type of the transformation.
 */
// TimestampsAndWatermarksTransformation 类是 Flink 流处理 API 内部的一个核心组件，
// 它代表了用户在 DataStream 上调用 .assignTimestampsAndWatermarks(WatermarkStrategy) 操作后在逻辑执行图（DAG）中生成的 Transformation 节点。
// 逻辑图表示： 它在 Flink 编译 Job 图时，用于表示“提取事件时间戳并生成水位线”这个特定的操作节点。
// 封装水位线策略： 它封装了用户定义的 WatermarkStrategy 对象，该对象包含时间戳提取器（Timestamp Extractor）和水位线生成器（Watermark Generator）
// 连接和配置： 作为图中的一个物理节点，它存储了输入 Transformation、并行度以及操作链策略，供 Flink 的优化器和运行时使用。
// 它是一个元数据容器，将用户在代码中配置的水位线和时间戳分配逻辑转化成 Flink DAG 中的一个可执行、可配置的节点。
// IN>	输入/输出类型	流中元素的类型。 时间戳和水位线操作不会改变元素的类型。
@Internal
public class TimestampsAndWatermarksTransformation<IN> extends PhysicalTransformation<IN> {
    // 上游输入。
    // 指向当前这个水位线操作所连接的上一个 Transformation 节点。
    private final Transformation<IN> input;
    // 水位线策略。
    // 存储用户配置的用于提取时间戳和生成水位线的具体策略实现。
    private final WatermarkStrategy<IN> watermarkStrategy;

    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param parallelism The parallelism of this {@code Transformation}
     * @param input The input transformation of this {@code Transformation}
     * @param watermarkStrategy The {@link WatermarkStrategy} to use
     */
    public TimestampsAndWatermarksTransformation(
            String name,
            int parallelism,
            Transformation<IN> input,
            WatermarkStrategy<IN> watermarkStrategy,
            boolean parallelismConfigured) {
        super(name, input.getOutputType(), parallelism, parallelismConfigured);
        this.input = input;
        this.watermarkStrategy = watermarkStrategy;
    }

    /** Returns the {@code TypeInformation} for the elements of the input. */
    public TypeInformation<IN> getInputType() {
        return input.getOutputType();
    }

    /** Returns the {@code WatermarkStrategy} to use. */
    public WatermarkStrategy<IN> getWatermarkStrategy() {
        return watermarkStrategy;
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        List<Transformation<?>> transformations = Lists.newArrayList();
        transformations.add(this);
        transformations.addAll(input.getTransitivePredecessors());
        return transformations;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {
        this.chainingStrategy = checkNotNull(chainingStrategy);
    }
}
