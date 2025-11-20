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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link PhysicalTransformation} for {@link Source}. */
// SourceTransformation 是 Flink 中用于描述数据源 (Source) 操作的专用 Transformation。它表示数据流图的起始节点，即数据是如何进入 Flink 运行时环境的。
// 定义数据入口： 它是 Flink Job Graph 中的第一个操作符，封装了用户提供的 Source 接口实例，该实例定义了如何从外部系统（如 Kafka、文件、Socket）读取数据。
// 配置时间语义： 它持有并应用用户配置的 WatermarkStrategy（水位线策略），负责为进入 Flink 的数据流分配时间戳并生成水位线，这是处理时间语义的关键。
// 边界性声明： 它实现了 WithBoundedness 接口，可以声明数据源是有界 (Bounded) 还是无界 (Unbounded)。
// SplitT	分片类型	Source 使用的数据分片类型（如 Kafka 分区、文件路径）。它继承自 SourceSplit。
// EnumChkT	枚举检查点类型	Source Coordinator 用于枚举和检查分片时的检查点类型。
@Internal
public class SourceTransformation<OUT, SplitT extends SourceSplit, EnumChkT>
        extends TransformationWithLineage<OUT> implements WithBoundedness {
    // 数据源实例。
    // 核心对象，包含了读取外部数据的具体逻辑和配置。
    private final Source<OUT, SplitT, EnumChkT> source;
    // 水位线策略。
    // 定义了如何从 OUT 类型的元素中提取事件时间，以及如何生成 Watermark。
    private final WatermarkStrategy<OUT> watermarkStrategy;
    // 链接策略。
    // 定义此 Source Operator 是否可以与其下游操作符链接在一起
    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;
    // Coordinator 监听 ID。
    // Flink 内部用于 Source Coordinator 组件在 JobManager 端监听消息的标识符。
    private @Nullable String coordinatorListeningID;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param source The {@link Source} itself
     * @param watermarkStrategy The {@link WatermarkStrategy} to use
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    public SourceTransformation(
            String name,
            Source<OUT, SplitT, EnumChkT> source,
            WatermarkStrategy<OUT> watermarkStrategy,
            TypeInformation<OUT> outputType,
            int parallelism) {
        super(name, outputType, parallelism);
        this.source = source;
        this.watermarkStrategy = watermarkStrategy;
        this.extractLineageVertex();
    }

    public SourceTransformation(
            String name,
            Source<OUT, SplitT, EnumChkT> source,
            WatermarkStrategy<OUT> watermarkStrategy,
            TypeInformation<OUT> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        super(name, outputType, parallelism, parallelismConfigured);
        this.source = source;
        this.watermarkStrategy = watermarkStrategy;
        this.extractLineageVertex();
    }

    public Source<OUT, SplitT, EnumChkT> getSource() {
        return source;
    }

    public WatermarkStrategy<OUT> getWatermarkStrategy() {
        return watermarkStrategy;
    }

    @Override
    public Boundedness getBoundedness() {
        return source.getBoundedness();
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        return Collections.singletonList(this);
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {
        this.chainingStrategy = checkNotNull(chainingStrategy);
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    public void setCoordinatorListeningID(@Nullable String coordinatorListeningID) {
        this.coordinatorListeningID = coordinatorListeningID;
    }

    @Nullable
    public String getCoordinatorListeningID() {
        return coordinatorListeningID;
    }

    private void extractLineageVertex() {
        if (source instanceof LineageVertexProvider) {
            setLineageVertex(((LineageVertexProvider) source).getLineageVertex());
        }
    }
}
