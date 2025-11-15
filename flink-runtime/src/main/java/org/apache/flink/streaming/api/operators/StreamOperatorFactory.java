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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Serializable;

/**
 * A factory to create {@link StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
// 主要作用是充当 StreamOperator（流式算子）的工厂
// 在 Flink 运行时，直接创建 StreamOperator 实例是比较繁琐的，因为它需要大量的上下文信息（如配置、运行时数据结构等）。
// StreamOperatorFactory 职责是将算子的定义与算子的实际创建解耦，并允许在 作业图（StreamGraph）生成阶段 对算子进行一些必要的配置和优化。
// 创建算子实例： 根据运行时参数（StreamOperatorParameters）创建具体的 StreamOperator 实例，该实例将会在 StreamTask 中执行实际的计算逻辑。
// 配置与优化： 允许 Flink 框架在构建作业图时，配置算子的输入/输出类型（如果算子支持）和链式策略（Chaining Strategy），从而影响作业的并行执行和优化。
// <OUT>  表示此工厂创建的 StreamOperator 的输出数据类型。
@PublicEvolving
public interface StreamOperatorFactory<OUT> extends Serializable {

    /** Create the operator. Sets access to the context and the output. */
    // 创建算子实例
    <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters);

    /** Set the chaining strategy for operator factory. */
    // 用于指定当前算子是否可以与其上游/下游算子链在一起（Chain），以在同一个线程/任务槽中执行，从而减少网络传输和序列化开销。
    void setChainingStrategy(ChainingStrategy strategy);

    /** Get the chaining strategy of operator factory. */
    ChainingStrategy getChainingStrategy();

    /** Is this factory for {@link StreamSource}. */
    // 标记是否为 Source 算子。 返回 true 表示此工厂创建的是一个 新的 StreamSource 类型的算子
    default boolean isStreamSource() {
        return false;
    }

    default boolean isLegacySource() {
        return false;
    }

    /**
     * If the stream operator need access to the output type information at {@link StreamGraph}
     * generation. This can be useful for cases where the output type is specified by the returns
     * method and, thus, after the stream operator has been created.
     */
    // 检查算子是否需要在 StreamGraph 生成阶段配置其输出类型
    // 如果返回 true，则 Flink 会调用 setOutputType 方法。默认返回 false。
    default boolean isOutputTypeConfigurable() {
        return false;
    }

    /**
     * Is called by the {@link StreamGraph#addOperator} method when the {@link StreamGraph} is
     * generated. The method is called with the output {@link TypeInformation} which is also used
     * for the {@link StreamTask} output serializer.
     *
     * @param type Output type information of the {@link StreamTask}
     * @param executionConfig Execution configuration
     */
    //设置算子的输出类型。 在 StreamGraph 生成时，如果 isOutputTypeConfigurable() 返回 true，此方法会被调用来传入最终确定的输出 TypeInformation 和 ExecutionConfig。
    default void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {}

    /** If the stream operator need to be configured with the data type they will operate on. */
    // 检查算子是否需要在 StreamGraph 生成阶段配置其输入类型
    default boolean isInputTypeConfigurable() {
        return false;
    }

    /**
     * Is called by the {@link StreamGraph#addOperator} method when the {@link StreamGraph} is
     * generated.
     *
     * @param type The data type of the input.
     * @param executionConfig The execution config for this parallel execution.
     */
    default void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {}

    /** Returns the runtime class of the stream operator. */
    Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader);

    /**
     * Is called to get the OperatorAttributes of the operator. OperatorAttributes can inform the
     * frame to optimize the job performance.
     *
     * @return OperatorAttributes of the operator.
     */
    // 获取算子的属性。 返回 OperatorAttributes 对象，
    // 该对象可以向 Flink 框架提供算子的特性信息，以便框架进行优化（
    @Experimental
    default OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().build();
    }
}
