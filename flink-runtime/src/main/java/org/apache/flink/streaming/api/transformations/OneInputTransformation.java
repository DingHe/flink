/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * This Transformation represents the application of a {@link
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator} to one input {@link
 * Transformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation}
 * @param <OUT> The type of the elements that result from this {@code OneInputTransformation}
 */
// OneInputTransformation 是 Flink DataStream API 中最重要的 Transformation 子类之一。
// 它代表了只有一个输入流的物理操作，例如 map()、filter()、keyBy().window().aggregate() 等大多数单流操作。
// 定义单输入操作： 它将一个上游的 Transformation（即 input）与一个具体的 OneInputStreamOperator（通过其 operatorFactory）关联起来，从而在逻辑图上定义了一个具有单一输入的操作。
// 存储操作符和工厂： 它持有创建实际运行时操作符实例（StreamOperator）所需的工厂类，并将操作符的配置（如链接策略）向下传递。
// 管理 Keyed State 配置： 它负责保存与有状态操作相关的配置，特别是用于对 Keyed State 进行分区和访问的 KeySelector 及其 KeyType。


@Internal
public class OneInputTransformation<IN, OUT> extends PhysicalTransformation<OUT> {
    // 上游输入。
    // 指向此操作符的唯一直接上游 Transformation 实例。
    // 这个上游的输出数据就是本操作符的输入数据。
    private final Transformation<IN> input;
    // 操作符工厂。
    // 负责创建实际的运行时操作符 (OneInputStreamOperator) 实例。
    // 使用工厂模式支持操作符的包装和运行时配置注入。
    private final StreamOperatorFactory<OUT> operatorFactory;
    // 状态 Key 选择器。
    // 如果此操作符是有状态的（如 keyBy() 后的聚合），这个选择器定义了如何从输入元素 IN 中提取状态键 (State Key)。
    // 用于分区 Keyed State。
    private KeySelector<IN, ?> stateKeySelector;
    // 状态 Key 类型。
    // 状态键的数据类型信息，用于序列化和比较状态键。
    private TypeInformation<?> stateKeyType;

    /**
     * Creates a new {@code OneInputTransformation} from the given input and operator.
     *
     * @param input The input {@code Transformation}
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param operator The {@code TwoInputStreamOperator}
     * @param outputType The type of the elements produced by this {@code OneInputTransformation}
     * @param parallelism The parallelism of this {@code OneInputTransformation}
     */
    public OneInputTransformation(
            Transformation<IN> input,
            String name,
            OneInputStreamOperator<IN, OUT> operator,
            TypeInformation<OUT> outputType,
            int parallelism) {
        this(input, name, SimpleOperatorFactory.of(operator), outputType, parallelism);
    }

    public OneInputTransformation(
            Transformation<IN> input,
            String name,
            OneInputStreamOperator<IN, OUT> operator,
            TypeInformation<OUT> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        this(
                input,
                name,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                parallelismConfigured);
    }

    public OneInputTransformation(
            Transformation<IN> input,
            String name,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outputType,
            int parallelism) {
        super(name, outputType, parallelism);
        this.input = input;
        this.operatorFactory = operatorFactory;
    }

    /**
     * Creates a new {@code LegacySinkTransformation} from the given input {@code Transformation}.
     *
     * @param input The input {@code Transformation}
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param operatorFactory The {@code TwoInputStreamOperator} factory
     * @param outputType The type of the elements produced by this {@code OneInputTransformation}
     * @param parallelism The parallelism of this {@code OneInputTransformation}
     * @param parallelismConfigured If true, the parallelism of the transformation is explicitly set
     *     and should be respected. Otherwise the parallelism can be changed at runtime.
     */
    public OneInputTransformation(
            Transformation<IN> input,
            String name,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        super(name, outputType, parallelism, parallelismConfigured);
        this.input = input;
        this.operatorFactory = operatorFactory;
    }

    /** Returns the {@code TypeInformation} for the elements of the input. */
    public TypeInformation<IN> getInputType() {
        return input.getOutputType();
    }

    @VisibleForTesting
    public OneInputStreamOperator<IN, OUT> getOperator() {
        return (OneInputStreamOperator<IN, OUT>)
                ((SimpleOperatorFactory) operatorFactory).getOperator();
    }

    /** Returns the {@code StreamOperatorFactory} of this Transformation. */
    public StreamOperatorFactory<OUT> getOperatorFactory() {
        return operatorFactory;
    }

    /**
     * Sets the {@link KeySelector} that must be used for partitioning keyed state of this
     * operation.
     *
     * @param stateKeySelector The {@code KeySelector} to set
     */
    public void setStateKeySelector(KeySelector<IN, ?> stateKeySelector) {
        this.stateKeySelector = stateKeySelector;
        updateManagedMemoryStateBackendUseCase(stateKeySelector != null);
    }

    /**
     * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
     * Operation.
     *
     * @see #setStateKeySelector
     */
    public KeySelector<IN, ?> getStateKeySelector() {
        return stateKeySelector;
    }

    public void setStateKeyType(TypeInformation<?> stateKeyType) {
        this.stateKeyType = stateKeyType;
    }

    public TypeInformation<?> getStateKeyType() {
        return stateKeyType;
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }

    @Override
    public final void setChainingStrategy(ChainingStrategy strategy) {
        operatorFactory.setChainingStrategy(strategy);
    }
    // 检查是否只在流结束时输出。
    // 查询操作符工厂的属性，检查此操作符是否只在输入流处理完毕（收到 End-of-Stream 标记）后才产生输出（例如某些特殊的 Bounded 聚合操作）
    public boolean isOutputOnlyAfterEndOfStream() {
        return operatorFactory.getOperatorAttributes().isOutputOnlyAfterEndOfStream();
    }
    // 检查是否支持内部排序。
    // 查询操作符工厂的属性，检查此操作符是否能够支持 Flink 的内部数据排序功能（例如用于批处理优化）。
    public boolean isInternalSorterSupported() {
        return operatorFactory.getOperatorAttributes().isInternalSorterSupported();
    }
}
