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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This Transformation represents the application of a {@link TwoInputStreamOperator} to two input
 * {@code Transformations}. The result is again only one stream.
 *
 * @param <IN1> The type of the elements in the first input {@code Transformation}
 * @param <IN2> The type of the elements in the second input {@code Transformation}
 * @param <OUT> The type of the elements that result from this {@code TwoInputTransformation}
 */
// TwoInputTransformation 是 Flink DataStream API 中用于表示接收两个输入流的物理操作的基类。
// 核心作用是将两个独立的上游数据流合并，并在它们之上应用一个双流操作符（TwoInputStreamOperator）。
// 这是实现流的 Join (连接)、CoMap (协同映射)、Connect (连接) 等二元操作的关键逻辑组件。
// 定义双输入操作： 关联两个上游 Transformation (input1 和 input2) 与一个 TwoInputStreamOperator。
// 双键控状态配置： 管理与 Keyed State 相关的配置，特别是为两个输入流分别设置 KeySelector，确保在进行 Keyed 二元操作时，来自两个流的元素能够基于相同的键值到达同一个并行子任务。
@Internal
public class TwoInputTransformation<IN1, IN2, OUT> extends PhysicalTransformation<OUT> {
    // 第一个输入。 指向第一个上游 Transformation 实例。
    private final Transformation<IN1> input1;
    // 第二个输入。 指向第二个上游 Transformation 实例。
    private final Transformation<IN2> input2;
    // 操作符工厂。 负责创建实际的运行时双流操作符 (TwoInputStreamOperator) 实例。
    private final StreamOperatorFactory<OUT> operatorFactory;
    // 第一个输入的状态 Key 选择器。
    // 用于从第一个输入流 (IN1) 的元素中提取 Key，以便进行 Keyed 状态分区。
    private KeySelector<IN1, ?> stateKeySelector1;
    // 第二个输入的状态 Key 选择器。
    // 用于从第二个输入流 (IN2) 的元素中提取 Key，以便进行 Keyed 状态分区。
    private KeySelector<IN2, ?> stateKeySelector2;
    // 状态 Key 类型。
    // 两个输入流经过 KeySelector 提取后，共享的状态键的数据类型信息。
    private TypeInformation<?> stateKeyType;

    /**
     * Creates a new {@code TwoInputTransformation} from the given inputs and operator.
     *
     * @param input1 The first input {@code Transformation}
     * @param input2 The second input {@code Transformation}
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param operator The {@code TwoInputStreamOperator}
     * @param outputType The type of the elements produced by this Transformation
     * @param parallelism The parallelism of this Transformation
     */
    public TwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            TwoInputStreamOperator<IN1, IN2, OUT> operator,
            TypeInformation<OUT> outputType,
            int parallelism) {
        this(input1, input2, name, SimpleOperatorFactory.of(operator), outputType, parallelism);
    }

    public TwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            TwoInputStreamOperator<IN1, IN2, OUT> operator,
            TypeInformation<OUT> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        this(
                input1,
                input2,
                name,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                parallelismConfigured);
    }

    public TwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outputType,
            int parallelism) {
        super(name, outputType, parallelism);
        this.input1 = input1;
        this.input2 = input2;
        this.operatorFactory = operatorFactory;
    }

    /**
     * Creates a new {@code TwoInputTransformation} from the given inputs and operator.
     *
     * @param input1 The first input {@code Transformation}
     * @param input2 The second input {@code Transformation}
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param operatorFactory The {@code TwoInputStreamOperator} factory
     * @param outputType The type of the elements produced by this Transformation
     * @param parallelism The parallelism of this Transformation
     * @param parallelismConfigured If true, the parallelism of the transformation is explicitly set
     *     and should be respected. Otherwise the parallelism can be changed at runtime.
     */
    public TwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        super(name, outputType, parallelism, parallelismConfigured);
        this.input1 = input1;
        this.input2 = input2;
        this.operatorFactory = operatorFactory;
    }

    /** Returns the first input {@code Transformation} of this {@code TwoInputTransformation}. */
    public Transformation<IN1> getInput1() {
        return input1;
    }

    /** Returns the second input {@code Transformation} of this {@code TwoInputTransformation}. */
    public Transformation<IN2> getInput2() {
        return input2;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        final List<Transformation<?>> inputs = new ArrayList<>();
        inputs.add(input1);
        inputs.add(input2);
        return inputs;
    }

    /** Returns the {@code TypeInformation} for the elements from the first input. */
    public TypeInformation<IN1> getInputType1() {
        return input1.getOutputType();
    }

    /** Returns the {@code TypeInformation} for the elements from the second input. */
    public TypeInformation<IN2> getInputType2() {
        return input2.getOutputType();
    }

    @VisibleForTesting
    public TwoInputStreamOperator<IN1, IN2, OUT> getOperator() {
        return (TwoInputStreamOperator<IN1, IN2, OUT>)
                ((SimpleOperatorFactory) operatorFactory).getOperator();
    }

    /** Returns the {@code StreamOperatorFactory} of this Transformation. */
    public StreamOperatorFactory<OUT> getOperatorFactory() {
        return operatorFactory;
    }

    /**
     * Sets the {@link KeySelector KeySelectors} that must be used for partitioning keyed state of
     * this transformation.
     *
     * @param stateKeySelector1 The {@code KeySelector} to set for the first input
     * @param stateKeySelector2 The {@code KeySelector} to set for the first input
     */
    public void setStateKeySelectors(
            KeySelector<IN1, ?> stateKeySelector1, KeySelector<IN2, ?> stateKeySelector2) {
        this.stateKeySelector1 = stateKeySelector1;
        this.stateKeySelector2 = stateKeySelector2;
        updateManagedMemoryStateBackendUseCase(
                stateKeySelector1 != null || stateKeySelector2 != null);
    }

    /**
     * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
     * Operation for the first input.
     *
     * @see #setStateKeySelectors
     */
    public KeySelector<IN1, ?> getStateKeySelector1() {
        return stateKeySelector1;
    }

    /**
     * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
     * Operation for the second input.
     *
     * @see #setStateKeySelectors
     */
    public KeySelector<IN2, ?> getStateKeySelector2() {
        return stateKeySelector2;
    }

    public void setStateKeyType(TypeInformation<?> stateKeyType) {
        this.stateKeyType = stateKeyType;
    }

    public TypeInformation<?> getStateKeyType() {
        return stateKeyType;
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        List<Transformation<?>> predecessors =
                Stream.of(input1, input2)
                        .flatMap(input -> input.getTransitivePredecessors().stream())
                        .distinct()
                        .collect(Collectors.toList());
        predecessors.add(this);
        return predecessors;
    }

    @Override
    public final void setChainingStrategy(ChainingStrategy strategy) {
        operatorFactory.setChainingStrategy(strategy);
    }

    public boolean isOutputOnlyAfterEndOfStream() {
        return operatorFactory.getOperatorAttributes().isOutputOnlyAfterEndOfStream();
    }

    public boolean isInternalSorterSupported() {
        return operatorFactory.getOperatorAttributes().isInternalSorterSupported();
    }
}
