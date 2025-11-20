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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * A {@link Transformation} that describes a reduce operation on a {@link KeyedStream}.
 *
 * @param <IN> The input and output type of the transformation.
 * @param <K> The type of the key of the stream.
 */
// ReduceTransformation 是 Flink 中用于描述基于键控流（KeyedStream）的 reduce 聚合操作的逻辑和物理表示。
// 表示键控聚合： 它专门用于表示用户在 KeyedStream 上调用的 reduce() 操作。这意味着它对每个唯一的键（Key）维护一个连续的聚合结果。
// 强制状态依赖： 由于 reduce 操作需要在状态中存储当前的聚合结果，它在构造时会显式声明对状态后端的托管内存依赖，通过调用 updateManagedMemoryStateBackendUseCase(true) 来实现。
@Internal
public final class ReduceTransformation<IN, K> extends PhysicalTransformation<IN> {
    // 上游输入。
    // 指向此 ReduceTransformation 的直接上游，即被进行键控聚合的 Transformation。
    private final Transformation<IN> input;
    // 归约函数。
    // 包含了用户定义的聚合逻辑。它接收两个类型为 IN 的元素，并返回一个类型为 IN 的聚合结果。
    private final ReduceFunction<IN> reducer;
    // 键选择器。
    // 定义了如何从输入元素 IN 中提取 Key (K)。
    // 这个 Key 用于将数据路由到正确的并行子任务，并进行 Keyed State 分区。
    private final KeySelector<IN, K> keySelector;
    // 键类型信息。
    // 状态键 (K) 的数据类型信息，用于序列化和比较键。
    private final TypeInformation<K> keyTypeInfo;
    //链接策略。 定义此操作符在生成物理执行图时，与上游或下游操作符的链接方式（ALWAYS、NEVER 等）
    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    public ReduceTransformation(
            String name,
            int parallelism,
            Transformation<IN> input,
            ReduceFunction<IN> reducer,
            KeySelector<IN, K> keySelector,
            TypeInformation<K> keyTypeInfo,
            boolean parallelismConfigured) {
        super(name, input.getOutputType(), parallelism, parallelismConfigured);
        this.input = input;
        this.reducer = reducer;
        this.keySelector = keySelector;
        this.keyTypeInfo = keyTypeInfo;

        updateManagedMemoryStateBackendUseCase(true);
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        this.chainingStrategy = strategy;
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    public KeySelector<IN, K> getKeySelector() {
        return keySelector;
    }

    public TypeInformation<K> getKeyTypeInfo() {
        return keyTypeInfo;
    }

    public ReduceFunction<IN> getReducer() {
        return reducer;
    }

    /** Returns the {@code TypeInformation} for the elements of the input. */
    public TypeInformation<IN> getInputType() {
        return input.getOutputType();
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
}
