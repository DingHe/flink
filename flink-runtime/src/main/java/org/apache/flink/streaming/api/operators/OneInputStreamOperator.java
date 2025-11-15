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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Interface for stream operators with one input. Use {@link
 * org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if you want to
 * implement a custom operator.
 *
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 */
// OneInputStreamOperator 是 Flink 中用于实现单输入算子（如 map、filter、flatMap、keyBy 后的聚合等）的核心接口，它在 Flink 的算子体系中扮演了关键角色。
// 明确输入结构： 它是所有单输入算子的契约。
// 通过实现这个接口，一个自定义算子明确告诉 Flink 运行时，它将从一个上游算子接收数据。
// 集成核心功能： 它通过继承两个重要的父接口 (StreamOperator<OUT> 和 Input<IN>)，整合了算子的基本生命周期管理、状态管理以及处理输入数据的能力。
// 支持 keyed State： 它提供了设置 Key 上下文的方法，这对于处理需要使用 keyed State（基于 Key 的状态）的操作至关重要。
@PublicEvolving
public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT>, Input<IN> {
    // 设置 Key 上下文元素（通用）
    // 主要目的是在处理一个输入记录之前，将 Keyed State 的上下文切换到该记录对应的 Key 上。这对于在 keyBy 之后的操作中使用状态（State）至关重要。
    @Override
    default void setKeyContextElement(StreamRecord<IN> record) throws Exception {
        setKeyContextElement1(record);
    }
}
