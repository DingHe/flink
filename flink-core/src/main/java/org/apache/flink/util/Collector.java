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

package org.apache.flink.util;

import org.apache.flink.annotation.Public;

/**
 * Collects a record and forwards it. The collector is the "push" counterpart of the {@link
 * java.util.Iterator}, which "pulls" data in.
 */
// Flink 数据流编程模型中实现向下游输出数据的通用机制。
// 在 Flink 中，运算符（Operator）处理完输入数据后，需要将结果发送给链中的下一个运算符或输出到外部系统。
// Collector 就是用于实现这种**数据推送（Push-based）**的抽象。
// 推模式 vs 拉模式： 如接口注释所述，Collector 是 Java 标准库中 Iterator 的“推模式”对应物。Iterator 是拉取（Pull）数据，而 Collector 是推送（Push）数据。
// 核心用途： 任何生成结果记录的 Flink 函数（例如 MapFunction, FlatMapFunction, SourceFunction 等）都会获得一个 Collector 实例，它们通过调用 collect(T record) 方法将结果向下游发出

@Public
public interface Collector<T> {

    /**
     * Emits a record.
     *
     * @param record The record to collect.
     */
    // 发射一条记录。 这是 Collector 的主要功能，用于将处理后的数据发送给下游组件。
    void collect(T record);

    /** Closes the collector. If any data was buffered, that data will be flushed. */
    void close();
}
