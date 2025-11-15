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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * Stream operators can implement this interface if they need access to the context and the output.
 *
 * @param <OUT> The output type of the operator
 * @deprecated This class is deprecated in favour of using {@link StreamOperatorFactory} and it's
 *     {@link StreamOperatorFactory#createStreamOperator} and passing the required parameters to the
 *     Operator's constructor in create method.
 */
// 用于定义算子在运行时启动前的**初始化（Setup）**过程。
// 环境注入： 它的主要职责是让 Flink 运行时环境（StreamTask、StreamConfig、Output）能够被注入（Setup）到算子实例中，使算子获得执行所需的上下文信息。
// 算子生命周期的一部分： setup() 方法是在算子生命周期中，紧接着算子实例创建之后、在 open() 方法执行之前被调用的关键步骤。
// 链式策略管理： 它也负责定义和获取算子的链式（Chaining）策略，影响 Flink 如何将多个算子打包在一个任务中执行。
@Deprecated
@PublicEvolving
public interface SetupableStreamOperator<OUT> {

    /** Initializes the operator. Sets access to the context and the output. */
    // containingTask (StreamTask<?, ?>)：算子所在的任务容器。
    // 算子可以通过它访问任务级别的服务，例如计时器服务（Timer Service）、状态后端等。
    // config (StreamConfig)：当前算子（Operator）的配置。 包含了并行度、链式策略、序列化器配置等算子特定的运行时配置信息。
    // output (Output<StreamRecord<OUT>>)：算子的输出通道。 算子使用这个接口将处理后的结果数据、Watermark、Checkpoint Barrier 等元素发送给下游算子。
    void setup(
            StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output);

    ChainingStrategy getChainingStrategy();

    void setChainingStrategy(ChainingStrategy strategy);
}
