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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

/**
 * A {@link Transformation} that creates a physical operation. It enables setting {@link
 * ChainingStrategy}.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}
 * @see Transformation
 */
// PhysicalTransformation 是 Flink Transformation 体系中的一个抽象子类，它处于逻辑操作和物理执行之间的桥梁位置
// 核心作用是：表示一个在运行时图（StreamGraph 或 JobGraph）中将形成一个独立物理操作符（Operator）的逻辑节点。
// 相比于其父类 Transformation（只关注逻辑结构和配置），PhysicalTransformation 增加了与 物理运行时特性 相关的配置，最重要的是：
// 设置链接策略 (Chaining Strategy)： 决定此操作符是否可以与上游或下游的操作符**链接（Chain）**在一起，从而在同一个 Task 线程中运行，以优化性能。
// 支持并发执行尝试 (Concurrent Execution Attempts)： 允许在任务失败恢复时，Flink 能够尝试并发执行该任务的多个尝试。
@Internal
public abstract class PhysicalTransformation<T> extends Transformation<T> {
    //支持并发执行尝试标志。 默认值为 true。
    // 它决定了 Flink 在任务失败恢复时，是否可以同时启动多个执行尝试（例如，在启用 TaskExecutor 容错时）。
    // 对于某些具有副作用或外部交互的操作符，可能需要将其设置为 false。
    private boolean supportsConcurrentExecutionAttempts = true;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    PhysicalTransformation(String name, TypeInformation<T> outputType, int parallelism) {
        super(name, outputType, parallelism);
    }

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     * @param parallelismConfigured If true, the parallelism of the transformation is explicitly set
     *     and should be respected. Otherwise the parallelism can be changed at runtime.
     */
    PhysicalTransformation(
            String name,
            TypeInformation<T> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        super(name, outputType, parallelism, parallelismConfigured);
    }

    /** Sets the chaining strategy of this {@code Transformation}. */
    public abstract void setChainingStrategy(ChainingStrategy strategy);

    public boolean isSupportsConcurrentExecutionAttempts() {
        return supportsConcurrentExecutionAttempts;
    }

    public void setSupportsConcurrentExecutionAttempts(
            boolean supportsConcurrentExecutionAttempts) {
        this.supportsConcurrentExecutionAttempts = supportsConcurrentExecutionAttempts;
    }
}
