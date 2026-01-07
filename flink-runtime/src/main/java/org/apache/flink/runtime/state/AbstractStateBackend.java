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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import java.io.IOException;

/**
 * An abstract base implementation of the {@link StateBackend} interface.
 *
 * <p>This class has currently no contents and only kept to not break the prior class hierarchy for
 * users.
 */
// 提供通用工具和逻辑： 尽管代码注释指出该类目前内容不多，主要是为了保持向后兼容，
// 但它被设计用来封装所有具体 StateBackend 实现类（如 HashMapStateBackend 或 RocksDBStateBackend）之间共享的通用逻辑、工具方法和通用配置。
// 强制实现核心方法： 它实现了 StateBackend 接口，并使用 abstract 关键字强制所有继承该抽象类的具体后端实现（如 HashMapStateBackend）去实现创建 Keyed State 和 Operator State 后端的核心逻辑。

@PublicEvolving
public abstract class AbstractStateBackend implements StateBackend, java.io.Serializable {

    private static final long serialVersionUID = 4620415814639230247L;

    public static StreamCompressionDecorator getCompressionDecorator(
            ExecutionConfig executionConfig) {
        if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
            return SnappyStreamCompressionDecorator.INSTANCE;
        } else {
            return UncompressedStreamCompressionDecorator.INSTANCE;
        }
    }
    // 状态延迟跟踪配置构建器。
    // 用于构建状态延迟跟踪的配置。如果启用了状态访问延迟跟踪，该配置将传递给实际的状态后端实例，以便收集读写状态的延迟指标。
    protected LatencyTrackingStateConfig.Builder latencyTrackingConfigBuilder =
            LatencyTrackingStateConfig.newBuilder();

    // ------------------------------------------------------------------------
    //  State Backend - State-Holding Backends
    // ------------------------------------------------------------------------
    // 这些方法继承自 StateBackend 接口，但在 AbstractStateBackend 中被声明为抽象方法，强制子类必须提供具体的实现。
    @Override
    public abstract <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException;

    @Override
    public abstract OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws Exception;
}
