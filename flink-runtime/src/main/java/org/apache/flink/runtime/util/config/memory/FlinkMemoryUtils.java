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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

/**
 * Utility to derive the {@link FlinkMemory} components.
 *
 * <p>The {@link FlinkMemory} represents memory components which constitute the Total Flink Memory.
 * The Flink memory components can be derived from either its total size or a subset of configured
 * required fine-grained components. See implementations for details about the concrete fine-grained
 * components.
 *
 * @param <FM> the Flink memory components
 */
// 专门负责根据不同的已知条件，计算并推导出 “Flink 总内存（Total Flink Memory）” 内部各个组件的分配规格。
// 在 Flink 的内存模型中，Total Flink Memory 是一个复合体。
// 对于 JobManager，它包含：JVM 堆内存 + 堆外内存。
// 对于 TaskManager，它更复杂，包含：框架堆内存/堆外内存 + 任务堆内存/堆外内存 + 托管内存 + 网络内存。
// 支持双向推导：
// 自下而上：根据用户配置的各个细粒度组件（如固定了 Managed Memory 和 Network Memory），反推总内存。
// 自上而下：根据给定的总内存（Total Size），按照比例或默认值切分出各个子组件。
public interface FlinkMemoryUtils<FM extends FlinkMemory> {
    // “细粒度优先” 推导模式
    // 场景：当用户在配置文件中没有设置总内存（既没设 total.process.size 也没设 total.flink.size），但显式设置了所有的核心细粒度组件时使用。
    FM deriveFromRequiredFineGrainedOptions(Configuration config);
    // “总量分配” 推导模式。
    // 场景：当 Flink 已经确定了“Flink 总内存”是多少（例如用户配置了 total.process.size，减去 JVM 开销后得到了 totalFlinkMemorySize）。
    FM deriveFromTotalFlinkMemory(Configuration config, MemorySize totalFlinkMemorySize);
}
