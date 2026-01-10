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

import org.apache.flink.configuration.MemorySize;

import java.io.Serializable;

/**
 * Memory components which constitute the Total Flink Memory.
 *
 * <p>The relationships of Flink JVM and rest memory components are shown below.
 *
 * <pre>
 *               ┌ ─ ─  Total Flink Memory - ─ ─ ┐
 *                 ┌───────────────────────────┐
 *               | │       JVM Heap Memory     │ |
 *                 └───────────────────────────┘
 *               |┌ ─ ─ - - - Off-Heap  - - ─ ─ ┐|
 *                │┌───────────────────────────┐│
 *               │ │     JVM Direct Memory     │ │
 *                │└───────────────────────────┘│
 *               │ ┌───────────────────────────┐ │
 *                ││   Rest Off-Heap Memory    ││
 *               │ └───────────────────────────┘ │
 *                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 *
 * <p>The JVM and rest memory components can consist of further concrete Flink memory components
 * depending on the process type. The Flink memory components can be derived from either its total
 * size or a subset of configured required fine-grained components. Check the implementations for
 * details about the concrete components.
 */
// FlinkMemory 是一个基础接口。它定义了构成 Flink 总内存（Total Flink Memory） 的核心组件。
// FlinkMemory 的主要作用是抽象化 Flink 进程所占用的内存结构。
// Flink 作为一个计算引擎，对内存的控制非常精细。
// 它将进程内存划分为“Flink 管理的内存”和“JVM 运行环境占用的内存”。该接口定义了 Flink 内部管理内存的三个最基本维度：
// 明确边界：区分了 JVM 堆内存和堆外内存。
// 统一规范：为不同类型的 Flink 进程（如 JobManager 和 TaskManager）提供了统一的内存描述规范。
// 计算基准：它是计算整个 Java 进程总内存（Total Process Memory）的基础。

public interface FlinkMemory extends Serializable {
    // 获取 JVM 堆内存 的大小
    // 这部分内存对应 JVM 启动参数中的 -Xmx 和 -Xms
    // 在 JobManager 中，它主要用于存储作业图（JobGraph）、元数据以及运行时消耗。
    // 在 TaskManager 中，它通常包含框架堆内存（Framework Heap）和任务堆内存（Task Heap）。
    MemorySize getJvmHeapMemorySize();
    // 获取 JVM 直接内存（Direct Memory） 的大小
    // 这部分属于堆外内存（Off-heap），对应 JVM 启动参数 -XX:MaxDirectMemorySize
    // Flink 大量使用直接内存进行网络传输（Network Buffers）和某些框架层面的操作，以减少垃圾回收（GC）的压力。
    MemorySize getJvmDirectMemorySize();
    // 获取 Flink 总内存 的大小。
    // 计算公式通常为：Total Flink Memory = JVM Heap + JVM Direct Memory + Rest Off-heap Memory。
    MemorySize getTotalFlinkMemorySize();
}
