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

package org.apache.flink.runtime.util.config.memory.taskmanager;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.FlinkMemory;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink internal memory components of Task Executor.
 *
 * <p>A TaskExecutor's internal Flink memory consists of the following components.
 *
 * <ul>
 *   <li>Framework Heap Memory
 *   <li>Framework Off-Heap Memory
 *   <li>Task Heap Memory
 *   <li>Task Off-Heap Memory
 *   <li>Network Memory
 *   <li>Managed Memory
 * </ul>
 *
 * <p>The relationships of TaskExecutor Flink memory components are shown below.
 *
 * <pre>
 *               ┌ ─ ─  Total Flink Memory - ─ ─ ┐
 *               |┌ ─ ─ - - - On-Heap - - - ─ ─ ┐|
 *                 ┌───────────────────────────┐
 *               |││   Framework Heap Memory   ││|
 *                 └───────────────────────────┘
 *               │ ┌───────────────────────────┐ │
 *                ||      Task Heap Memory     ││
 *               │ └───────────────────────────┘ │
 *                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *               |┌ ─ ─ - - - Off-Heap  - - ─ ─ ┐|
 *                │┌───────────────────────────┐│
 *               │ │ Framework Off-Heap Memory │ │ ─┐
 *                │└───────────────────────────┘│   │
 *               │ ┌───────────────────────────┐ │  │
 *                ││   Task Off-Heap Memory    ││   ┼─ JVM Direct Memory
 *               │ └───────────────────────────┘ │  │
 *                │┌───────────────────────────┐│   │
 *               │ │      Network Memory       │ │ ─┘
 *                │└───────────────────────────┘│
 *               │ ┌───────────────────────────┐ │
 *                |│      Managed Memory       │|
 *               │ └───────────────────────────┘ │
 *                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
// 详细定义了 TaskManager (TaskExecutor) 进程内部复杂的内存组成结构。
// TaskExecutorFlinkMemory 的主要作用是精确描述和持有 TaskManager 内部各细粒度组件的内存配额。
// 将内存划分为堆内（On-Heap）和堆外（Off-Heap），并进一步拆分为框架（Framework）、任务（Task）、网络（Network）和托管（Managed）内存
// Flink 逻辑组件	                             JVM 物理归属	        对应 JVM 参数
// Framework Heap + Task Heap	                 JVM Heap	            -Xmx / -Xms
// Framework Off-Heap + Task Off-Heap + Network	 JVM Direct Memory	    -XX:MaxDirectMemorySize
// Managed Memory	                             Off-Heap (Native)	    由 Flink 内部管理，不体现在 JVM 参数中
public class TaskExecutorFlinkMemory implements FlinkMemory {
    private static final long serialVersionUID = 1L;
    // 用于 Flink 框架自身运行所需的堆内存，不计入具体任务的资源消耗。
    private final MemorySize frameworkHeap;
    // 用于 Flink 框架自身运行所需的堆外内存（直接内存）
    private final MemorySize frameworkOffHeap;
    // 用于用户代码及算子运行时的堆内存。
    private final MemorySize taskHeap;
    // 用于用户代码及算子运行时的堆外内存。
    private final MemorySize taskOffHeap;
    // 用于网络传输中的 Buffer 缓冲（Data Shuffle 等）。这部分属于 JVM 直接内存。
    private final MemorySize network;
    // 核心组件。
    // 由 Flink 内存管理器直接管理的内存，主要用于 RocksDB 状态后端、批处理中的排序和聚合。通常配置为堆外内存。
    private final MemorySize managed;

    public TaskExecutorFlinkMemory(
            final MemorySize frameworkHeap,
            final MemorySize frameworkOffHeap,
            final MemorySize taskHeap,
            final MemorySize taskOffHeap,
            final MemorySize network,
            final MemorySize managed) {

        this.frameworkHeap = checkNotNull(frameworkHeap);
        this.frameworkOffHeap = checkNotNull(frameworkOffHeap);
        this.taskHeap = checkNotNull(taskHeap);
        this.taskOffHeap = checkNotNull(taskOffHeap);
        this.network = checkNotNull(network);
        this.managed = checkNotNull(managed);
    }

    public MemorySize getFrameworkHeap() {
        return frameworkHeap;
    }

    public MemorySize getFrameworkOffHeap() {
        return frameworkOffHeap;
    }

    public MemorySize getTaskHeap() {
        return taskHeap;
    }

    public MemorySize getTaskOffHeap() {
        return taskOffHeap;
    }

    public MemorySize getNetwork() {
        return network;
    }

    public MemorySize getManaged() {
        return managed;
    }

    @Override
    public MemorySize getJvmHeapMemorySize() {
        return frameworkHeap.add(taskHeap);
    }

    @Override
    public MemorySize getJvmDirectMemorySize() {
        return frameworkOffHeap.add(taskOffHeap).add(network);
    }

    @Override
    public MemorySize getTotalFlinkMemorySize() {
        return frameworkHeap
                .add(frameworkOffHeap)
                .add(taskHeap)
                .add(taskOffHeap)
                .add(network)
                .add(managed);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof TaskExecutorFlinkMemory) {
            TaskExecutorFlinkMemory that = (TaskExecutorFlinkMemory) obj;
            return Objects.equals(this.frameworkHeap, that.frameworkHeap)
                    && Objects.equals(this.frameworkOffHeap, that.frameworkOffHeap)
                    && Objects.equals(this.taskHeap, that.taskHeap)
                    && Objects.equals(this.taskOffHeap, that.taskOffHeap)
                    && Objects.equals(this.network, that.network)
                    && Objects.equals(this.managed, that.managed);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                frameworkHeap, frameworkOffHeap, taskHeap, taskOffHeap, network, managed);
    }
}
