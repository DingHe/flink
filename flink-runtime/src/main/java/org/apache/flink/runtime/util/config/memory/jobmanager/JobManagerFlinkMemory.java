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

package org.apache.flink.runtime.util.config.memory.jobmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.FlinkMemory;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink internal memory components of Job Manager.
 *
 * <p>A Job Manager's internal Flink memory consists of the following components.
 *
 * <ul>
 *   <li>JVM Heap Memory
 *   <li>Off-Heap Memory (also JVM Direct Memory)
 * </ul>
 *
 * <p>The relationships of Job Manager Flink memory components are shown below.
 *
 * <pre>
 *               ┌ ─ ─  Total Flink Memory - ─ ─ ┐
 *                 ┌───────────────────────────┐
 *               | │       JVM Heap Memory     │ |
 *                 └───────────────────────────┘
 *               │ ┌───────────────────────────┐ │
 *                 |    Off-heap Heap Memory   │   -─ JVM Direct Memory
 *               │ └───────────────────────────┘ │
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
// 专门用于描述 JobManager (Master) 进程内部的 Flink 内存组件。
// 主要作用是持有并管理 JobManager 进程中由 Flink 掌控的内存配额信息。
// JVM 堆内存 (JVM Heap)：用于运行 JobManager 核心逻辑、维护作业图 (JobGraph) 和元数据。
// 堆外内存 (Off-heap Memory)：在 JobManager 场景下，这部分主要对应 JVM 直接内存 (JVM Direct Memory)，用于某些网络通信或框架开销。
public class JobManagerFlinkMemory implements FlinkMemory {
    private static final long serialVersionUID = 1L;
    // 存储经过计算后的 JVM 堆内存 实际大小。
    private final MemorySize jvmHeap;
    // 存储经过计算后的 堆外内存 大小。
    // 在 JobManager 中，这部分内存会被配置为 JVM 的直接内存（Direct Memory）。
    private final MemorySize offHeapMemory;

    @VisibleForTesting
    public JobManagerFlinkMemory(MemorySize jvmHeap, MemorySize offHeapMemory) {
        this.jvmHeap = checkNotNull(jvmHeap);
        this.offHeapMemory = checkNotNull(offHeapMemory);
    }

    @Override
    public MemorySize getJvmHeapMemorySize() {
        return jvmHeap;
    }

    @Override
    public MemorySize getJvmDirectMemorySize() {
        return offHeapMemory;
    }

    @Override
    public MemorySize getTotalFlinkMemorySize() {
        return jvmHeap.add(offHeapMemory);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof JobManagerFlinkMemory) {
            JobManagerFlinkMemory that = (JobManagerFlinkMemory) obj;
            return Objects.equals(this.jvmHeap, that.jvmHeap)
                    && Objects.equals(this.offHeapMemory, that.offHeapMemory);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jvmHeap, offHeapMemory);
    }
}
