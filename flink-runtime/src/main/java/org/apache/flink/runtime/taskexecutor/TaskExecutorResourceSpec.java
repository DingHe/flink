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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Specification of resources to use in running {@link
 * org.apache.flink.runtime.taskexecutor.TaskExecutor}.
 */
// 定义了 TaskExecutor（即 TaskManager 进程）内部用于执行任务的资源规格说明书。
// TaskExecutorResourceSpec 的主要作用是 “资源蓝图”。
// 在 Flink 1.10 引入新的内存模型后，TaskManager 的内存被划分得非常精细。
// 这个类专门负责记录那些 直接用于 Task 执行 的资源。
// 它不关注框架本身的消耗（比如 Flink 框架堆内存），而是关注分配给用户的算子、网络传输和托管内存的具体数额。
public class TaskExecutorResourceSpec {
    // 定义该 TaskExecutor 拥有的 CPU 核心数
    private final CPUResource cpuCores;
    // 任务堆内存
    // 门给用户代码（算子）使用的 JVM 堆内存。
    // 用户在 UDF 中创建的对象主要存储在这里。
    private final MemorySize taskHeapSize;
    // 任务堆外内存
    // 如果用户代码显式地申请了堆外内存（Direct Memory），或者使用了需要堆外内存的库，则由这部分资源覆盖。
    private final MemorySize taskOffHeapSize;
    // 网络内存
    // 用于网络传输的缓冲（Network Buffers），例如数据在算子之间通过网络发送和接收时所需的内存。
    private final MemorySize networkMemSize;
    // 托管内存。
    // 由 Flink 内存管理器直接管理的内存。主要用于 RocksDB 状态后端、批处理中的排序和聚合、以及某些缓存场景
    // 这是 Flink 能够控制不发生 OOM 的核心区域
    private final MemorySize managedMemorySize;
    // 扩展资源（自定义资源）。
    // 用于支持像 GPU、FPGA 等特殊硬件资源
    private final Map<String, ExternalResource> extendedResources;

    public TaskExecutorResourceSpec(
            CPUResource cpuCores,
            MemorySize taskHeapSize,
            MemorySize taskOffHeapSize,
            MemorySize networkMemSize,
            MemorySize managedMemorySize,
            Collection<ExternalResource> extendedResources) {
        this.cpuCores = cpuCores;
        this.taskHeapSize = taskHeapSize;
        this.taskOffHeapSize = taskOffHeapSize;
        this.networkMemSize = networkMemSize;
        this.managedMemorySize = managedMemorySize;
        this.extendedResources =
                Preconditions.checkNotNull(extendedResources).stream()
                        .filter(resource -> !resource.isZero())
                        .collect(Collectors.toMap(ExternalResource::getName, Function.identity()));
        Preconditions.checkArgument(
                this.extendedResources.size() == extendedResources.size(),
                "Duplicate resource name encountered in external resources.");
    }

    public CPUResource getCpuCores() {
        return cpuCores;
    }

    public MemorySize getTaskHeapSize() {
        return taskHeapSize;
    }

    public MemorySize getTaskOffHeapSize() {
        return taskOffHeapSize;
    }

    public MemorySize getNetworkMemSize() {
        return networkMemSize;
    }

    public MemorySize getManagedMemorySize() {
        return managedMemorySize;
    }

    public Map<String, ExternalResource> getExtendedResources() {
        return Collections.unmodifiableMap(extendedResources);
    }
}
