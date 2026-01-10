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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.FlinkMemoryUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FlinkMemoryUtils} for Job Manager.
 *
 * <p>The required fine-grained component is {@link JobManagerOptions#JVM_HEAP_MEMORY}.
 */
// JobManagerFlinkMemoryUtils 是 Flink 内存管理模块中专门为 JobManager 打造的内存推导实现类。
// 它实现了 FlinkMemoryUtils 接口，负责将抽象的配置逻辑转化为具体的 JobManagerFlinkMemory 对象（即 JM 的堆内存和堆外内存）
// 由于 JobManager 的内存结构相对简单（主要就是 JVM Heap 和 Off-heap），这个类的核心作用就是：
// 执行推导计算：根据用户提供的“总内存”或“子项内存”，计算出另一方。
// 一致性校验（Sanity Check）：确保用户手动设置的各个内存项之间没有冲突（例如：堆内存 + 堆外内存 > 总内存）。
// 设置安全阈值：验证计算出的堆内存是否达到了 Flink 推荐的最小启动要求。
public class JobManagerFlinkMemoryUtils implements FlinkMemoryUtils<JobManagerFlinkMemory> {
    private static final Logger LOG = LoggerFactory.getLogger(JobManagerFlinkMemoryUtils.class);
    // 基于细粒度配置项进行推导
    @Override
    public JobManagerFlinkMemory deriveFromRequiredFineGrainedOptions(Configuration config) {
        // 从配置中获取 JVM_HEAP_MEMORY 和 OFF_HEAP_MEMORY 的值
        MemorySize jvmHeapMemorySize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, JobManagerOptions.JVM_HEAP_MEMORY);
        MemorySize offHeapMemorySize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, JobManagerOptions.OFF_HEAP_MEMORY);
        // 检查是否配置了 TOTAL_FLINK_MEMORY
        if (config.contains(JobManagerOptions.TOTAL_FLINK_MEMORY)) {
            // derive network memory from total flink memory, and check against network min/max
            MemorySize totalFlinkMemorySize =
                    ProcessMemoryUtils.getMemorySizeFromConfig(
                            config, JobManagerOptions.TOTAL_FLINK_MEMORY);
            // 冲突处理：如果既有总内存又有子项配置，调用 sanityCheckTotalFlinkMemory 检查它们相加是否等于总和。
            if (config.contains(JobManagerOptions.OFF_HEAP_MEMORY)) {
                // off-heap memory is explicitly set by user
                sanityCheckTotalFlinkMemory(
                        totalFlinkMemorySize, jvmHeapMemorySize, offHeapMemorySize);
            } else {
                // off-heap memory is not explicitly set by user, derive it from Total Flink Memory
                // and JVM Heap
                // 自动填充：如果只配了总内存没配堆外内存，调用 deriveOffHeapMemory 用减法算出堆外内存。
                offHeapMemorySize =
                        deriveOffHeapMemory(
                                jvmHeapMemorySize, totalFlinkMemorySize, offHeapMemorySize);
            }
        }

        return createJobManagerFlinkMemory(jvmHeapMemorySize, offHeapMemorySize);
    }
    // 检查 堆内内存 + 堆外内存 = Flink 总内存
    private static void sanityCheckTotalFlinkMemory(
            MemorySize totalFlinkMemorySize,
            MemorySize jvmHeapMemorySize,
            MemorySize offHeapMemorySize) {
        MemorySize derivedTotalFlinkMemorySize = jvmHeapMemorySize.add(offHeapMemorySize);
        if (derivedTotalFlinkMemorySize.getBytes() != totalFlinkMemorySize.getBytes()) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Sum of the configured JVM Heap Memory (%s) and the configured Off-heap Memory (%s) "
                                    + "does not match the configured Total Flink Memory (%s). Please, make the configuration consistent "
                                    + "or configure only one option: either JVM Heap or Total Flink Memory.",
                            jvmHeapMemorySize.toHumanReadableString(),
                            offHeapMemorySize.toHumanReadableString(),
                            totalFlinkMemorySize.toHumanReadableString()));
        }
    }
    // 执行减法逻辑推导堆外内存
    private static MemorySize deriveOffHeapMemory(
            MemorySize jvmHeapMemorySize,
            MemorySize totalFlinkMemorySize,
            MemorySize defaultOffHeapMemorySize) {
        if (totalFlinkMemorySize.getBytes() < jvmHeapMemorySize.getBytes()) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The configured JVM Heap Memory (%s) exceeds the configured Total Flink Memory (%s). "
                                    + "Please, make the configuration consistent or configure only one option: either JVM Heap "
                                    + "or Total Flink Memory.",
                            jvmHeapMemorySize.toHumanReadableString(),
                            totalFlinkMemorySize.toHumanReadableString()));
        }
        // 堆外内存 = flink总内存 - 堆内内存
        MemorySize offHeapMemorySize = totalFlinkMemorySize.subtract(jvmHeapMemorySize);
        // 如果减出来的结果与默认值不符，会打印一条 INFO 日志告知用户默认值已被忽略，实际以减法结果为准。
        if (offHeapMemorySize.getBytes() != defaultOffHeapMemorySize.getBytes()) {
            LOG.info(
                    "The Off-Heap Memory size ({}) is derived the configured Total Flink Memory size ({}) minus "
                            + "the configured JVM Heap Memory size ({}). The default Off-Heap Memory size ({}) is ignored.",
                    offHeapMemorySize.toHumanReadableString(),
                    totalFlinkMemorySize.toHumanReadableString(),
                    jvmHeapMemorySize.toHumanReadableString(),
                    defaultOffHeapMemorySize.toHumanReadableString());
        }
        return offHeapMemorySize;
    }
    // 从给定的 Flink 总内存容量中切分组件。
    @Override
    public JobManagerFlinkMemory deriveFromTotalFlinkMemory(
            Configuration config, MemorySize totalFlinkMemorySize) {
        // 获取配置中的 OFF_HEAP_MEMORY（如果没有配置则使用默认值，JM 默认为 128MB）
        MemorySize offHeapMemorySize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, JobManagerOptions.OFF_HEAP_MEMORY);
        // 确保总内存大于堆外内存，否则抛出异常。
        if (totalFlinkMemorySize.compareTo(offHeapMemorySize) < 1) {
            throw new IllegalConfigurationException(
                    "The configured Total Flink Memory (%s) is less than the configured Off-heap Memory (%s).",
                    totalFlinkMemorySize.toHumanReadableString(),
                    offHeapMemorySize.toHumanReadableString());
        }
        // 这是最常见的场景，即用户只指定了 JM 的总内存，Flink 扣除掉默认的堆外内存后，把剩下的全给 JVM Heap。
        MemorySize derivedJvmHeapMemorySize = totalFlinkMemorySize.subtract(offHeapMemorySize);

        return createJobManagerFlinkMemory(derivedJvmHeapMemorySize, offHeapMemorySize);
    }

    private static JobManagerFlinkMemory createJobManagerFlinkMemory(
            MemorySize jvmHeap, MemorySize offHeapMemory) {
        verifyJvmHeapSize(jvmHeap);
        return new JobManagerFlinkMemory(jvmHeap, offHeapMemory);
    }

    private static void verifyJvmHeapSize(MemorySize jvmHeapSize) {
        if (jvmHeapSize.compareTo(JobManagerOptions.MIN_JVM_HEAP_SIZE) < 0) {
            LOG.warn(
                    "The configured or derived JVM heap memory size ({}) is less than its recommended minimum value ({})",
                    jvmHeapSize.toHumanReadableString(),
                    JobManagerOptions.MIN_JVM_HEAP_SIZE.toHumanReadableString());
        }
    }
}
