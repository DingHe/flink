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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.config.memory.FlinkMemoryUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.RangeFraction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkMemoryUtils} for Task Executor.
 *
 * <p>The required fine-grained components are {@link TaskManagerOptions#TASK_HEAP_MEMORY} and
 * {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}.
 */
// TaskExecutorFlinkMemoryUtils 是 Flink 内存管理系统中最复杂、最核心的推导类之一。
// 它实现了 FlinkMemoryUtils 接口，专门负责计算 TaskManager (TaskExecutor) 的内存布局。
// 相比 JobManager，TaskManager 的内存组件更多（包括网络内存、托管内存、框架内存等），且支持复杂的比例分配（Fraction）和范围限制（Min/Max）。
// 该类的主要作用是根据用户在 flink-conf.yaml 中给出的不完整或混合配置，推导出 TaskManager 的 Total Flink Memory 内部的所有 6 个组件
// Framework Heap: 框架堆内存
// Framework Off-Heap: 框架堆外内存
// Task Heap: 任务堆内存（算子运行空间）
// Task Off-Heap: 任务堆外内存
// Network Memory: 网络缓冲内存（用于 Shuffle）
// Managed Memory: 托管内存（用于 RocksDB 或批处理排序）

public class TaskExecutorFlinkMemoryUtils implements FlinkMemoryUtils<TaskExecutorFlinkMemory> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorFlinkMemoryUtils.class);

    // 当用户已经显式配置了“任务堆内存”和“托管内存”等细粒度项时，如何推导出 TaskManager 的完整内存模型。
    @Override
    public TaskExecutorFlinkMemory deriveFromRequiredFineGrainedOptions(Configuration config) {
        // 获取用户必须显式定义的两项核心内存：任务堆内存（用于算子执行）和托管内存（用于 RocksDB/批处理）
        final MemorySize taskHeapMemorySize = getTaskHeapMemorySize(config);
        final MemorySize managedMemorySize = getManagedMemorySize(config);
        // 获取框架级内存和任务堆外内存。
        // 这些通常有默认值（如框架内存通常是 128MB），Flink 会从配置中读取或使用默认值。
        final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
        final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
        final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

        final MemorySize networkMemorySize;
        final MemorySize totalFlinkExcludeNetworkMemorySize =
                frameworkHeapMemorySize
                        .add(frameworkOffHeapMemorySize)
                        .add(taskHeapMemorySize)
                        .add(taskOffHeapMemorySize)
                        .add(managedMemorySize);

        // 计算出除了**网络内存（Network Memory）**之外，Flink 内部组件总共占用了多少空间。
        // 这是为了后续通过“减法”或“反推”来确定网络内存。
        // 情况 A：用户配置了 Flink 总内存
        if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
            // derive network memory from total flink memory, and check against network min/max
            final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
            // 如果各个子项加起来已经超过了设定的总内存，说明配置矛盾，抛出异常
            if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Heap Memory ("
                                + taskHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + ") and Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            // $网络内存 = 总内存 - 其他组件总和$。计算完成后
            // 还要调用 sanityCheck 确保这个网络内存值符合 Min/Max 的范围限制。
            networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
            sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
                    config, networkMemorySize, totalFlinkMemorySize);
        } else {
         // 情况 B：用户没配 Flink 总内存
         // 如果用户用了旧版参数（如 network-num-buffers），则按旧逻辑算。
         // 如果没配总内存，Flink 会使用 deriveNetworkMemoryWithInverseFraction。
         // 由于网络内存是按比例定义的（默认 0.1），公式变为：$Network = \frac{OthersSum \times Fraction}{1 - Fraction}$。
         // 通过这种方式，在不知道总量的情况下，根据已知项和比例强行算出网络内存。
            // derive network memory from network configs
            networkMemorySize =
                    isUsingLegacyNetworkConfigs(config)
                            ? getNetworkMemorySizeWithLegacyConfig(config)
                            : deriveNetworkMemoryWithInverseFraction(
                                    config, totalFlinkExcludeNetworkMemorySize);
        }

        final TaskExecutorFlinkMemory flinkInternalMemory =
                new TaskExecutorFlinkMemory(
                        frameworkHeapMemorySize,
                        frameworkOffHeapMemorySize,
                        taskHeapMemorySize,
                        taskOffHeapMemorySize,
                        networkMemorySize,
                        managedMemorySize);
        sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

        return flinkInternalMemory;
    }
    // 核心任务是：在已知“Flink 总内存”的情况下，如何合理地把这块大蛋糕切分给堆内存、托管内存和网络内存。
    @Override
    public TaskExecutorFlinkMemory deriveFromTotalFlinkMemory(
            final Configuration config, final MemorySize totalFlinkMemorySize) {
        // 获取相对固定的内存组件大小
        // 这些项通常有默认值（如 Framework Heap 默认为 128MB），且不随总内存比例变化。它们是第一批被扣除的“死权重”。
        final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
        final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
        final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config); // 默认值为0

        final MemorySize taskHeapMemorySize;
        final MemorySize networkMemorySize;
        final MemorySize managedMemorySize;
        // 如果用户硬性规定了任务堆的大小，则先满足这个需求。
        // 同时按比例或固定值算出托管内存（Managed Memory）。
        if (isTaskHeapMemorySizeExplicitlyConfigured(config)) {
            // task heap memory is configured,
            // derive managed memory first, leave the remaining to network memory and check against
            // network min/max
            taskHeapMemorySize = getTaskHeapMemorySize(config);
            // 管理内存默认是flink 总内存的0.4
            managedMemorySize =
                    deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
            final MemorySize totalFlinkExcludeNetworkMemorySize =
                    frameworkHeapMemorySize
                            .add(frameworkOffHeapMemorySize)
                            .add(taskHeapMemorySize)
                            .add(taskOffHeapMemorySize)
                            .add(managedMemorySize);
            if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Heap Memory ("
                                + taskHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + ") and Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            // 网络内存填补剩余
            // 在这种模式下，网络内存（Network Memory）变成了“残差”。
            // 系统用总内存减去其他所有项，剩下的全给网络。如果剩下的钱不够（变为负数），则抛出配置异常。
            networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
            sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
                    config, networkMemorySize, totalFlinkMemorySize);
        } else {
            // 分支 B：用户没有配置 task.heap.size（最推荐的默认模式）
            // task heap memory is not configured
            // derive managed memory and network memory, leave the remaining to task heap memory
            // 先根据各自的比例（Fraction）从总内存中切出托管内存和网络内存。
            // 例如，默认情况下托管内存切走 40%，
            managedMemorySize =
                    deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
            // 网络内存切走 10%。
            networkMemorySize =
                    isUsingLegacyNetworkConfigs(config)
                            ? getNetworkMemorySizeWithLegacyConfig(config)
                            : deriveNetworkMemoryWithFraction(config, totalFlinkMemorySize);
            final MemorySize totalFlinkExcludeTaskHeapMemorySize =
                    frameworkHeapMemorySize
                            .add(frameworkOffHeapMemorySize)
                            .add(taskOffHeapMemorySize)
                            .add(managedMemorySize)
                            .add(networkMemorySize);
            if (totalFlinkExcludeTaskHeapMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + "), Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") and Network Memory ("
                                + networkMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            // 在这种模式下，Task Heap 变成了最后确定的项。
            // 系统扣除掉所有框架开销、堆外开销、托管和网络内存后，把剩下的空间全部留给任务执行（Task Heap）。这保证了资源利用率最大化。
            taskHeapMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeTaskHeapMemorySize);
        }

        final TaskExecutorFlinkMemory flinkInternalMemory =
                new TaskExecutorFlinkMemory(
                        frameworkHeapMemorySize,
                        frameworkOffHeapMemorySize,
                        taskHeapMemorySize,
                        taskOffHeapMemorySize,
                        networkMemorySize,
                        managedMemorySize);
        sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

        return flinkInternalMemory;
    }
    // 管理内存如何不配置，默认是0.4
    private static MemorySize deriveManagedMemoryAbsoluteOrWithFraction(
            final Configuration config, final MemorySize base) {
        return isManagedMemorySizeExplicitlyConfigured(config)
                ? getManagedMemorySize(config)
                : ProcessMemoryUtils.deriveWithFraction(
                        "managed memory", base, getManagedMemoryRangeFraction(config));
    }

    private static MemorySize deriveNetworkMemoryWithFraction(
            final Configuration config, final MemorySize base) {
        return ProcessMemoryUtils.deriveWithFraction(
                "network memory", base, getNetworkMemoryRangeFraction(config));
    }

    private static MemorySize deriveNetworkMemoryWithInverseFraction(
            final Configuration config, final MemorySize base) {
        return ProcessMemoryUtils.deriveWithInverseFraction(
                "network memory", base, getNetworkMemoryRangeFraction(config));
    }

    public static MemorySize getFrameworkHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY);
    }

    public static MemorySize getFrameworkOffHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY);
    }

    private static MemorySize getTaskHeapMemorySize(final Configuration config) {
        checkArgument(isTaskHeapMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TASK_HEAP_MEMORY);
    }

    private static MemorySize getTaskOffHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TASK_OFF_HEAP_MEMORY);
    }

    private static MemorySize getManagedMemorySize(final Configuration config) {
        checkArgument(isManagedMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.MANAGED_MEMORY_SIZE);
    }

    private static RangeFraction getManagedMemoryRangeFraction(final Configuration config) {
        return ProcessMemoryUtils.getRangeFraction(
                MemorySize.ZERO,
                MemorySize.MAX_VALUE,
                TaskManagerOptions.MANAGED_MEMORY_FRACTION,
                config);
    }
    // 当用户依然使用 Flink 1.10 之前的旧版配置方式时，根据“缓冲区数量”来计算网络内存的总字节大小。
    private static MemorySize getNetworkMemorySizeWithLegacyConfig(final Configuration config) {
        checkArgument(isUsingLegacyNetworkConfigs(config));
        @SuppressWarnings("deprecation")
        // 获取缓冲区数量。
        // 从配置对象中读取 taskmanager.network.numberOfBuffers 这个 Key 的值。
        final long numOfBuffers = config.get(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
        // 获取内存页大小（Page Size）
        final long pageSize = ConfigurationParserUtils.getPageSize(config);
        // 计算并返回总内存大小。
        // $网络内存总字节数 = 缓冲区数量 \times 单个缓冲区的大小$
        return new MemorySize(numOfBuffers * pageSize);
    }

    private static RangeFraction getNetworkMemoryRangeFraction(final Configuration config) {
        final MemorySize minSize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, TaskManagerOptions.NETWORK_MEMORY_MIN);
        final MemorySize maxSize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, TaskManagerOptions.NETWORK_MEMORY_MAX);
        return ProcessMemoryUtils.getRangeFraction(
                minSize, maxSize, TaskManagerOptions.NETWORK_MEMORY_FRACTION, config);
    }

    private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
        checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TOTAL_FLINK_MEMORY);
    }

    private static boolean isTaskHeapMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.TASK_HEAP_MEMORY);
    }
    // 是否显示配置类管理内存
    private static boolean isManagedMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE);
    }

    // 核心作用是：判定当前配置是否应该激活旧版本的网络内存计算逻辑。
    // 在 Flink 1.10 之后，官方推荐使用比例（Fraction）和范围（Min/Max）来设置网络内存，而在此之前是直接设置缓冲区的数量。
    private static boolean isUsingLegacyNetworkConfigs(final Configuration config) {
        // use the legacy number-of-buffer config option only when it is explicitly configured and
        // none of new config options is explicitly configured
        // 检测用户是否使用了 新版（1.10+） 的内存配置项
        final boolean anyNetworkConfigured =
                config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN)
                        || config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX)
                        || config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
        // 检查旧版配置是否存在
        final boolean legacyConfigured =
                config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
        // 决定最终是否“判定”为使用旧逻辑。
        return !anyNetworkConfigured && legacyConfigured;
    }

    private static boolean isNetworkMemoryFractionExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
    }

    private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY);
    }
    // 心作用是：验证“推导出的各组件内存之和”是否等于“用户配置的总内存大小”。这能防止因为用户配置冲突或程序计算逻辑错误导致的内存分配不均。
    private static void sanityCheckTotalFlinkMemory(
            final Configuration config, final TaskExecutorFlinkMemory flinkInternalMemory) {
        // 判断用户是否显式配置了 taskmanager.memory.flink.size（Flink 总内存）
        if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
            // 从配置中读取用户设定的 Flink 总内存具体数值（例如：4GB）
            final MemorySize configuredTotalFlinkMemorySize = getTotalFlinkMemorySize(config);
            // 关键一致性检查。
            if (!configuredTotalFlinkMemorySize.equals(
                    flinkInternalMemory.getTotalFlinkMemorySize())) {
                throw new IllegalConfigurationException(
                        "Configured and Derived Flink internal memory sizes (total "
                                + flinkInternalMemory
                                        .getTotalFlinkMemorySize()
                                        .toHumanReadableString()
                                + ") do not add up to the configured Total Flink Memory size ("
                                + configuredTotalFlinkMemorySize.toHumanReadableString()
                                + "). Configured and Derived Flink internal memory sizes are: "
                                + "Framework Heap Memory ("
                                + flinkInternalMemory.getFrameworkHeap().toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + flinkInternalMemory.getFrameworkOffHeap().toHumanReadableString()
                                + "), Task Heap Memory ("
                                + flinkInternalMemory.getTaskHeap().toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + flinkInternalMemory.getTaskOffHeap().toHumanReadableString()
                                + "), Network Memory ("
                                + flinkInternalMemory.getNetwork().toHumanReadableString()
                                + "), Managed Memory ("
                                + flinkInternalMemory.getManaged().toHumanReadableString()
                                + ").");
            }
        }
    }

    private static void sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
            final Configuration config,
            final MemorySize derivedNetworkMemorySize,
            final MemorySize totalFlinkMemorySize) {
        try {
            sanityCheckNetworkMemory(config, derivedNetworkMemorySize, totalFlinkMemorySize);
        } catch (IllegalConfigurationException e) {
            throw new IllegalConfigurationException(
                    "If Total Flink, Task Heap and (or) Managed Memory sizes are explicitly configured then "
                            + "the Network Memory size is the rest of the Total Flink memory after subtracting all other "
                            + "configured types of memory, but the derived Network Memory is inconsistent with its configuration.",
                    e);
        }
    }

    private static void sanityCheckNetworkMemory(
            final Configuration config,
            final MemorySize derivedNetworkMemorySize,
            final MemorySize totalFlinkMemorySize) {
        if (isUsingLegacyNetworkConfigs(config)) {
            final MemorySize configuredNetworkMemorySize =
                    getNetworkMemorySizeWithLegacyConfig(config);
            if (!configuredNetworkMemorySize.equals(derivedNetworkMemorySize)) {
                throw new IllegalConfigurationException(
                        "Derived Network Memory size ("
                                + derivedNetworkMemorySize.toHumanReadableString()
                                + ") does not match configured Network Memory size ("
                                + configuredNetworkMemorySize.toHumanReadableString()
                                + ").");
            }
        } else {
            final RangeFraction networkRangeFraction = getNetworkMemoryRangeFraction(config);
            if (derivedNetworkMemorySize.getBytes() > networkRangeFraction.getMaxSize().getBytes()
                    || derivedNetworkMemorySize.getBytes()
                            < networkRangeFraction.getMinSize().getBytes()) {
                throw new IllegalConfigurationException(
                        "Derived Network Memory size ("
                                + derivedNetworkMemorySize.toHumanReadableString()
                                + ") is not in configured Network Memory range ["
                                + networkRangeFraction.getMinSize().toHumanReadableString()
                                + ", "
                                + networkRangeFraction.getMaxSize().toHumanReadableString()
                                + "].");
            }
            if (isNetworkMemoryFractionExplicitlyConfigured(config)
                    && !derivedNetworkMemorySize.equals(
                            totalFlinkMemorySize.multiply(networkRangeFraction.getFraction()))) {
                LOG.info(
                        "The derived Network Memory size ({}) does not match "
                                + "the configured Network Memory fraction ({}) from the configured Total Flink Memory size ({}). "
                                + "The derived Network Memory size will be used.",
                        derivedNetworkMemorySize.toHumanReadableString(),
                        networkRangeFraction.getFraction(),
                        totalFlinkMemorySize.toHumanReadableString());
            }
        }
    }
}
