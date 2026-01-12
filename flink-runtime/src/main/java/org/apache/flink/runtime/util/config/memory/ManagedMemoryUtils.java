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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.state.StateBackendLoader;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** Utils for configuration and calculations related to managed memory and its various use cases. */
// ManagedMemoryUtils 是一个至关重要的工具类。它主要负责处理 托管内存（Managed Memory） 在不同使用场景（Use Case）之间的分配比例计算。
// 由于托管内存总量是确定的，但可能有多个消费者（如 RocksDB 状态后端、批处理算子、Python 进程）同时竞争这块内存。
// 该类通过读取配置中的 权重（Weights），计算出每个消费者在当前槽位（Slot）中应该获得的 比例（Fraction）
// 解析权重配置：从配置文件中读取各个内存消费者的权重。
// 计算 Slot 比例：将某个具体用途的比例转化为占整个 Slot 托管内存的最终比例。
// 处理状态后端逻辑：自动判断当前状态后端是否真的需要托管内存（例如 HeapStateBackend 就不需要）。
// 校验一致性：确保内存配置在不同层级（如集群级和作业级）没有发生冲突。
public enum ManagedMemoryUtils {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(ManagedMemoryUtils.class);
    // 计算内存比例时保留的小数点精度
    private static final int MANAGED_MEMORY_FRACTION_SCALE = 16;

    /** Names of managed memory use cases, in the fallback order. */
    // 定义了内存用途与配置项名称之间的映射和回退（Fallback）关系
    // OPERATOR（算子）：优先查找 operator，找不到则找 dataproc。
    // STATE_BACKEND（状态后端）：优先查找 state_backend，找不到则找 dataproc
    // PYTHON：查找 python
    @SuppressWarnings("deprecation")
    private static final Map<ManagedMemoryUseCase, List<String>> USE_CASE_CONSUMER_NAMES =
            ImmutableMap.of(
                    ManagedMemoryUseCase.OPERATOR,
                    ImmutableList.of(
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_OPERATOR,
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC),
                    ManagedMemoryUseCase.STATE_BACKEND,
                    ImmutableList.of(
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_STATE_BACKEND,
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC),
                    ManagedMemoryUseCase.PYTHON,
                    ImmutableList.of(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_PYTHON));
    // 计算某个用途最终占 Slot 托管内存的百分比。
    // 核心任务是：计算某个具体的内存用途（如 RocksDB 或 Python 进程）最终在当前 Task Slot 托管内存中占据的绝对比例。
    public static double convertToFractionOfSlot(
            ManagedMemoryUseCase useCase,
            double fractionOfUseCase,
            Set<ManagedMemoryUseCase> allUseCases,
            Configuration jobConfig,
            Configuration clusterConfig,
            Optional<Boolean> stateBackendFromApplicationUsesManagedMemory,
            ClassLoader classLoader) {
        // 创建一个新的配置对象，将集群默认配置（clusterConfig）和作业级配置（jobConfig）合并。
        Configuration config = new Configuration(clusterConfig);
        config.addAll(jobConfig);
        // 判断当前作业使用的**状态后端（State Backend）**是否需要使用托管内存
        // 例如，RocksDB 需要托管内存（用于 Block Cache 等），而 HashMap 状态后端存储在 JVM 堆内，不占用托管内存。
        final boolean stateBackendUsesManagedMemory =
                StateBackendLoader.stateBackendFromApplicationOrConfigOrDefaultUseManagedMemory(
                        config, stateBackendFromApplicationUsesManagedMemory, classLoader);
        // 如果当前请求计算的是“状态后端”的比例，但实际用的后端根本不需要托管内存，则直接返回 0.0
        if (useCase.equals(ManagedMemoryUseCase.STATE_BACKEND) && !stateBackendUsesManagedMemory) {
            return 0.0;
        }
        // 从配置中加载所有用途的**权重（Weights）**映射表
        final Map<ManagedMemoryUseCase, Integer> allUseCaseWeights =
                getManagedMemoryUseCaseWeightsFromConfig(clusterConfig);
        // 计算当前场景下所有有效用途的总权重之和
        final int totalWeights =
                allUseCases.stream()
                        .filter(
                                (uc) ->
                                        !uc.equals(ManagedMemoryUseCase.STATE_BACKEND)
                                                || stateBackendUsesManagedMemory)
                        .mapToInt((uc) -> allUseCaseWeights.getOrDefault(uc, 0))
                        .sum();
        // 计算指定的 useCase 占整个 Slot 托管内存的比例
        final int useCaseWeight = allUseCaseWeights.getOrDefault(useCase, 0);
        final double useCaseFractionOfSlot =
                totalWeights > 0 ? getFractionRoundedDown(useCaseWeight, totalWeights) : 0.0;
        // 将“用途占 Slot 的比例”乘以“具体请求占用途的比例”，得出最终结果
        // useCaseFractionOfSlot 是宏观比例（如：状态后端占 Slot 的 70%）
        // fractionOfUseCase 是微观比例（如：某个特定算子只占状态后端份额的 50%）
        return fractionOfUseCase * useCaseFractionOfSlot;
    }
    // 核心作用是：从配置中解析并确定不同托管内存用途（Use Case）的权重。
    // Flink 允许用户通过 taskmanager.memory.managed.consumer-weights 参数手动调整内存分配比例，该方法负责将这些配置字符串转化为程序可读的权重数值映射表。

    @VisibleForTesting
    static Map<ManagedMemoryUseCase, Integer> getManagedMemoryUseCaseWeightsFromConfig(
            Configuration config) {
        // 从 Flink 配置中读取 taskmanager.memory.managed.consumer-weights。
        // 在配置文件中，这通常是一个键值对列表（例如 state_backend:70,operator:30）
        final Map<String, String> configuredWeights =
                config.get(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS);
        // 初始化一个空的 HashMap，用于存放解析后最终生效的权重（Key 是枚举类型，Value 是整数）
        final Map<ManagedMemoryUseCase, Integer> effectiveWeights = new HashMap<>();

        for (Map.Entry<ManagedMemoryUseCase, List<String>> entry :
                USE_CASE_CONSUMER_NAMES.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            // 获取该用途对应的配置别名列表。
            // 这是为了兼容性考虑（比如 STATE_BACKEND 对应的别名可能是 state_backend 或旧版的 dataproc
            final Iterator<String> nameIter = entry.getValue().iterator();
            // 表示是否已经为当前用途找到了权重
            boolean findWeight = false;
            while (!findWeight && nameIter.hasNext()) {
                final String name = nameIter.next();
                final String weightStr = configuredWeights.get(name);
                // 将字符串类型的权重转换为整数
                if (weightStr != null) {
                    final int weight = Integer.parseInt(weightStr);
                    findWeight = true;

                    if (weight < 0) {
                        throw new IllegalConfigurationException(
                                String.format(
                                        "Managed memory weight should not be negative. Configured "
                                                + "weight for %s is %d.",
                                        useCase, weight));
                    }

                    if (weight == 0) {
                        LOG.debug(
                                "Managed memory consumer weight for {} is configured to 0. Jobs "
                                        + "containing this type of managed memory consumers may "
                                        + "fail due to not being able to allocate managed memory.",
                                useCase);
                    }

                    effectiveWeights.put(useCase, weight);
                }
            }

            if (!findWeight) {
                LOG.debug(
                        "Managed memory consumer weight for {} is not configured. Jobs containing "
                                + "this type of managed memory consumers may fail due to not being "
                                + "able to allocate managed memory.",
                        useCase);
            }
        }

        return effectiveWeights;
    }

    public static double getFractionRoundedDown(final long dividend, final long divisor) {
        return BigDecimal.valueOf(dividend)
                .divide(
                        BigDecimal.valueOf(divisor),
                        MANAGED_MEMORY_FRACTION_SCALE,
                        BigDecimal.ROUND_DOWN)
                .doubleValue();
    }

    public static void validateUseCaseWeightsNotConflict(
            Map<ManagedMemoryUseCase, Integer> weights1,
            Map<ManagedMemoryUseCase, Integer> weights2) {
        weights1.forEach(
                (useCase, weight1) ->
                        checkState(
                                weights2.getOrDefault(useCase, weight1).equals(weight1),
                                String.format(
                                        "Conflict managed memory consumer weights for '%s' were configured: '%d' and '%d'.",
                                        useCase, weight1, weights2.get(useCase))));
    }
}
