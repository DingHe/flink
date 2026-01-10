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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Utilities to fallback to new options from the legacy ones for the backwards compatibility.
 *
 * <p>If {@link LegacyMemoryOptions} are set, they are interpreted as other new memory options for
 * the backwards compatibility.
 */
public class MemoryBackwardsCompatibilityUtils {
    private static final Logger LOG =
            LoggerFactory.getLogger(MemoryBackwardsCompatibilityUtils.class);

    private final LegacyMemoryOptions legacyMemoryOptions;

    public MemoryBackwardsCompatibilityUtils(LegacyMemoryOptions legacyMemoryOptions) {
        this.legacyMemoryOptions = legacyMemoryOptions;
    }
    // 当用户没有配置新的内存选项时，自动将旧版的堆内存配置“映射”或“平替”到新的配置项中。
    // configuration：当前集群的配置对象（包含用户在 flink-conf.yaml 中定义的所有内容）。
    // configOption：目标新配置项（通常是 jobmanager.memory.process.size 或 taskmanager.memory.process.size）。
    public Configuration getConfWithLegacyHeapSizeMappedToNewConfigOption(
            Configuration configuration, ConfigOption<MemorySize> configOption) {
        // 首先检查用户是否已经配置了新版的选项。
        // 如果用户已经在配置文件里明确写了新参数（比如 jobmanager.memory.process.size: 2g），那么 Flink 认为用户已经遵循了新的内存模型，
        // 直接返回原始配置，不再去寻找旧版参数，以防冲突。
        if (configuration.contains(configOption)) {
            return configuration;
        }
        // 如果新版选项不存在,则在旧版代码里面查找
        return getLegacyHeapMemoryIfExplicitlyConfigured(configuration)
                .map(
                        legacyHeapSize -> {
                            Configuration copiedConfig = new Configuration(configuration);
                            copiedConfig.set(configOption, legacyHeapSize);
                            LOG.info(
                                    "'{}' is not specified, use the configured deprecated task manager heap value ({}) for it.",
                                    configOption.key(),
                                    legacyHeapSize.toHumanReadableString());
                            return copiedConfig;
                        })
                .orElse(configuration);
    }
    // 为了兼容旧版本的 Flink 配置，尝试从环境变量或旧版配置项中提取显式设置的堆内存（Heap Memory）大小
    // 在 Flink 1.10 引入新的内存模型后，许多旧的参数（如 jobmanager.heap.size 或环境变量）被废弃，但为了保证用户升级后旧配置依然有效，Flink 通过这个方法进行“兜底”检查。
    private Optional<MemorySize> getLegacyHeapMemoryIfExplicitlyConfigured(
            Configuration configuration) {
        @SuppressWarnings("CallToSystemGetenv")
        // 尝试从操作系统的环境变量中获取内存设置
        // legacyMemoryOptions.getEnvVar() 返回的是旧版特定的环境变量名（例如 _FLINK_JM_HEAP_MB）。
        // 这通常是在 YARN 或 K8s 环境中，由启动脚本设置并传递给进程的。
        String totalProcessEnv = System.getenv(legacyMemoryOptions.getEnvVar());
        if (totalProcessEnv != null) {
            //noinspection OverlyBroadCatchBlock
            try {
                return Optional.of(MemorySize.parse(totalProcessEnv));
            } catch (Throwable t) {
                throw new IllegalConfigurationException(
                        "Cannot read total process memory size from environment variable value "
                                + totalProcessEnv
                                + '.',
                        t);
            }
        }
        // 检查 flink-conf.yaml 中是否显式配置了旧版的堆内存参数。
        // legacyMemoryOptions.getHeap() 通常指 jobmanager.heap.size 或 taskmanager.heap.size。
        if (configuration.contains(legacyMemoryOptions.getHeap())) {
            return Optional.of(
                    ProcessMemoryUtils.getMemorySizeFromConfig(
                            configuration, legacyMemoryOptions.getHeap()));
        }
        // 检查是否配置了仅支持数值（单位固定为 MB）的极旧版本参数。
        if (configuration.contains(legacyMemoryOptions.getHeapMb())) {
            final long legacyHeapMemoryMB = configuration.get(legacyMemoryOptions.getHeapMb());
            if (legacyHeapMemoryMB < 0) {
                throw new IllegalConfigurationException(
                        "Configured total process memory size ("
                                + legacyHeapMemoryMB
                                + "MB) must not be less than 0.");
            }
            //noinspection MagicNumber
            return Optional.of(new MemorySize(legacyHeapMemoryMB << 20)); // megabytes to bytes;
        }
        // 如果没有找到任何显式的旧版配置。
        return Optional.empty();
    }
}
