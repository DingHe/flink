/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ServiceLoader;

import static org.apache.flink.shaded.guava32.com.google.common.collect.Iterators.concat;

/** A thin wrapper around {@link PluginManager} to load {@link StateChangelogStorage}. */
@Internal
public class StateChangelogStorageLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateChangelogStorageLoader.class);

    /**
     * Mapping of state changelog storage identifier to the corresponding storage factories,
     * populated in {@link StateChangelogStorageLoader#initialize(PluginManager)}.
     */
    private static final HashMap<String, StateChangelogStorageFactory>
            STATE_CHANGELOG_STORAGE_FACTORIES = new HashMap<>();

    static {
        // Guarantee to trigger once.
        initialize(null);
    }
    // 该方法通过 SPI 机制 和 插件隔离加载机制，为 Flink 提供了一个可扩展的 Changelog 存储发现系统。
    // 它允许用户通过简单的配置字符串（Identifier）来切换底层存储，而不需要修改 Flink 的核心代码。
    public static void initialize(PluginManager pluginManager) {
        STATE_CHANGELOG_STORAGE_FACTORIES.clear();
        // 情况 A (pluginManager == null)：仅从当前的 Classpath 下通过 Java 原生 ServiceLoader 加载实现类。
        // 情况 B (pluginManager != null)：双渠道加载。它会同时从 Flink 的插件目录（通过 pluginManager）和标准的 Classpath（通过 ServiceLoader）加载工厂，并使用 concat 方法将两者的迭代器合并。
        // 这保证了无论是内置实现还是用户自定义插件都能被识别。
        Iterator<StateChangelogStorageFactory> iterator =
                pluginManager == null
                        ? ServiceLoader.load(StateChangelogStorageFactory.class).iterator()
                        : concat(
                                pluginManager.load(StateChangelogStorageFactory.class),
                                ServiceLoader.load(StateChangelogStorageFactory.class).iterator());
        iterator.forEachRemaining(
                factory -> {
                    // 遍历找到的所有工厂，并获取其“标识符（Identifier）”
                    // getIdentifier()：每个工厂都会定义一个唯一的快捷名称（如 memory、filesystem 或 dstl）。
                    String identifier = factory.getIdentifier().toLowerCase();
                    StateChangelogStorageFactory prev =
                            STATE_CHANGELOG_STORAGE_FACTORIES.get(identifier);
                    if (prev == null) {
                        STATE_CHANGELOG_STORAGE_FACTORIES.put(identifier, factory);
                    } else {
                        LOG.warn(
                                "StateChangelogStorageLoader found duplicated factory,"
                                        + " using {} instead of {} for name {}.",
                                prev.getClass().getName(),
                                factory.getClass().getName(),
                                identifier);
                    }
                });
        LOG.info(
                "StateChangelogStorageLoader initialized with shortcut names {{}}.",
                String.join(",", STATE_CHANGELOG_STORAGE_FACTORIES.keySet()));
    }

    @Nullable
    public static StateChangelogStorage<?> load(
            JobID jobID,
            Configuration configuration,
            TaskManagerJobMetricGroup metricGroup,
            LocalRecoveryConfig localRecoveryConfig)
            throws IOException {
        final String identifier =
                configuration.get(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE).toLowerCase();

        StateChangelogStorageFactory factory = STATE_CHANGELOG_STORAGE_FACTORIES.get(identifier);
        if (factory == null) {
            LOG.warn("Cannot find a factory for changelog storage with name '{}'.", identifier);
            return null;
        } else {
            LOG.info("Creating a changelog storage with name '{}'.", identifier);
            return factory.createStorage(jobID, configuration, metricGroup, localRecoveryConfig);
        }
    }

    @Nonnull
    public static StateChangelogStorageView<?> loadFromStateHandle(
            Configuration configuration, ChangelogStateHandle changelogStateHandle)
            throws IOException {
        StateChangelogStorageFactory factory =
                STATE_CHANGELOG_STORAGE_FACTORIES.get(changelogStateHandle.getStorageIdentifier());
        if (factory == null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Cannot find a factory for changelog storage with name '%s' to restore from '%s'.",
                            changelogStateHandle.getStorageIdentifier(),
                            changelogStateHandle.getClass().getSimpleName()));
        } else {
            LOG.info(
                    "Creating a changelog storage with name '{}' to restore from '{}'.",
                    changelogStateHandle.getStorageIdentifier(),
                    changelogStateHandle.getClass().getSimpleName());
            return factory.createStorageView(configuration);
        }
    }
}
