/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava32.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava32.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** Default implementation of {@link PluginManager}. */
// DefaultPluginManager 是 Flink 插件机制（Plugin System）的核心默认实现。
// 它承接了之前提到的 PluginManager 接口，负责具体管理插件的生命周期、类加载器的维护以及服务发现。
// DefaultPluginManager 的核心作用是 “隔离加载与按需分发”。
// 类加载器缓存：为每个插件维护一个独立的 PluginLoader。这意味着每个插件都有自己独立的 ClassLoader，从而实现与 Flink 核心库及其他插件的依赖隔离。
// SPI 服务聚合：当用户请求某种服务（如 FileSystemFactory）时，它会遍历所有插件的加载器，并将所有找到的实现类聚合成一个统一的迭代器返回。
@Internal
@ThreadSafe
public class DefaultPluginManager implements PluginManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPluginManager.class);

    /**
     * Parent-classloader to all classloader that are used for plugin loading. We expect that this
     * is thread-safe.
     */
    // 所有插件类加载器的“父加载器”。
    // 通常是 Flink 的系统类加载器（AppClassLoader），确保插件能访问到 Flink 核心 API。
    private final ClassLoader parentClassLoader;

    /** A collection of descriptions of all plugins known to this plugin manager. */
    // 多插件管理：它持有一组 PluginDescriptor（插件描述符），代表了物理磁盘上发现的所有插件。
    // 存储插件的描述信息，包括插件的 ID、JAR 包路径等元数据。
    private final Collection<PluginDescriptor> pluginDescriptors;
    // 互斥锁，保护 pluginLoaders 映射表的并发访问，确保同一个插件不会被初始化两次。
    private final Lock pluginLoadersLock;
    // 插件加载器缓存。Key 是插件 ID，Value 是对应的加载器实例。这避免了重复创建加载器带来的性能开销和内存泄漏。
    @GuardedBy("pluginLoadersLock")
    private final Map<String, PluginLoader> pluginLoaders;

    /** List of patterns for classes that should always be resolved from the parent ClassLoader. */
    // 父加载器优先模式的类过滤规则。
    // 定义哪些包路径下的类必须由 parentClassLoader 加载（如 org.apache.flink.*），以防止插件包里自带的 Flink 核心类导致冲突。
    private final String[] alwaysParentFirstPatterns;

    @VisibleForTesting
    DefaultPluginManager() {
        parentClassLoader = null;
        pluginDescriptors = null;
        pluginLoadersLock = null;
        pluginLoaders = null;
        alwaysParentFirstPatterns = null;
    }

    public DefaultPluginManager(
            Collection<PluginDescriptor> pluginDescriptors, String[] alwaysParentFirstPatterns) {
        this(
                pluginDescriptors,
                DefaultPluginManager.class.getClassLoader(),
                alwaysParentFirstPatterns);
    }

    public DefaultPluginManager(
            Collection<PluginDescriptor> pluginDescriptors,
            ClassLoader parentClassLoader,
            String[] alwaysParentFirstPatterns) {
        this.pluginDescriptors = pluginDescriptors;
        this.pluginLoadersLock = new ReentrantLock();
        this.pluginLoaders = new HashMap<>();
        this.parentClassLoader = parentClassLoader;
        this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
    }

    @Override
    public <P> Iterator<P> load(Class<P> service) {
        ArrayList<Iterator<P>> combinedIterators = new ArrayList<>(pluginDescriptors.size());
        for (PluginDescriptor pluginDescriptor : pluginDescriptors) {
            PluginLoader pluginLoader;
            String pluginId = pluginDescriptor.getPluginId();
            pluginLoadersLock.lock();
            try {
                if (pluginLoaders.containsKey(pluginId)) {
                    LOG.info("Plugin loader with ID found, reusing it: {}", pluginId);
                    pluginLoader = pluginLoaders.get(pluginId);
                } else {
                    LOG.info("Plugin loader with ID not found, creating it: {}", pluginId);
                    pluginLoader =
                            PluginLoader.create(
                                    pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns);
                    pluginLoaders.putIfAbsent(pluginId, pluginLoader);
                }
            } finally {
                pluginLoadersLock.unlock();
            }
            combinedIterators.add(pluginLoader.load(service));
        }
        return Iterators.concat(combinedIterators.iterator());
    }

    @Override
    public String toString() {
        return "PluginManager{"
                + "parentClassLoader="
                + parentClassLoader
                + ", pluginDescriptors="
                + pluginDescriptors
                + ", pluginLoaders="
                + Joiner.on(",").withKeyValueSeparator("=").join(pluginLoaders)
                + ", alwaysParentFirstPatterns="
                + Arrays.toString(alwaysParentFirstPatterns)
                + '}';
    }
}
