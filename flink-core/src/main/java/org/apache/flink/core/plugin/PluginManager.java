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

import java.util.Iterator;

/**
 * PluginManager is responsible for managing cluster plugins which are loaded using separate class
 * loaders so that their dependencies don't interfere with Flink's dependencies.
 */
// 在 Flink 的架构设计中，PluginManager 是实现插件化机制的核心接口。它的存在使得 Flink 能够以一种解耦的方式扩展功能，而不会产生库冲突（Dependency Hell）。
// Flink 的插件系统（通常位于 Flink 安装目录的 plugins/ 文件夹下）具有以下几个关键特性，而 PluginManager 正是这些特性的入口：
// 隔离性加载：每个插件都由独立的类加载器（ClassLoader）加载。这意味着如果插件需要一个特定版本的库（例如旧版的 Guava），而 Flink 核心使用的是新版，两者可以共存而不会发生冲突。
// 基于 SPI 的发现机制：它利用了 Java 的 SPI (Service Provider Interface) 模式。你只需要定义一个接口（如 FileSystemFactory），PluginManager 就能自动找到所有插件包中对该接口的实现。
// 动态扩展性：通过插件机制，Flink 可以支持多种文件系统（S3, OSS, Azure）、各种安全认证方式或监控指标收集器，而无需将这些庞大的依赖全部打包进 Flink 核心包。
public interface PluginManager {

    /**
     * Returns in iterator over all available implementations of the given service interface (SPI)
     * in all the plugins known to this plugin manager instance.
     *
     * @param service the service interface (SPI) for which implementations are requested.
     * @param <P> Type of the requested plugin service.
     * @return Iterator over all implementations of the given service that could be loaded from all
     *     known plugins.
     */
    // service: 这是你想要加载的服务的 接口类型（Class 对象）。
    // 例如，如果你想加载所有的文件系统插件，你会传入 FileSystemFactory.class。
    // <P>: 代表请求的插件服务类型。这保证了返回的迭代器中的对象类型与请求的接口类型一致。
    <P> Iterator<P> load(Class<P> service);
}
