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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.io.File;

/** Interface to access {@link TaskExecutor} information. */
// flink 中用于访问 TaskExecutor（即 TaskManager 的新名称）运行时信息的接口。
// 核心作用是：
// 配置访问层： 为 TaskExecutor 内部的各个组件（如 Task、网络栈、文件系统等）提供一个统一的、只读的入口，以获取 TaskExecutor 启动时加载的配置信息和环境参数。
// 环境上下文： 封装了 TaskExecutor 运行所依赖的环境上下文，包括地址信息、临时文件目录和关键的运行时行为配置（如 OOM 时的行为）。
public interface TaskManagerRuntimeInfo {

    /**
     * Gets the configuration that the TaskManager was started with.
     *
     * @return The configuration that the TaskManager was started with.
     */
    // 返回 TaskManager（TaskExecutor）启动时所使用的完整配置对象。所有 TaskManager 级别的配置参数（如内存大小、RPC 端口、心跳间隔等）都可以在此配置中找到。
    Configuration getConfiguration();

    /**
     * Gets the list of temporary file directories.
     *
     * @return The list of temporary file directories.
     */
    // 返回 TaskManager 用于存储临时文件（如 Spill 文件、批处理的中间结果等）的目录路径列表。这些路径通常由配置 io.tmpdirs 指定。
    String[] getTmpDirectories();

    /**
     * Checks whether the TaskManager should exit the JVM when the task thread throws an
     * OutOfMemoryError.
     *
     * @return True to terminate the JVM on an OutOfMemoryError, false otherwise.
     */
    boolean shouldExitJvmOnOutOfMemoryError();

    /**
     * Gets the external address of the TaskManager.
     *
     * @return The external address of the TaskManager.
     */
    // TaskManager 报告给 JobManager 或其他组件的地址。
    // 它是外部组件用来连接到该 TaskManager 的地址（例如，在 NAT 或容器环境中，它可能是公共 IP）
    String getTaskManagerExternalAddress();

    /**
     * Gets the bind address of the Taskmanager.
     *
     * @return The bind address of the TaskManager.
     */
    // 通过查询配置 (getConfiguration()) 来获取 TaskManagerOptions.BIND_HOST 配置项的值。
    // 这是 TaskManager 进程实际绑定用于监听连接的本地网络接口地址。
    default String getTaskManagerBindAddress() {
        return getConfiguration().get(TaskManagerOptions.BIND_HOST);
    }

    /**
     * Gets the temporary working directory of the TaskManager instance.
     *
     * @return The temporary working directory of the TaskManager.
     */
    // 返回当前 TaskManager 实例的临时工作目录的 File 对象。这个目录通常用于存放 TaskManager 运行时产生的一些特定文件。
    File getTmpWorkingDirectory();
}
