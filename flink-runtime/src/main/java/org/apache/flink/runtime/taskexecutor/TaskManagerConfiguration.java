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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;

/** Configuration object for {@link TaskExecutor}. */
// 专门用于封装 Flink TaskExecutor (即 TaskManager) 启动和运行时所需的所有核心配置和环境参数
// 核心作用总结：
// 集中配置： 它将 Flink 配置 (Configuration 对象) 中与 TaskExecutor 行为相关的关键参数（如插槽数量、超时时间、资源配置等）提取出来，作为独立的、类型安全（Type-safe）的字段存储。
// 运行时信息： 它实现了 TaskManagerRuntimeInfo 接口，意味着它不仅是 TaskExecutor 的配置，也是其运行时环境信息的来源，供 TaskExecutor 内部的各个组件在运行时快速查询。
public class TaskManagerConfiguration implements TaskManagerRuntimeInfo {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerConfiguration.class);
    // TaskSlot 数量。 该 TaskExecutor 将提供的并发任务插槽数量。
    private final int numberSlots;
    // 单个 TaskSlot 默认拥有的资源配置（CPU、内存等）
    private final ResourceProfile defaultSlotResourceProfile;
    // TaskExecutor 进程可用的全部资源总量。
    private final ResourceProfile totalResourceProfile;
    // Flink 用于存储临时文件（如 Spill 文件）的本地文件系统目录列表。
    private final String[] tmpDirectories;
    // TaskExecutor 与 JobManager 或其他组件进行 RPC 通信时的默认请求超时时间
    private final Duration rpcTimeout;
    // TaskSlot 在一段时间内未被使用或被释放后，保持空闲状态的超时时间。
    private final Duration slotTimeout;

    // null indicates an infinite duration
    // TaskExecutor 尝试向 JobManager 注册的最大持续时间。null 表示无限期尝试注册。
    @Nullable private final Duration maxRegistrationDuration;
    // TaskExecutor 启动时使用的原始 Flink 配置，但被包装成不可修改版本，防止运行时被意外修改。
    private final UnmodifiableConfiguration configuration;
    // 标志位，指示任务线程发生 OOM 时是否应该终止整个 JVM 进程。
    private final boolean exitJvmOnOutOfMemory;
    // 完整的 TaskManager 日志文件路径。
    @Nullable private final String taskManagerLogPath;
    // TaskManager 标准输出文件（stdout）的路径
    @Nullable private final String taskManagerStdoutPath;
    // TaskManager 的日志文件所在的目录
    @Nullable private final String taskManagerLogDir;
    // TaskExecutor 报告给 JobManager 的外部可访问的网络地址。
    private final String taskManagerExternalAddress;
    // TaskExecutor 实例的临时工作目录的 File 对象
    private final File tmpWorkingDirectory;
    // TaskExecutor 在注册失败时重试连接 JobManager 的相关配置，包括重试次数、延迟等。
    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    public TaskManagerConfiguration(
            int numberSlots,
            ResourceProfile defaultSlotResourceProfile,
            ResourceProfile totalResourceProfile,
            String[] tmpDirectories,
            Duration rpcTimeout,
            Duration slotTimeout,
            @Nullable Duration maxRegistrationDuration,
            Configuration configuration,
            boolean exitJvmOnOutOfMemory,
            @Nullable String taskManagerLogPath,
            @Nullable String taskManagerStdoutPath,
            @Nullable String taskManagerLogDir,
            String taskManagerExternalAddress,
            File tmpWorkingDirectory,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.numberSlots = numberSlots;
        this.defaultSlotResourceProfile = defaultSlotResourceProfile;
        this.totalResourceProfile = totalResourceProfile;
        this.tmpDirectories = Preconditions.checkNotNull(tmpDirectories);
        this.rpcTimeout = Preconditions.checkNotNull(rpcTimeout);
        this.slotTimeout = Preconditions.checkNotNull(slotTimeout);
        this.maxRegistrationDuration = maxRegistrationDuration;
        this.configuration =
                new UnmodifiableConfiguration(Preconditions.checkNotNull(configuration));
        this.exitJvmOnOutOfMemory = exitJvmOnOutOfMemory;
        this.taskManagerLogPath = taskManagerLogPath;
        this.taskManagerStdoutPath = taskManagerStdoutPath;
        this.taskManagerLogDir = taskManagerLogDir;
        this.taskManagerExternalAddress = taskManagerExternalAddress;
        this.tmpWorkingDirectory = tmpWorkingDirectory;
        this.retryingRegistrationConfiguration = retryingRegistrationConfiguration;
    }

    public int getNumberSlots() {
        return numberSlots;
    }

    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    public ResourceProfile getTotalResourceProfile() {
        return totalResourceProfile;
    }

    public Duration getRpcTimeout() {
        return rpcTimeout;
    }

    public Duration getSlotTimeout() {
        return slotTimeout;
    }

    @Nullable
    public Duration getMaxRegistrationDuration() {
        return maxRegistrationDuration;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public String[] getTmpDirectories() {
        return tmpDirectories;
    }

    @Override
    public boolean shouldExitJvmOnOutOfMemoryError() {
        return exitJvmOnOutOfMemory;
    }

    @Nullable
    public String getTaskManagerLogPath() {
        return taskManagerLogPath;
    }

    @Nullable
    public String getTaskManagerStdoutPath() {
        return taskManagerStdoutPath;
    }

    @Nullable
    public String getTaskManagerLogDir() {
        return taskManagerLogDir;
    }

    @Override
    public String getTaskManagerExternalAddress() {
        return taskManagerExternalAddress;
    }

    @Override
    public File getTmpWorkingDirectory() {
        return tmpWorkingDirectory;
    }

    public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
        return retryingRegistrationConfiguration;
    }

    // --------------------------------------------------------------------------------------------
    //  Static factory methods
    // --------------------------------------------------------------------------------------------

    public static TaskManagerConfiguration fromConfiguration(
            Configuration configuration,
            TaskExecutorResourceSpec taskExecutorResourceSpec,
            String externalAddress,
            File tmpWorkingDirectory) {
        int numberSlots = configuration.get(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        if (numberSlots == -1) {
            numberSlots = 1;
        }

        final String[] tmpDirPaths = ConfigurationUtils.parseTempDirectories(configuration);

        final Duration rpcTimeout = configuration.get(RpcOptions.ASK_TIMEOUT_DURATION);

        LOG.debug("Messages have a max timeout of " + rpcTimeout);

        final Duration slotTimeout = configuration.get(TaskManagerOptions.SLOT_TIMEOUT);

        Duration finiteRegistrationDuration;
        try {
            finiteRegistrationDuration = configuration.get(TaskManagerOptions.REGISTRATION_TIMEOUT);
        } catch (IllegalArgumentException e) {
            LOG.warn(
                    "Invalid format for parameter {}. Set the timeout to be infinite.",
                    TaskManagerOptions.REGISTRATION_TIMEOUT.key());
            finiteRegistrationDuration = null;
        }

        final boolean exitOnOom = configuration.get(TaskManagerOptions.KILL_ON_OUT_OF_MEMORY);

        final String taskManagerLogPath =
                configuration.get(TaskManagerOptions.TASK_MANAGER_LOG_PATH);
        final String taskManagerStdoutPath;
        final String taskManagerLogDir;

        if (taskManagerLogPath != null) {
            final int extension = taskManagerLogPath.lastIndexOf('.');
            taskManagerLogDir = new File(taskManagerLogPath).getParent();

            if (extension > 0) {
                taskManagerStdoutPath = taskManagerLogPath.substring(0, extension) + ".out";
            } else {
                taskManagerStdoutPath = null;
            }
        } else {
            taskManagerStdoutPath = null;
            taskManagerLogDir = null;
        }

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);

        return new TaskManagerConfiguration(
                numberSlots,
                TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(
                        taskExecutorResourceSpec, numberSlots),
                TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(
                        taskExecutorResourceSpec),
                tmpDirPaths,
                rpcTimeout,
                slotTimeout,
                finiteRegistrationDuration,
                configuration,
                exitOnOom,
                taskManagerLogPath,
                taskManagerStdoutPath,
                taskManagerLogDir,
                externalAddress,
                tmpWorkingDirectory,
                retryingRegistrationConfiguration);
    }
}
