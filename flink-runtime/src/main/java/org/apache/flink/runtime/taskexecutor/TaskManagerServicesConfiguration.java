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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Reference;

import javax.annotation.Nullable;

import java.io.File;
import java.net.InetAddress;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for the task manager services such as the memory manager, the io manager and the
 * metric registry.
 */
// TaskManagerServicesConfiguration 是一个承上启下的参数聚合类。它将散落在 Configuration 中的原始配置项，转化为各个 TaskManager 服务（如网络堆栈、内存管理器、IO 管理器等）能够直接使用的强类型参数
// 它在 TaskManagerServices 真正创建之前被构造，负责：
// 解析：将字符串形式的配置解析为具体的 Java 对象（如 InetAddress、MemorySize）
// 校验：确保关键参数（如超时时间、路径）符合业务逻辑要求。
// 分发：作为容器，将解析好的参数一次性传递给 TaskManagerServices 用于各个子组件的初始化。
public class TaskManagerServicesConfiguration {

    private static final String LOCAL_STATE_SUB_DIRECTORY_ROOT = "localState_";
    // 原始的 Flink 配置对象，供后续灵活查询。
    private final Configuration configuration;
    // 当前 TaskManager 在集群中的唯一身份标识
    private final ResourceID resourceID;
    // TaskManager 对外公开的 IP/主机名（用于其他节点访问）
    private final String externalAddress;
    // 节点 ID，通常用于指标上报和唯一性确定，默认通常等于 externalAddress
    private final String nodeId;
    // 本地监听的网卡地址
    private final InetAddress bindAddress;
    // Netty 数据交换服务的端口
    private final int externalDataPort;
    // 是否仅允许本地通信（通常用于单机测试模式）
    private final boolean localCommunicationOnly;
    // TaskManager 使用的临时目录列表，用于存放溢写数据等
    private final String[] tmpDirPaths;
    // 用于状态本地恢复（Local Recovery）的存储目录
    private final Reference<File[]> localRecoveryStateDirectories;
    // 当前 TaskManager 提供的任务槽（Task Slot）总数
    private final int numberOfSlots;
    // 可查询状态（Queryable State）服务的配置，如果不开启则为空。
    @Nullable private final QueryableStateConfiguration queryableStateConfig;
    // 内存管理器的页大小（默认 32KB），决定了网络缓冲区和托管内存的切割粒度
    private final int pageSize;
    // 定时器服务关闭时的超时时间
    private final long timerServiceShutdownTimeout;
    // 是否开启本地状态恢复
    private final boolean localRecoveryEnabled;
    // 是否开启本地备份（Checkpoint 辅助）
    private final boolean localBackupEnabled;
    // TaskManager 向 JobManager 注册时的重试策略配置。
    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;
    // 系统资源指标（CPU、内存等）的采样频率
    private Optional<Time> systemResourceMetricsProbingInterval;
    // 之前讨论过的资源规格（CPU、任务内存、托管内存等）
    private final TaskExecutorResourceSpec taskExecutorResourceSpec;
    // 用户代码类加载顺序（Parent-first 或 Child-first）
    private final FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder;
    // 哪些包路径必须强制使用 Parent-first 加载模式
    private final String[] alwaysParentFirstLoaderPatterns;
    // 用于 IO 操作（读写磁盘）的线程池大小。
    private final int numIoThreads;

    private TaskManagerServicesConfiguration(
            Configuration configuration,
            ResourceID resourceID,
            String externalAddress,
            InetAddress bindAddress,
            int externalDataPort,
            boolean localCommunicationOnly,
            String[] tmpDirPaths,
            Reference<File[]> localRecoveryStateDirectories,
            boolean localRecoveryEnabled,
            boolean localBackupEnabled,
            @Nullable QueryableStateConfiguration queryableStateConfig,
            int numberOfSlots,
            int pageSize,
            TaskExecutorResourceSpec taskExecutorResourceSpec,
            long timerServiceShutdownTimeout,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration,
            Optional<Time> systemResourceMetricsProbingInterval,
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
            String[] alwaysParentFirstLoaderPatterns,
            int numIoThreads,
            String nodeId) {
        this.configuration = checkNotNull(configuration);
        this.resourceID = checkNotNull(resourceID);

        this.externalAddress = checkNotNull(externalAddress);
        this.bindAddress = checkNotNull(bindAddress);
        this.externalDataPort = externalDataPort;
        this.localCommunicationOnly = localCommunicationOnly;
        this.tmpDirPaths = checkNotNull(tmpDirPaths);
        this.localRecoveryStateDirectories = checkNotNull(localRecoveryStateDirectories);
        this.localRecoveryEnabled = localRecoveryEnabled;
        this.localBackupEnabled = localBackupEnabled;
        this.queryableStateConfig = queryableStateConfig;
        this.numberOfSlots = numberOfSlots;

        this.pageSize = pageSize;

        this.taskExecutorResourceSpec = taskExecutorResourceSpec;
        this.classLoaderResolveOrder = classLoaderResolveOrder;
        this.alwaysParentFirstLoaderPatterns = alwaysParentFirstLoaderPatterns;
        this.numIoThreads = numIoThreads;

        checkArgument(
                timerServiceShutdownTimeout >= 0L,
                "The timer " + "service shutdown timeout must be greater or equal to 0.");
        this.timerServiceShutdownTimeout = timerServiceShutdownTimeout;
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.systemResourceMetricsProbingInterval =
                checkNotNull(systemResourceMetricsProbingInterval);

        this.nodeId = checkNotNull(nodeId);
    }

    // --------------------------------------------------------------------------------------------
    //  Getter/Setter
    // --------------------------------------------------------------------------------------------

    public Configuration getConfiguration() {
        return configuration;
    }

    public ResourceID getResourceID() {
        return resourceID;
    }

    String getExternalAddress() {
        return externalAddress;
    }

    InetAddress getBindAddress() {
        return bindAddress;
    }

    int getExternalDataPort() {
        return externalDataPort;
    }

    boolean isLocalCommunicationOnly() {
        return localCommunicationOnly;
    }

    public String[] getTmpDirPaths() {
        return tmpDirPaths;
    }

    Reference<File[]> getLocalRecoveryStateDirectories() {
        return localRecoveryStateDirectories;
    }

    boolean isLocalRecoveryEnabled() {
        return localRecoveryEnabled;
    }

    boolean isLocalBackupEnabled() {
        return localBackupEnabled;
    }

    @Nullable
    QueryableStateConfiguration getQueryableStateConfig() {
        return queryableStateConfig;
    }

    public int getNumberOfSlots() {
        return numberOfSlots;
    }

    public int getPageSize() {
        return pageSize;
    }

    public TaskExecutorResourceSpec getTaskExecutorResourceSpec() {
        return taskExecutorResourceSpec;
    }

    public MemorySize getNetworkMemorySize() {
        return taskExecutorResourceSpec.getNetworkMemSize();
    }

    public MemorySize getManagedMemorySize() {
        return taskExecutorResourceSpec.getManagedMemorySize();
    }

    long getTimerServiceShutdownTimeout() {
        return timerServiceShutdownTimeout;
    }

    public Optional<Time> getSystemResourceMetricsProbingInterval() {
        return systemResourceMetricsProbingInterval;
    }

    RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
        return retryingRegistrationConfiguration;
    }

    public FlinkUserCodeClassLoaders.ResolveOrder getClassLoaderResolveOrder() {
        return classLoaderResolveOrder;
    }

    public String[] getAlwaysParentFirstLoaderPatterns() {
        return alwaysParentFirstLoaderPatterns;
    }

    public int getNumIoThreads() {
        return numIoThreads;
    }

    public String getNodeId() {
        return nodeId;
    }

    // --------------------------------------------------------------------------------------------
    //  Parsing of Flink configuration
    // --------------------------------------------------------------------------------------------

    /**
     * Utility method to extract TaskManager config parameters from the configuration and to sanity
     * check them.
     *
     * @param configuration The configuration.
     * @param resourceID resource ID of the task manager
     * @param externalAddress identifying the IP address under which the TaskManager will be
     *     accessible
     * @param localCommunicationOnly True if only local communication is possible. Use only in cases
     *     where only one task manager runs.
     * @param taskExecutorResourceSpec resource specification of the TaskManager to start
     * @param workingDirectory working directory of the TaskManager
     * @return configuration of task manager services used to create them
     */
    // 采用私有构造函数，强制通过 fromConfiguration 静态工厂方法进行创建。
    public static TaskManagerServicesConfiguration fromConfiguration(
            Configuration configuration,
            ResourceID resourceID,
            String externalAddress,
            boolean localCommunicationOnly,
            TaskExecutorResourceSpec taskExecutorResourceSpec,
            WorkingDirectory workingDirectory)
            throws Exception {
        String[] localStateRootDirs = ConfigurationUtils.parseLocalStateDirectories(configuration);
        final Reference<File[]> localStateDirs;

        if (localStateRootDirs.length == 0) {
            localStateDirs =
                    Reference.borrowed(new File[] {workingDirectory.getLocalStateDirectory()});
        } else {
            File[] createdLocalStateDirs = new File[localStateRootDirs.length];
            final String localStateDirectoryName = LOCAL_STATE_SUB_DIRECTORY_ROOT + resourceID;

            for (int i = 0; i < localStateRootDirs.length; i++) {
                createdLocalStateDirs[i] = new File(localStateRootDirs[i], localStateDirectoryName);
            }

            localStateDirs = Reference.owned(createdLocalStateDirs);
        }

        boolean localRecoveryEnabled = configuration.get(StateRecoveryOptions.LOCAL_RECOVERY);
        boolean localBackupEnabled = configuration.get(CheckpointingOptions.LOCAL_BACKUP_ENABLED);

        final QueryableStateConfiguration queryableStateConfig =
                QueryableStateConfiguration.fromConfiguration(configuration);

        long timerServiceShutdownTimeout =
                configuration.get(RpcOptions.ASK_TIMEOUT_DURATION).toMillis();

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);

        final int externalDataPort = configuration.get(NettyShuffleEnvironmentOptions.DATA_PORT);

        String bindAddr =
                configuration.get(TaskManagerOptions.BIND_HOST, NetUtils.getWildcardIPAddress());
        InetAddress bindAddress = InetAddress.getByName(bindAddr);

        final String classLoaderResolveOrder =
                configuration.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER);

        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(configuration);

        final int numIoThreads = ClusterEntrypointUtils.getPoolSize(configuration);

        final String[] tmpDirs = ConfigurationUtils.parseTempDirectories(configuration);

        // If TaskManagerOptionsInternal.TASK_MANAGER_NODE_ID is not set, use the external address
        // as the node id.
        final String nodeId =
                configuration
                        .getOptional(TaskManagerOptionsInternal.TASK_MANAGER_NODE_ID)
                        .orElse(externalAddress);

        return new TaskManagerServicesConfiguration(
                configuration,
                resourceID,
                externalAddress,
                bindAddress,
                externalDataPort,
                localCommunicationOnly,
                tmpDirs,
                localStateDirs,
                localRecoveryEnabled,
                localBackupEnabled,
                queryableStateConfig,
                ConfigurationParserUtils.getSlot(configuration),
                ConfigurationParserUtils.getPageSize(configuration),
                taskExecutorResourceSpec,
                timerServiceShutdownTimeout,
                retryingRegistrationConfiguration,
                ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration),
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder),
                alwaysParentFirstLoaderPatterns,
                numIoThreads,
                nodeId);
    }
}
