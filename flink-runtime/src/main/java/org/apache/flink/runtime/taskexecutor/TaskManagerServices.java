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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;
import org.apache.flink.runtime.state.TaskExecutorChannelStateExecutorFactoryManager;
import org.apache.flink.runtime.state.TaskExecutorFileMergingManager;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.taskexecutor.slot.DefaultTimerService;
import org.apache.flink.runtime.taskexecutor.slot.FileSlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.NoOpSlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.SlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.DefaultGroupCache;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link ShuffleEnvironment}. All services are exclusive to a single {@link TaskExecutor}.
 * Consequently, the respective {@link TaskExecutor} is responsible for closing them.
 */
// Flink TaskManager（任务执行器）中的核心服务容器
// 核心作用是生命周期管理与依赖注入。它负责创建、持有、并最终销毁 TaskManager 运行所需的几乎所有底层服务组件（如内存管理、网络传输、槽位管理等）
// 通过将这些零散的服务聚合在一起，它为 TaskExecutor 提供了一个统一的操作界面。你可以把它想象成 TaskManager 的“后勤保障部”。
public class TaskManagerServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServices.class);

    /** TaskManager services. */
    // 存储未解析的 TM 位置信息（ResourceID、IP、端口），用于向集群汇报身份。
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;
    // 当前 TM 能够使用的托管内存（Managed Memory）的总大小（字节）。
    private final long managedMemorySize;
    // 负责管理同步/异步磁盘 IO。
    // 当数据超过内存限制时，负责将数据溢写（Spill）到磁盘。
    private final IOManager ioManager;
    // 网络层核心。
    // 负责管理节点间的数据交换（Shuffle），处理数据的分发、接收和网络缓冲池。
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;
    // 负责管理可查询状态（Queryable State），允许外部应用查询正在运行的任务状态。
    private final KvStateService kvStateService;
    // 管理任务中的广播变量，确保同一节点上的多个任务共享同一份广播数据副本。
    private final BroadcastVariableManager broadcastVariableManager;
    // 槽位管理器。
    // 记录当前 TM 上所有 Slot 的分配情况、状态及其与 Task 的绑定关系。
    private final TaskSlotTable<Task> taskSlotTable;
    // 记录当前 TM 上承载的所有 Job 的元数据信息。
    private final JobTable jobTable;
    // 领导者监听服务。
    // 负责监听各 JobManager 的 Leader 变化，确保 TM 能连接到正确的 JM。
    private final JobLeaderService jobLeaderService;
    // 管理本地状态存储。
    // 通过在本地保留状态副本，加速 Checkpoint 恢复（Local Recovery）。
    private final TaskExecutorLocalStateStoresManager taskManagerStateStore;
    // 实验性功能，负责合并小文件，优化大规模状态下的文件系统压力。
    private final TaskExecutorFileMergingManager taskManagerFileMergingManager;
    // 管理状态更新的流水（Changelog）存储，支持异步状态上报。
    private final TaskExecutorStateChangelogStoragesManager taskManagerChangelogManager;
    private final TaskExecutorChannelStateExecutorFactoryManager taskManagerChannelStateManager; //管理通道状态（Channel State），支持流处理的高效状态恢复
    // 事件总线。负责在任务之间分发控制事件（如迭代计算中的屏障、取消信号等）。
    private final TaskEventDispatcher taskEventDispatcher;
    // 专用的线程池，处理后台 IO 密集型操作，防止阻塞主 RPC 线程。
    private final ExecutorService ioExecutor;
    // 类加载器管理。
    // 负责从 BlobService 下载作业 JAR 包并创建对应的 UserCodeClassLoader。
    private final LibraryCacheManager libraryCacheManager;
    // 用于将 Slot 分配状态持久化到磁盘，以便 TM 异常重启后能尝试恢复之前的分配。
    private final SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService;
    // 管理跨任务共享的资源（如 RocksDB 的共享内存、缓存等）。
    private final SharedResources sharedResources;
    // 元数据缓存。缓存 JobInformation、TaskInformation 等静态信息，避免重复的网络传输。
    private final GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache;
    private final GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache;
    private final GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup>
            shuffleDescriptorsCache; //缓存任务之间 Shuffle 操作的描述信息

    TaskManagerServices(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
            long managedMemorySize,
            IOManager ioManager,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            KvStateService kvStateService,
            BroadcastVariableManager broadcastVariableManager,
            TaskSlotTable<Task> taskSlotTable,
            JobTable jobTable,
            JobLeaderService jobLeaderService,
            TaskExecutorLocalStateStoresManager taskManagerStateStore,
            TaskExecutorFileMergingManager taskManagerFileMergingManager,
            TaskExecutorStateChangelogStoragesManager taskManagerChangelogManager,
            TaskExecutorChannelStateExecutorFactoryManager taskManagerChannelStateManager,
            TaskEventDispatcher taskEventDispatcher,
            ExecutorService ioExecutor,
            LibraryCacheManager libraryCacheManager,
            SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService,
            SharedResources sharedResources,
            GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache,
            GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache) {

        this.unresolvedTaskManagerLocation =
                Preconditions.checkNotNull(unresolvedTaskManagerLocation);
        this.managedMemorySize = managedMemorySize;
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.shuffleEnvironment = Preconditions.checkNotNull(shuffleEnvironment);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
        this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
        this.jobTable = Preconditions.checkNotNull(jobTable);
        this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
        this.taskManagerStateStore = Preconditions.checkNotNull(taskManagerStateStore);
        this.taskManagerFileMergingManager =
                Preconditions.checkNotNull(taskManagerFileMergingManager);
        this.taskManagerChangelogManager = Preconditions.checkNotNull(taskManagerChangelogManager);
        this.taskManagerChannelStateManager = taskManagerChannelStateManager;
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.ioExecutor = Preconditions.checkNotNull(ioExecutor);
        this.libraryCacheManager = Preconditions.checkNotNull(libraryCacheManager);
        this.slotAllocationSnapshotPersistenceService = slotAllocationSnapshotPersistenceService;
        this.sharedResources = Preconditions.checkNotNull(sharedResources);
        this.jobInformationCache = jobInformationCache;
        this.taskInformationCache = taskInformationCache;
        this.shuffleDescriptorsCache = Preconditions.checkNotNull(shuffleDescriptorsCache);
    }

    // --------------------------------------------------------------------------------------------
    //  Getter/Setter
    // --------------------------------------------------------------------------------------------

    public long getManagedMemorySize() {
        return managedMemorySize;
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    public ShuffleEnvironment<?, ?> getShuffleEnvironment() {
        return shuffleEnvironment;
    }

    public KvStateService getKvStateService() {
        return kvStateService;
    }

    public UnresolvedTaskManagerLocation getUnresolvedTaskManagerLocation() {
        return unresolvedTaskManagerLocation;
    }

    public BroadcastVariableManager getBroadcastVariableManager() {
        return broadcastVariableManager;
    }

    public TaskSlotTable<Task> getTaskSlotTable() {
        return taskSlotTable;
    }

    public JobTable getJobTable() {
        return jobTable;
    }

    public JobLeaderService getJobLeaderService() {
        return jobLeaderService;
    }

    public TaskExecutorLocalStateStoresManager getTaskManagerStateStore() {
        return taskManagerStateStore;
    }

    public TaskExecutorFileMergingManager getTaskManagerFileMergingManager() {
        return taskManagerFileMergingManager;
    }

    public TaskExecutorStateChangelogStoragesManager getTaskManagerChangelogManager() {
        return taskManagerChangelogManager;
    }

    public TaskExecutorChannelStateExecutorFactoryManager getTaskManagerChannelStateManager() {
        return taskManagerChannelStateManager;
    }

    public TaskEventDispatcher getTaskEventDispatcher() {
        return taskEventDispatcher;
    }

    public Executor getIOExecutor() {
        return ioExecutor;
    }

    public LibraryCacheManager getLibraryCacheManager() {
        return libraryCacheManager;
    }

    public SharedResources getSharedResources() {
        return sharedResources;
    }

    public GroupCache<JobID, PermanentBlobKey, JobInformation> getJobInformationCache() {
        return jobInformationCache;
    }

    public GroupCache<JobID, PermanentBlobKey, TaskInformation> getTaskInformationCache() {
        return taskInformationCache;
    }

    public GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> getShuffleDescriptorCache() {
        return shuffleDescriptorsCache;
    }

    // --------------------------------------------------------------------------------------------
    //  Shut down method
    // --------------------------------------------------------------------------------------------

    /** Shuts the {@link TaskExecutor} services down. */
    public void shutDown() throws FlinkException {

        Exception exception = null;

        try {
            taskManagerStateStore.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        try {
            ioManager.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            shuffleEnvironment.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            kvStateService.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskSlotTable.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            ioExecutor.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobTable.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            libraryCacheManager.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        taskEventDispatcher.clearAll();

        if (exception != null) {
            throw new FlinkException(
                    "Could not properly shut down the TaskManager services.", exception);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Static factory methods for task manager services
    // --------------------------------------------------------------------------------------------

    /**
     * Creates and returns the task manager services.
     *
     * @param taskManagerServicesConfiguration task manager configuration
     * @param permanentBlobService permanentBlobService used by the services
     * @param taskManagerMetricGroup metric group of the task manager
     * @param ioExecutor executor for async IO operations
     * @param scheduledExecutor scheduled executor in rpc service
     * @param fatalErrorHandler to handle class loading OOMs
     * @param workingDirectory the working directory of the process
     * @return task manager components
     * @throws Exception
     */
    public static TaskManagerServices fromConfiguration(
            TaskManagerServicesConfiguration taskManagerServicesConfiguration,
            PermanentBlobService permanentBlobService,
            MetricGroup taskManagerMetricGroup,
            ExecutorService ioExecutor,
            ScheduledExecutor scheduledExecutor,
            FatalErrorHandler fatalErrorHandler,
            WorkingDirectory workingDirectory)
            throws Exception {

        // pre-start checks
        checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());

        final TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

        // start the I/O manager, it will create some temp directories.
        final IOManager ioManager =
                new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths(), ioExecutor);

        final ShuffleEnvironment<?, ?> shuffleEnvironment =
                createShuffleEnvironment(
                        taskManagerServicesConfiguration,
                        taskEventDispatcher,
                        taskManagerMetricGroup,
                        ioExecutor,
                        scheduledExecutor);
        final int listeningDataPort = shuffleEnvironment.start();

        LOG.info(
                "TaskManager data connection initialized successfully; listening internally on port: {}",
                listeningDataPort);

        final KvStateService kvStateService =
                KvStateService.fromConfiguration(taskManagerServicesConfiguration);
        kvStateService.start();

        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new UnresolvedTaskManagerLocation(
                        taskManagerServicesConfiguration.getResourceID(),
                        taskManagerServicesConfiguration.getExternalAddress(),
                        // we expose the task manager location with the listening port
                        // iff the external data port is not explicitly defined
                        taskManagerServicesConfiguration.getExternalDataPort() > 0
                                ? taskManagerServicesConfiguration.getExternalDataPort()
                                : listeningDataPort,
                        taskManagerServicesConfiguration.getNodeId());

        final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();

        final TaskSlotTable<Task> taskSlotTable =
                createTaskSlotTable(
                        taskManagerServicesConfiguration.getNumberOfSlots(),
                        taskManagerServicesConfiguration.getTaskExecutorResourceSpec(),
                        taskManagerServicesConfiguration.getTimerServiceShutdownTimeout(),
                        taskManagerServicesConfiguration.getPageSize(),
                        ioExecutor);

        final JobTable jobTable = DefaultJobTable.create();

        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        unresolvedTaskManagerLocation,
                        taskManagerServicesConfiguration.getRetryingRegistrationConfiguration());

        final TaskExecutorLocalStateStoresManager taskStateManager =
                new TaskExecutorLocalStateStoresManager(
                        taskManagerServicesConfiguration.isLocalRecoveryEnabled(),
                        taskManagerServicesConfiguration.isLocalBackupEnabled(),
                        taskManagerServicesConfiguration.getLocalRecoveryStateDirectories(),
                        ioExecutor);

        final TaskExecutorStateChangelogStoragesManager changelogStoragesManager =
                new TaskExecutorStateChangelogStoragesManager();

        final TaskExecutorChannelStateExecutorFactoryManager channelStateExecutorFactoryManager =
                new TaskExecutorChannelStateExecutorFactoryManager();

        final TaskExecutorFileMergingManager fileMergingManager =
                new TaskExecutorFileMergingManager();

        final boolean failOnJvmMetaspaceOomError =
                taskManagerServicesConfiguration
                        .getConfiguration()
                        .get(CoreOptions.FAIL_ON_USER_CLASS_LOADING_METASPACE_OOM);
        final boolean checkClassLoaderLeak =
                taskManagerServicesConfiguration
                        .getConfiguration()
                        .get(CoreOptions.CHECK_LEAKED_CLASSLOADER);
        final LibraryCacheManager libraryCacheManager =
                new BlobLibraryCacheManager(
                        permanentBlobService,
                        BlobLibraryCacheManager.defaultClassLoaderFactory(
                                taskManagerServicesConfiguration.getClassLoaderResolveOrder(),
                                taskManagerServicesConfiguration
                                        .getAlwaysParentFirstLoaderPatterns(),
                                failOnJvmMetaspaceOomError ? fatalErrorHandler : null,
                                checkClassLoaderLeak),
                        false);

        final SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService;

        if (taskManagerServicesConfiguration.isLocalRecoveryEnabled()) {
            slotAllocationSnapshotPersistenceService =
                    new FileSlotAllocationSnapshotPersistenceService(
                            workingDirectory.getSlotAllocationSnapshotDirectory());
        } else {
            slotAllocationSnapshotPersistenceService =
                    NoOpSlotAllocationSnapshotPersistenceService.INSTANCE;
        }

        final GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache =
                new DefaultGroupCache.Factory<JobID, PermanentBlobKey, JobInformation>().create();
        final GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache =
                new DefaultGroupCache.Factory<JobID, PermanentBlobKey, TaskInformation>().create();

        final GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache =
                new DefaultGroupCache.Factory<JobID, PermanentBlobKey, ShuffleDescriptorGroup>()
                        .create();

        return new TaskManagerServices(
                unresolvedTaskManagerLocation,
                taskManagerServicesConfiguration.getManagedMemorySize().getBytes(),
                ioManager,
                shuffleEnvironment,
                kvStateService,
                broadcastVariableManager,
                taskSlotTable,
                jobTable,
                jobLeaderService,
                taskStateManager,
                fileMergingManager,
                changelogStoragesManager,
                channelStateExecutorFactoryManager,
                taskEventDispatcher,
                ioExecutor,
                libraryCacheManager,
                slotAllocationSnapshotPersistenceService,
                new SharedResources(),
                jobInformationCache,
                taskInformationCache,
                shuffleDescriptorsCache);
    }

    private static TaskSlotTable<Task> createTaskSlotTable(
            final int numberOfSlots,
            final TaskExecutorResourceSpec taskExecutorResourceSpec,
            final long timerServiceShutdownTimeout,
            final int pageSize,
            final Executor memoryVerificationExecutor) {
        final TimerService<AllocationID> timerService =
                new DefaultTimerService<>(
                        new ScheduledThreadPoolExecutor(1), timerServiceShutdownTimeout);
        return new TaskSlotTableImpl<>(
                numberOfSlots,
                TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(
                        taskExecutorResourceSpec),
                TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(
                        taskExecutorResourceSpec, numberOfSlots),
                pageSize,
                timerService,
                memoryVerificationExecutor);
    }

    private static ShuffleEnvironment<?, ?> createShuffleEnvironment(
            TaskManagerServicesConfiguration taskManagerServicesConfiguration,
            TaskEventDispatcher taskEventDispatcher,
            MetricGroup taskManagerMetricGroup,
            Executor ioExecutor,
            ScheduledExecutor scheduledExecutor)
            throws FlinkException {

        final ShuffleEnvironmentContext shuffleEnvironmentContext =
                new ShuffleEnvironmentContext(
                        taskManagerServicesConfiguration.getConfiguration(),
                        taskManagerServicesConfiguration.getResourceID(),
                        taskManagerServicesConfiguration.getNetworkMemorySize(),
                        taskManagerServicesConfiguration.isLocalCommunicationOnly(),
                        taskManagerServicesConfiguration.getBindAddress(),
                        taskManagerServicesConfiguration.getNumberOfSlots(),
                        taskManagerServicesConfiguration.getTmpDirPaths(),
                        taskEventDispatcher,
                        taskManagerMetricGroup,
                        ioExecutor,
                        scheduledExecutor);

        return ShuffleServiceLoader.loadShuffleServiceFactory(
                        taskManagerServicesConfiguration.getConfiguration())
                .createShuffleEnvironment(shuffleEnvironmentContext);
    }

    /**
     * Validates that all the directories denoted by the strings do actually exist or can be
     * created, are proper directories (not files), and are writable.
     *
     * @param tmpDirs The array of directory paths to check.
     * @throws IOException Thrown if any of the directories does not exist and cannot be created or
     *     is not writable or is a file, rather than a directory.
     */
    private static void checkTempDirs(String[] tmpDirs) throws IOException {
        for (String dir : tmpDirs) {
            if (dir != null && !dir.equals("")) {
                File file = new File(dir);
                if (!file.exists()) {
                    if (!file.mkdirs()) {
                        throw new IOException(
                                "Temporary file directory "
                                        + file.getAbsolutePath()
                                        + " does not exist and could not be created.");
                    }
                }
                if (!file.isDirectory()) {
                    throw new IOException(
                            "Temporary file directory "
                                    + file.getAbsolutePath()
                                    + " is not a directory.");
                }
                if (!file.canWrite()) {
                    throw new IOException(
                            "Temporary file directory "
                                    + file.getAbsolutePath()
                                    + " is not writable.");
                }

                if (LOG.isInfoEnabled()) {
                    long totalSpaceGb = file.getTotalSpace() >> 30;
                    long usableSpaceGb = file.getUsableSpace() >> 30;
                    double usablePercentage = (double) usableSpaceGb / totalSpaceGb * 100;
                    String path = file.getAbsolutePath();
                    LOG.info(
                            String.format(
                                    "Temporary file directory '%s': total %d GB, "
                                            + "usable %d GB (%.2f%% usable)",
                                    path, totalSpaceGb, usableSpaceGb, usablePercentage));
                }
            } else {
                throw new IllegalArgumentException("Temporary file directory #$id is null.");
            }
        }
    }

    public SlotAllocationSnapshotPersistenceService getSlotAllocationSnapshotPersistenceService() {
        return slotAllocationSnapshotPersistenceService;
    }
}
