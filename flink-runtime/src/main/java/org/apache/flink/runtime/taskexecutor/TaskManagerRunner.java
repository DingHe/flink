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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.TaskExecutorBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DeterminismEnvelope;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.TraceReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemUtils;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.token.DelegationTokenReceiverRepository;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Reference;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode. It
 * constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
// TaskManagerRunner 是一个至关重要的类。它不仅是 TaskManager 进程的可执行入口（即 main 方法所在处），还承担了“组件装配工”的角色。
// 主要职责是：
// 启动入口：负责解析命令行参数并加载 Flink 配置。
// 生命周期管理：控制 TaskManager 从初始化、启动到关闭的完整过程，并注册 JVM 钩子（Shutdown Hook）以确保优雅退出。
// 服务编排：它负责初始化 TaskManager 运行所需的几乎所有底层服务，包括：
// 通信层：RPC 服务（基于 Pekko/Akka）。
// 协调层：高可用服务（HA）、心跳服务。
// 存储层：Blob 缓存服务（用于分发 Jar 包）、工作目录管理。
// 监控层：指标注册表（Metrics）、JMX 服务。
// 核心组件创建：最终实例化并启动 TaskExecutor，这是真正负责执行 Task 的核心对象。
public class TaskManagerRunner implements FatalErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

    private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

    private static final int SUCCESS_EXIT_CODE = 0;
    @VisibleForTesting public static final int FAILURE_EXIT_CODE = 1;
    // JVM 关闭钩子。当接收到操作系统信号（如 SIGTERM）时，执行异步清理工作。
    private final Thread shutdownHook;
    // 内部同步锁，确保多线程下（如启动与关闭并发）服务的状态一致性。
    private final Object lock = new Object();
    // 存储从 flink-conf.yaml 解析出的所有配置参数。
    private final Configuration configuration;

    private final Time timeout;

    private final PluginManager pluginManager;
    // 任务执行器服务工厂，用于创建 TaskExecutorService 实例。
    private final TaskExecutorServiceFactory taskExecutorServiceFactory;
    // 运行状态承诺。
    // 当 TaskManager 彻底退出时，该 Future 会完成并返回退出码。
    private final CompletableFuture<Result> terminationFuture;

    @GuardedBy("lock") //只是标注访问这几个字段要枷锁
    private DeterminismEnvelope<ResourceID> resourceId;

    /** Executor used to run future callbacks. */
    @GuardedBy("lock")
    private ExecutorService executor;

    @GuardedBy("lock")
    private RpcSystem rpcSystem;
    // 基础 RPC 通信服务，处理节点间的消息分发。
    @GuardedBy("lock")
    private RpcService rpcService;
    // 连接 Zookeeper 或 Kubernetes，用于获取 Leader（ResourceManager）地址。
    @GuardedBy("lock")
    private HighAvailabilityServices highAvailabilityServices;
    // 指标注册表，负责收集并上报 TaskManager 的 CPU、内存及自定义指标。
    @GuardedBy("lock")
    private MetricRegistryImpl metricRegistry;
    // 用于从 JobManager 下载作业所需的 Jar 包和库。
    @GuardedBy("lock")
    private BlobCacheService blobCacheService;

    @GuardedBy("lock")
    private DeterminismEnvelope<WorkingDirectory> workingDirectory;
    // 对 TaskExecutor 的包装，是执行算子逻辑的核心服务。
    @GuardedBy("lock")
    private TaskExecutorService taskExecutorService;
    // 标记位，记录当前服务是否已经启动了关闭流程。
    @GuardedBy("lock")
    private boolean shutdown;

    public TaskManagerRunner(
            Configuration configuration,
            PluginManager pluginManager,
            TaskExecutorServiceFactory taskExecutorServiceFactory)
            throws Exception {
        this.configuration = checkNotNull(configuration);
        this.pluginManager = checkNotNull(pluginManager);
        this.taskExecutorServiceFactory = checkNotNull(taskExecutorServiceFactory);

        timeout = Time.fromDuration(configuration.get(RpcOptions.ASK_TIMEOUT_DURATION));

        this.terminationFuture = new CompletableFuture<>();
        this.shutdown = false;

        this.shutdownHook =
                ShutdownHookUtil.addShutdownHook(
                        () -> this.closeAsync(Result.JVM_SHUTDOWN).join(),
                        getClass().getSimpleName(),
                        LOG);
    }

    private void startTaskManagerRunnerServices() throws Exception {
        synchronized (lock) {
            rpcSystem = RpcSystem.load(configuration);

            this.executor =
                    Executors.newScheduledThreadPool(
                            Hardware.getNumberCPUCores(),
                            new ExecutorThreadFactory("taskmanager-future"));

            highAvailabilityServices =
                    HighAvailabilityServicesUtils.createHighAvailabilityServices(
                            configuration,
                            executor,
                            AddressResolution.NO_ADDRESS_RESOLUTION,
                            rpcSystem,
                            this);
            //JMX 是 Java 提供的标准化监控和管理框架，允许开发者通过 MBeans（管理 Bean）来暴露应用程序内部状态
            JMXService.startInstance(configuration.get(JMXServerOptions.JMX_SERVER_PORT));

            rpcService = createRpcService(configuration, highAvailabilityServices, rpcSystem);

            this.resourceId =
                    getTaskManagerResourceID(
                            configuration, rpcService.getAddress(), rpcService.getPort());

            this.workingDirectory =
                    ClusterEntrypointUtils.createTaskManagerWorkingDirectory(
                            configuration, resourceId);

            LOG.info("Using working directory: {}", workingDirectory);

            HeartbeatServices heartbeatServices =
                    HeartbeatServices.fromConfiguration(configuration);
            //监控指标注册
            metricRegistry =
                    new MetricRegistryImpl(
                            MetricRegistryConfiguration.fromConfiguration(
                                    configuration,
                                    rpcSystem.getMaximumMessageSizeInBytes(configuration)),
                            ReporterSetup.fromConfiguration(configuration, pluginManager),
                            TraceReporterSetup.fromConfiguration(configuration, pluginManager));
            //监控指标查询
            final RpcService metricQueryServiceRpcService =
                    MetricUtils.startRemoteMetricsRpcService(
                            configuration,
                            rpcService.getAddress(),
                            configuration.get(TaskManagerOptions.BIND_HOST),
                            rpcSystem);
            metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId.unwrap());
            //块缓存服务
            blobCacheService =
                    BlobUtils.createBlobCacheService(
                            configuration,
                            Reference.borrowed(workingDirectory.unwrap().getBlobStorageDirectory()),
                            highAvailabilityServices.createBlobStore(),
                            null);
            //外部资源
            final ExternalResourceInfoProvider externalResourceInfoProvider =
                    ExternalResourceUtils.createStaticExternalResourceInfoProviderFromConfig(
                            configuration, pluginManager);

            final DelegationTokenReceiverRepository delegationTokenReceiverRepository =
                    new DelegationTokenReceiverRepository(configuration, pluginManager);

            taskExecutorService =
                    taskExecutorServiceFactory.createTaskExecutor(
                            this.configuration,
                            this.resourceId.unwrap(),
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            metricRegistry,
                            blobCacheService,
                            false,
                            externalResourceInfoProvider,
                            workingDirectory.unwrap(),
                            this,
                            delegationTokenReceiverRepository);

            handleUnexpectedTaskExecutorServiceTermination();

            MemoryLogger.startIfConfigured(
                    LOG, configuration, terminationFuture.thenAccept(ignored -> {}));
        }
    }

    @GuardedBy("lock")
    private void handleUnexpectedTaskExecutorServiceTermination() {
        taskExecutorService
                .getTerminationFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (lock) {
                                if (!shutdown) {
                                    onFatalError(
                                            new FlinkException(
                                                    "Unexpected termination of the TaskExecutor.",
                                                    throwable));
                                }
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    //  Lifecycle management
    // --------------------------------------------------------------------------------------------

    public void start() throws Exception {
        synchronized (lock) {
            startTaskManagerRunnerServices();
            taskExecutorService.start();
        }
    }

    public void close() throws Exception {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            ExceptionUtils.rethrowException(ExceptionUtils.stripExecutionException(e));
        }
    }

    public CompletableFuture<Result> closeAsync() {
        return closeAsync(Result.SUCCESS);
    }

    private CompletableFuture<Result> closeAsync(Result terminationResult) {
        synchronized (lock) {
            // remove shutdown hook to prevent resource leaks
            ShutdownHookUtil.removeShutdownHook(shutdownHook, this.getClass().getSimpleName(), LOG);

            if (shutdown) {
                return terminationFuture;
            }

            final CompletableFuture<Void> taskManagerTerminationFuture;
            if (taskExecutorService != null) {
                taskManagerTerminationFuture = taskExecutorService.closeAsync();
            } else {
                taskManagerTerminationFuture = FutureUtils.completedVoidFuture();
            }

            final CompletableFuture<Void> serviceTerminationFuture =
                    FutureUtils.composeAfterwards(
                            taskManagerTerminationFuture, this::shutDownServices);

            final CompletableFuture<Void> workingDirCleanupFuture =
                    FutureUtils.runAfterwards(
                            serviceTerminationFuture, () -> deleteWorkingDir(terminationResult));

            final CompletableFuture<Void> rpcSystemClassLoaderCloseFuture;

            if (rpcSystem != null) {
                rpcSystemClassLoaderCloseFuture =
                        FutureUtils.runAfterwards(workingDirCleanupFuture, rpcSystem::close);
            } else {
                rpcSystemClassLoaderCloseFuture = FutureUtils.completedVoidFuture();
            }

            rpcSystemClassLoaderCloseFuture.whenComplete(
                    (Void ignored, Throwable throwable) -> {
                        if (throwable != null) {
                            terminationFuture.completeExceptionally(throwable);
                        } else {
                            terminationFuture.complete(terminationResult);
                        }
                    });

            shutdown = true;
            return terminationFuture;
        }
    }

    private void deleteWorkingDir(Result terminationResult) throws IOException {
        synchronized (lock) {
            if (workingDirectory != null) {
                if (!workingDirectory.isDeterministic() || terminationResult == Result.SUCCESS) {
                    workingDirectory.unwrap().delete();
                }
            }
        }
    }

    private CompletableFuture<Void> shutDownServices() {
        synchronized (lock) {
            Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
            Exception exception = null;

            try {
                JMXService.stopInstance();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            if (blobCacheService != null) {
                try {
                    blobCacheService.close();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (metricRegistry != null) {
                try {
                    terminationFutures.add(metricRegistry.closeAsync());
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (highAvailabilityServices != null) {
                try {
                    highAvailabilityServices.close();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (rpcService != null) {
                terminationFutures.add(rpcService.closeAsync());
            }

            if (executor != null) {
                terminationFutures.add(
                        ExecutorUtils.nonBlockingShutdown(
                                timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    // export the termination future for caller to know it is terminated
    public CompletableFuture<Result> getTerminationFuture() {
        return terminationFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  FatalErrorHandler methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void onFatalError(Throwable exception) {
        TaskManagerExceptionUtils.tryEnrichTaskManagerError(exception);
        LOG.error(
                "Fatal error occurred while executing the TaskManager. Shutting it down...",
                exception);

        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
            terminateJVM();
        } else {
            closeAsync(Result.FAILURE);

            FutureUtils.orTimeout(
                    terminationFuture,
                    FATAL_ERROR_SHUTDOWN_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS,
                    String.format(
                            "Waiting for TaskManager shutting down timed out after %s ms.",
                            FATAL_ERROR_SHUTDOWN_TIMEOUT_MS));
        }
    }

    private void terminateJVM() {
        FlinkSecurityManager.forceProcessExit(FAILURE_EXIT_CODE);
    }

    // --------------------------------------------------------------------------------------------
    //  Static entry point
    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // startup checks and logging
        // 在日志中输出当前系统的上下文信息
        EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
        // 注册对操作系统信号（如 SIGTERM, SIGINT）的监听
        // 当管理员执行 kill <pid> 或在终端按 Ctrl+C 时，Flink 通过这个处理器捕获信号，并记录一条优雅的日志。这能防止进程被突发终止时，开发者不知道发生了什么。
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
        // 查询当前操作系统对进程允许打开的最大文件句柄数（File Descriptors）
        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if (maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        runTaskManagerProcessSecurely(args);
    }

    public static Configuration loadConfiguration(String[] args) throws FlinkParseException {
        return ConfigurationParserUtils.loadCommonConfiguration(
                args, TaskManagerRunner.class.getSimpleName());
    }
    // 初始化并启动 TaskManager 进程，并阻塞等待其运行结束，最后返回退出状态码。
    public static int runTaskManager(Configuration configuration, PluginManager pluginManager)
            throws Exception {
        final TaskManagerRunner taskManagerRunner;

        try {
            taskManagerRunner =
                    new TaskManagerRunner(
                            configuration,
                            pluginManager, // 传入之前讨论过的插件管理器，用于加载文件系统、安全模块等插件。
                            TaskManagerRunner::createTaskExecutorService); // 指定了如何创建底层的 TaskExecutor。TaskExecutor 才是真正执行任务的“劳动力”。
            taskManagerRunner.start(); // 正式启动内部各组件
        } catch (Exception exception) {
            throw new FlinkException("Failed to start the TaskManagerRunner.", exception);
        }

        try {
            return taskManagerRunner.getTerminationFuture().get().getExitCode();
        } catch (Throwable t) {
            throw new FlinkException(
                    "Unexpected failure during runtime of TaskManagerRunner.",
                    ExceptionUtils.stripExecutionException(t));
        }
    }
    // 从原始命令行参数向结构化配置对象过渡的关键步骤。
    //
    // 它确保了在没有任何配置的情况下，进程能够安全地报错退出，而不是带着错误的状态启动。
    public static void runTaskManagerProcessSecurely(String[] args) {
        // 这个变量将承载从 flink-conf.yaml 文件以及命令行 -D 参数中解析出来的所有配置信息
        Configuration configuration = null;

        try {
            // 它会定位 Flink 的配置目录（通常是 CONF_DIR）
            // 读取并解析 YAML 配置文件
            // 将命令行传入的动态参数（如 --configDir）覆盖到基础配置中
            configuration = loadConfiguration(args);
        } catch (FlinkParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(FAILURE_EXIT_CODE);
        }
        // 进入真正的“安全上下文”启动逻辑。
        runTaskManagerProcessSecurely(checkNotNull(configuration));
    }
    // 核心任务是在正式启动计算服务之前，建立安全防线并加载所有的底层插件。
    public static void runTaskManagerProcessSecurely(Configuration configuration) {
        // 根据配置初始化 Flink 自带的 Java SecurityManager
        FlinkSecurityManager.setFromConfiguration(configuration);
        // 加载 Flink 的插件系统。
        // Flink 采用插件化设计（放在 plugins/ 目录下），如各种文件系统（S3, OSS, Azure）、指标报告器（Prometheus）等。此行代码负责扫描目录并加载这些隔离的插件类。
        final PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        // 全局初始化 Flink 支持的所有文件系统（HDFS, S3, Local 等）
        // 它会结合刚才加载的插件，使得 Flink 后续可以通过 hdfs:// 或 s3:// 这种 Schema 读写数据。
        FileSystem.initialize(configuration, pluginManager);
        // 加载用于“状态增量日志（Changelog）”的存储插件
        StateChangelogStorageLoader.initialize(pluginManager);

        int exitCode;
        Throwable throwable = null;
        // 设置 JVM 范围内的 UncaughtExceptionHandler
        ClusterEntrypointUtils.configureUncaughtExceptionHandler(configuration);
        try {
            // 安装安全模块（如 Kerberos 认证、JAAS 配置）
            // 这是与 Hadoop 环境交互的基础。它会根据配置确定是否需要从 Keytab 登录，并启动自动刷新 TGT 的后台任务。
            SecurityUtils.install(new SecurityConfiguration(configuration));
            // 最核心的步骤。
            // 在安全上下文中启动真正的 TaskManager
            exitCode =
                    SecurityUtils.getInstalledContext()
                            .runSecured(() -> runTaskManager(configuration, pluginManager));
        } catch (Throwable t) {
            throwable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            exitCode = FAILURE_EXIT_CODE;
        }

        if (throwable != null) {
            LOG.error("Terminating TaskManagerRunner with exit code {}.", exitCode, throwable);
        } else {
            LOG.info("Terminating TaskManagerRunner with exit code {}.", exitCode);
        }

        System.exit(exitCode);
    }

    // --------------------------------------------------------------------------------------------
    //  Static utilities
    // --------------------------------------------------------------------------------------------
    // Flink TaskManager 的启动链路中起到了承上启下的作用。
    // 它将底层的、复杂的 TaskExecutor（任务执行器核心）封装成一个更易于生命周期管理的 TaskExecutorService 服务。
    public static TaskExecutorService createTaskExecutorService(
            Configuration configuration,
            ResourceID resourceID,
            RpcService rpcService, // 处理所有远程过程调用，是 TaskManager 内部及与 JobManager 通信的基础。
            HighAvailabilityServices highAvailabilityServices, // 提供元数据存储（如 Zookeeper），用于集群恢复和地址发现。
            HeartbeatServices heartbeatServices,
            MetricRegistry metricRegistry,
            BlobCacheService blobCacheService, // 负责从 JobManager 下载大文件（如 JAR 包）
            boolean localCommunicationOnly,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            WorkingDirectory workingDirectory,
            FatalErrorHandler fatalErrorHandler, // 致命错误处理器，当发生 OOM 或无法恢复的故障时触发关机。
            DelegationTokenReceiverRepository delegationTokenReceiverRepository) // 全相关的组件，用于处理 Hadoop 代理令牌。
            throws Exception {

        // startTaskManager 来构造并返回一个 TaskExecutor 实例
        final TaskExecutor taskExecutor =
                startTaskManager(
                        configuration,
                        resourceID,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        metricRegistry,
                        blobCacheService,
                        localCommunicationOnly,
                        externalResourceInfoProvider,
                        workingDirectory,
                        fatalErrorHandler,
                        delegationTokenReceiverRepository);

        return TaskExecutorToServiceAdapter.createFor(taskExecutor);
    }

    // startTaskManager 是 Flink TaskManager 启动过程中最关键的底层方法。它负责组装资源、初始化核心服务（内存、网络、IO）并最终创建执行引擎对象
    public static TaskExecutor startTaskManager(
            Configuration configuration,
            ResourceID resourceID, // TaskManager的唯一资源标识符
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            MetricRegistry metricRegistry,
            TaskExecutorBlobService taskExecutorBlobService,
            boolean localCommunicationOnly,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            WorkingDirectory workingDirectory,
            FatalErrorHandler fatalErrorHandler,
            DelegationTokenReceiverRepository delegationTokenReceiverRepository)
            throws Exception {
        // 确保核心依赖（配置、资源ID、RPC服务、HA服务）不为空
        checkNotNull(configuration);
        checkNotNull(resourceID);
        checkNotNull(rpcService);
        checkNotNull(highAvailabilityServices);
        // 在日志中记录 TaskManager 的唯一资源标识符（ResourceID），这对于在 Yarn/K8s 容器中定位问题至关重要。
        LOG.info("Starting TaskManager with ResourceID: {}", resourceID.getStringWithMetadata());
        // 根据配置重定向系统的 stdout 和 stderr。通常将打印到控制台的内容转义到日志文件中，防止日志分散
        SystemOutRedirectionUtils.redirectSystemOutAndError(configuration);
        // 获取当前 TaskManager 的外部 RPC 通信地址
        String externalAddress = rpcService.getAddress();
        // 从配置中解析内存模型（堆内、堆外、托管内存、网络内存等）和 CPU 核心数。
        // 它定义了当前 TaskManager 的物理资源边界
        final TaskExecutorResourceSpec taskExecutorResourceSpec =
                TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);
        // 将原始的 Configuration 转换为更具体的“服务层配置”，包含网络堆栈、IO设置等。
        TaskManagerServicesConfiguration taskManagerServicesConfiguration =
                TaskManagerServicesConfiguration.fromConfiguration(
                        configuration,
                        resourceID,
                        externalAddress,
                        localCommunicationOnly,
                        taskExecutorResourceSpec,
                        workingDirectory);
        // 初始化度量（Metrics）系统，为当前 TaskManager 创建一个度量组。这决定了你之后在 Flink UI 上看到的 CPU 使用率、内存占用等指标的路径。
        Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup =
                MetricUtils.instantiateTaskManagerMetricGroup(
                        metricRegistry,
                        externalAddress,
                        resourceID,
                        taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());
        // 创建专门用于磁盘读写和文件操作的线程池
        final ExecutorService ioExecutor =
                Executors.newFixedThreadPool(
                        taskManagerServicesConfiguration.getNumIoThreads(),
                        new ExecutorThreadFactory("flink-taskexecutor-io"));
        // 这是最重的方法。
        // 它在内部实例化了 TaskManager 的三大基石：
        // MemoryManager: 内存管理器（预分配托管内存）
        // ShuffleEnvironment: 网络通信与数据交换环境
        // TaskSlotTable: 槽位表，记录当前节点可以跑多少个并行的 Subtask
        TaskManagerServices taskManagerServices =
                TaskManagerServices.fromConfiguration(
                        taskManagerServicesConfiguration,
                        taskExecutorBlobService.getPermanentBlobService(),
                        taskManagerMetricGroup.f1,
                        ioExecutor,
                        rpcService.getScheduledExecutor(),
                        fatalErrorHandler,
                        workingDirectory);
        // 注册内存相关的监控指标，特别是针对托管内存（Managed Memory）的使用情况进行实时打点。
        MetricUtils.instantiateFlinkMemoryMetricGroup(
                taskManagerMetricGroup.f1,
                taskManagerServices.getTaskSlotTable(),
                taskManagerServices::getManagedMemorySize);
        // 封装 TaskManager 运行时的控制参数（如心跳超时、任务杀掉等待时间等）。
        TaskManagerConfiguration taskManagerConfiguration =
                TaskManagerConfiguration.fromConfiguration(
                        configuration,
                        taskExecutorResourceSpec,
                        externalAddress,
                        workingDirectory.getTmpDirectory());

        String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();
        // 终点。将所有初始化好的 RPC 服务、配置、存储、内存、监控组件全部注入到 TaskExecutor 实例中并返回
        return new TaskExecutor(
                rpcService,
                taskManagerConfiguration,
                highAvailabilityServices,
                taskManagerServices,
                externalResourceInfoProvider,
                heartbeatServices,
                taskManagerMetricGroup.f0,
                metricQueryServiceAddress,
                taskExecutorBlobService,
                fatalErrorHandler,
                new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()), // 用于追踪当前节点上的中间数据集（Data Partition）状态，确保在 Failover 时数据能被正确清理或复用。
                delegationTokenReceiverRepository);
    }

    /**
     * Create a RPC service for the task manager.
     *
     * @param configuration The configuration for the TaskManager.
     * @param haServices to use for the task manager hostname retrieval
     */
    @VisibleForTesting
    static RpcService createRpcService(
            final Configuration configuration,
            final HighAvailabilityServices haServices,
            final RpcSystem rpcSystem)
            throws Exception {

        checkNotNull(configuration);
        checkNotNull(haServices);
        //创建actor system，并加入监控actor和dead leter actor，然后创建PekkoRpcService服务
        return RpcUtils.createRemoteRpcService(
                rpcSystem,
                configuration,
                determineTaskManagerBindAddress(configuration, haServices, rpcSystem),
                configuration.get(TaskManagerOptions.RPC_PORT),
                configuration.get(TaskManagerOptions.BIND_HOST),
                configuration.getOptional(TaskManagerOptions.RPC_BIND_PORT));
    }
    //确定TaskManager绑定的地址
    private static String determineTaskManagerBindAddress(
            final Configuration configuration,
            final HighAvailabilityServices haServices,
            RpcSystemUtils rpcSystemUtils)
            throws Exception {

        final String configuredTaskManagerHostname = configuration.get(TaskManagerOptions.HOST);

        if (configuredTaskManagerHostname != null) {
            LOG.info(
                    "Using configured hostname/address for TaskManager: {}.",
                    configuredTaskManagerHostname);
            return configuredTaskManagerHostname;
        } else {
            return determineTaskManagerBindAddressByConnectingToResourceManager(
                    configuration, haServices, rpcSystemUtils);
        }
    }

    private static String determineTaskManagerBindAddressByConnectingToResourceManager(
            final Configuration configuration,
            final HighAvailabilityServices haServices,
            RpcSystemUtils rpcSystemUtils)
            throws LeaderRetrievalException {

        final Duration lookupTimeout = configuration.get(RpcOptions.LOOKUP_TIMEOUT_DURATION);

        final InetAddress taskManagerAddress =
                LeaderRetrievalUtils.findConnectingAddress(
                        haServices.getResourceManagerLeaderRetriever(),
                        lookupTimeout,
                        rpcSystemUtils);

        LOG.info(
                "TaskManager will use hostname/address '{}' ({}) for communication.",
                taskManagerAddress.getHostName(),
                taskManagerAddress.getHostAddress());

        HostBindPolicy bindPolicy =
                HostBindPolicy.fromString(configuration.get(TaskManagerOptions.HOST_BIND_POLICY));
        return bindPolicy == HostBindPolicy.IP
                ? taskManagerAddress.getHostAddress()
                : taskManagerAddress.getHostName();
    }

    @VisibleForTesting
    static DeterminismEnvelope<ResourceID> getTaskManagerResourceID(
            Configuration config, String rpcAddress, int rpcPort) {

        final String metadata =
                config.get(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, "");
        return config.getOptional(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID)
                .map(
                        value ->
                                DeterminismEnvelope.deterministicValue(
                                        new ResourceID(value, metadata)))
                .orElseGet(
                        FunctionUtils.uncheckedSupplier(
                                () -> {
                                    final String hostName =
                                            InetAddress.getLocalHost().getHostName();
                                    final String value =
                                            StringUtils.isNullOrWhitespaceOnly(rpcAddress)
                                                    ? hostName
                                                            + "-"
                                                            + new AbstractID()
                                                                    .toString()
                                                                    .substring(0, 6)
                                                    : rpcAddress
                                                            + ":"
                                                            + rpcPort
                                                            + "-"
                                                            + new AbstractID()
                                                                    .toString()
                                                                    .substring(0, 6);
                                    return DeterminismEnvelope.nondeterministicValue(
                                            new ResourceID(value, metadata));
                                }));
    }

    /** Factory for {@link TaskExecutor}. */
    public interface TaskExecutorServiceFactory {
        TaskExecutorService createTaskExecutor(
                Configuration configuration,
                ResourceID resourceID,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                MetricRegistry metricRegistry,
                BlobCacheService blobCacheService,
                boolean localCommunicationOnly,
                ExternalResourceInfoProvider externalResourceInfoProvider,
                WorkingDirectory workingDirectory,
                FatalErrorHandler fatalErrorHandler,
                DelegationTokenReceiverRepository delegationTokenReceiverRepository)
                throws Exception;
    }

    public interface TaskExecutorService extends AutoCloseableAsync {
        void start();

        CompletableFuture<Void> getTerminationFuture();
    }

    public enum Result {
        SUCCESS(SUCCESS_EXIT_CODE),
        JVM_SHUTDOWN(FAILURE_EXIT_CODE),
        FAILURE(FAILURE_EXIT_CODE);

        private final int exitCode;

        Result(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
