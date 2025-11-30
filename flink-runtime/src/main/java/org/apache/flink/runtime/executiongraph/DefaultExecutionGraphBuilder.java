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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerUtils;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link DefaultExecutionGraph} from a {@link
 * JobGraph}.
 */
public class DefaultExecutionGraphBuilder {
    // 这个方法是 Flink JobMaster 将逻辑作业图 (JobGraph) 转换为运行时执行图 (ExecutionGraph) 的核心入口。
    public static DefaultExecutionGraph buildGraph(
            JobGraph jobGraph,
            Configuration jobManagerConfig,  //与作业管理器相关的配置，如最大重试次数、检查点设置等
            ScheduledExecutorService futureExecutor,  //用于调度异步任务的执行器
            Executor ioExecutor,
            ClassLoader classLoader,
            CompletedCheckpointStore completedCheckpointStore, //已完成检查点的存储，用于作业恢复
            CheckpointsCleaner checkpointsCleaner,  //检查点清理器，用于清理过时的检查点
            CheckpointIDCounter checkpointIdCounter,
            Time rpcTimeout,
            BlobWriter blobWriter,  //用于写入二进制大对象（Blob）的工具类
            Logger log,
            ShuffleMaster<?> shuffleMaster,  //负责处理数据洗牌操作的组件
            JobMasterPartitionTracker partitionTracker, // 用于跟踪作业各个分区的状态
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,  //于存储每个任务的执行尝试次数
            VertexParallelismStore vertexParallelismStore, //存储每个任务的并行度信息
            CheckpointStatsTracker checkpointStatsTracker,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            boolean nonFinishedHybridPartitionShouldBeUnknown,  //指示未完成的混合分区是否应该被标记为“未知”
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobExecutionException, JobException {

        checkNotNull(jobGraph, "job graph cannot be null");
        // 获取作业名称。
        final String jobName = jobGraph.getName();
        // 获取作业 ID。
        final JobID jobId = jobGraph.getJobID();
        // 获取作业的类型（如批处理 BATCH 或流处理 STREAMING）。
        final JobType jobType = jobGraph.getJobType();
        // 包含了作业的基本信息，包括作业 ID、类型、名称、执行配置、作业配置、JAR 包路径等
        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobType,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths());
        // 获取历史记录限制。
        // 从 JobManager 配置中获取任务执行历史记录（失败尝试）的最大存储数量。
        final int executionHistorySizeLimit =
                jobManagerConfig.get(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);
        // 加载分区释放策略工厂。
        // 加载并创建用于决定何时释放（清理）中间结果分区的策略工厂。
        final PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory =
                PartitionGroupReleaseStrategyFactoryLoader.loadPartitionGroupReleaseStrategyFactory(
                        jobManagerConfig);
        // 获取 Shuffle 描述符卸载阈值。
        // 获取配置值，用于确定何时将任务部署描述符中的 Shuffle 描述符（可能包含大量数据）卸载到 BLOB 存储而不是直接嵌入到部署消息中
        final int offloadShuffleDescriptorsThreshold =
                jobManagerConfig.get(
                        TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD);
        //任务部署描述符工厂
        final TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory;
        try {
            // 实例化任务部署描述符工厂
            // 使用 BlobWriter 来序列化 JobInformation 并可能卸载 Shuffle 描述符，以准备 TaskManager 部署所需的数据。
            taskDeploymentDescriptorFactory =
                    new TaskDeploymentDescriptorFactory(
                            BlobWriter.serializeAndTryOffload(jobInformation, jobId, blobWriter),
                            jobId,
                            partitionLocationConstraint,
                            blobWriter,
                            nonFinishedHybridPartitionShouldBeUnknown,
                            offloadShuffleDescriptorsThreshold);
        } catch (IOException e) {
            throw new JobException("Could not create the TaskDeploymentDescriptorFactory.", e);
        }
        // 基于配置加载并创建所有外部或用户定义的监听器，
        // 这些监听器将在作业状态发生变化时被通知。
        final List<JobStatusChangedListener> jobStatusChangedListeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        classLoader, jobManagerConfig, ioExecutor);

        // create a new execution graph, if none exists so far
        // 使用前面准备的所有参数和服务组件（执行器、跟踪器、监听器、ShuffleMaster 等）初始化 DefaultExecutionGraph。
        // 这是 ExecutionGraph 对象的创建点。
        final DefaultExecutionGraph executionGraph =
                new DefaultExecutionGraph(
                        jobGraph.getJobType(),
                        jobInformation,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        executionHistorySizeLimit,
                        classLoader,
                        blobWriter,
                        partitionGroupReleaseStrategyFactory,
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentListener,
                        executionStateUpdateListener,
                        initializationTimestamp,
                        vertexAttemptNumberStore,
                        vertexParallelismStore,
                        isDynamicGraph,
                        executionJobVertexFactory,
                        jobGraph.getJobStatusHooks(),
                        markPartitionFinishedStrategy,
                        taskDeploymentDescriptorFactory,
                        jobStatusChangedListeners);

        // set the basic properties

        try {
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);
        // 遍历JobGraph的顶点
        for (JobVertex vertex : jobGraph.getVertices()) {
            // task的执行类
            String executableClass = vertex.getInvokableClassName();
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            try {  //initializeOnMaster实际什么也没做
                vertex.initializeOnMaster(
                        new SimpleInitializeOnMasterContext(
                                classLoader,
                                vertexParallelismStore
                                        .getParallelismInfo(vertex.getID())
                                        .getParallelism()));
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }

        log.info(
                "Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        // 根据任务依赖关系对 JobGraph 中的所有 JobVertex 进行拓扑排序。
        // topologically sort the job vertices and attach the graph to the existing one
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId);
        }

        // 连接 JobGraph 到 ExecutionGraph。
        // 建 ExecutionGraph 核心结构的关键一步。
        // 它遍历拓扑排序后的 JobVertex 列表，为每个 JobVertex 创建相应的 ExecutionJobVertex，
        // 并进一步创建 ExecutionVertex 和 Execution 实例，建立任务间的边（Intermediate Result Partitions）。
        executionGraph.attachJobGraph(sortedTopology, jobManagerJobMetricGroup);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        // configure the state checkpointing
        // 检查作业是否是动态图（不支持 Checkpointing），或者是否启用了 Checkpointing。
        if (isDynamicGraph) {
            // dynamic graph does not support checkpointing so we skip it
            log.warn("Skip setting up checkpointing for a job with dynamic graph.");
        } else if (isCheckpointingEnabled(jobGraph)) {
            // 获取 Checkpointing 配置。
            JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();

            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            // 尝试从应用配置中反序列化用户定义的 StateBackend。如果没有配置，则为 null。
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    // 尝试从应用配置中反序列化用户定义的 StateBackend。
                    // 如果没有配置，则为 null。
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                // 根据应用配置、Job 配置和 JobManager 配置的默认值，确定并加载最终要使用的 StateBackend 实例。
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // load the checkpoint storage from the application settings
            final CheckpointStorage applicationConfiguredStorage;
            final SerializedValue<CheckpointStorage> serializedAppConfiguredStorage =
                    snapshotSettings.getDefaultCheckpointStorage();

            if (serializedAppConfiguredStorage == null) {
                applicationConfiguredStorage = null;
            } else {
                try {
                    applicationConfiguredStorage =
                            serializedAppConfiguredStorage.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId,
                            "Could not deserialize application-defined checkpoint storage.",
                            e);
                }
            }

            final CheckpointStorage rootStorage;
            try {
                rootStorage =
                        CheckpointStorageLoader.load(
                                applicationConfiguredStorage,
                                rootBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured checkpoint storage", e);
            }

            // instantiate the user-defined checkpoint hooks

            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }
            // 获取 Checkpoint 协调器配置。
            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();
            // 启用 Checkpointing。
            executionGraph.enableCheckpointing(
                    chkConfig,
                    hooks,
                    checkpointIdCounter,
                    completedCheckpointStore,
                    rootBackend,
                    rootStorage,
                    checkpointStatsTracker,
                    checkpointsCleaner,
                    jobManagerConfig.get(STATE_CHANGE_LOG_STORAGE));
        }

        return executionGraph;
    }

    public static boolean isCheckpointingEnabled(JobGraph jobGraph) {
        return jobGraph.getCheckpointingSettings() != null;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private DefaultExecutionGraphBuilder() {}
}
