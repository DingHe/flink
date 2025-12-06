/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.VertexAttemptNumberStore;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default {@link ExecutionGraphFactory} implementation. */
// DefaultExecutionGraphFactory 是 ExecutionGraphFactory 接口的默认实现类。
// 构造 ExecutionGraph： 接收 JobGraph（逻辑作业图）和一系列 JobMaster 运行时服务（如执行器、BLOB 存储、Shuffle 服务等）。
// 恢复逻辑： 在 ExecutionGraph 创建完成后，协调 CheckpointCoordinator 检查并执行必要的状态恢复（从 Checkpoint 或 Savepoint），使作业能够从中断点继续执行
// 它是一个大型的装配器，负责将 JobMaster 的所有核心组件连接到新创建的 ExecutionGraph 实例中。

public class DefaultExecutionGraphFactory implements ExecutionGraphFactory {
    // 作业配置。 整个 Flink 集群和当前作业的配置参数。
    private final Configuration configuration;
    // 用户代码类加载器。
    // 用于加载用户提交的 Jar 包中的类和资源。
    private final ClassLoader userCodeClassLoader;
    // 执行部署跟踪器。
    // 负责跟踪哪些任务实例（Execution Attempts）当前正在 TaskManager 上部署或运行。
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    // 定时执行器。
    // 用于调度 Flink 内部的定时任务和异步操作，例如 Checkpoint 超时。
    private final ScheduledExecutorService futureExecutor;
    // I/O 执行器。
    // 用于执行可能涉及 I/O 或耗时较长的操作（如 Checkpoint 状态存储、文件操作），避免阻塞主调度线程。
    private final Executor ioExecutor;
    // RPC 超时时间。
    // 用于 JobMaster 与 TaskManager 之间远程调用（RPC）的默认超时配置。
    private final Time rpcTimeout;
    // 度量指标组。
    // JobManager 侧用于收集和报告该作业相关性能指标的容器。
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;
    // BLOB 写入器。
    // 用于向 BLINK 存储分发大型数据（如用户 Jar 包），在任务部署时使用。
    private final BlobWriter blobWriter;
    // 数据混洗管理器。
    // 负责管理作业中任务间的数据交换（Shuffle）机制，例如配置 JobMaster 上的数据分区跟踪。
    private final ShuffleMaster<?> shuffleMaster;
    // 分区跟踪器。
    // 跟踪作业中所有结果分区（Result Partition）的位置信息和状态，是 ShuffleMaster 的一个组件。
    private final JobMasterPartitionTracker jobMasterPartitionTracker;
    // 动态图模式标志。
    // 指示当前作业是否以动态图模式运行（例如，支持 Streaming Job 的 Job Vertex 动态添加/移除）。
    private final boolean isDynamicGraph;
    // 执行 Job Vertex 工厂。
    // 用于创建 ExecutionJobVertex 实例的工厂，用于创建 ExecutionGraph 的内部组件。
    private final ExecutionJobVertex.Factory executionJobVertexFactory;
    // 混合分区可见性标志。
    // 配置项，指定未完成的混合（Hybrid）结果分区是否应被视为状态未知（Unknown）。
    private final boolean nonFinishedHybridPartitionShouldBeUnknown;

    public DefaultExecutionGraphFactory(
            Configuration configuration,
            ClassLoader userCodeClassLoader,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            BlobWriter blobWriter,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker jobMasterPartitionTracker) {
        this(
                configuration,
                userCodeClassLoader,
                executionDeploymentTracker,
                futureExecutor,
                ioExecutor,
                rpcTimeout,
                jobManagerJobMetricGroup,
                blobWriter,
                shuffleMaster,
                jobMasterPartitionTracker,
                false,
                new ExecutionJobVertex.Factory(),
                false);
    }

    public DefaultExecutionGraphFactory(
            Configuration configuration,
            ClassLoader userCodeClassLoader,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            BlobWriter blobWriter,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker jobMasterPartitionTracker,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        this.configuration = configuration;
        this.userCodeClassLoader = userCodeClassLoader;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.rpcTimeout = rpcTimeout;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.blobWriter = blobWriter;
        this.shuffleMaster = shuffleMaster;
        this.jobMasterPartitionTracker = jobMasterPartitionTracker;
        this.isDynamicGraph = isDynamicGraph;
        this.executionJobVertexFactory = checkNotNull(executionJobVertexFactory);
        this.nonFinishedHybridPartitionShouldBeUnknown = nonFinishedHybridPartitionShouldBeUnknown;
    }
    // 创建、组装并尝试从状态恢复 ExecutionGraph
    @Override
    public ExecutionGraph createAndRestoreExecutionGraph(
            JobGraph jobGraph,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            CheckpointStatsTracker checkpointStatsTracker,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            ExecutionStateUpdateListener executionStateUpdateListener,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            Logger log)
            throws Exception {

        // 将任务部署事件桥接到 executionDeploymentTracker
        ExecutionDeploymentListener executionDeploymentListener =
                new ExecutionDeploymentTrackerDeploymentListenerAdapter(executionDeploymentTracker);
        ExecutionStateUpdateListener combinedExecutionStateUpdateListener =
                (execution, previousState, newState) -> {
                    executionStateUpdateListener.onStateUpdate(execution, previousState, newState);
                    if (newState.isTerminal()) {
                        executionDeploymentTracker.stopTrackingDeploymentOf(execution); //执行状态更新
                    }
                };
        // 委托构建
        // 将所有配置和组件传入，生成初始的 ExecutionGraph
        final ExecutionGraph newExecutionGraph =
                DefaultExecutionGraphBuilder.buildGraph(
                        jobGraph,
                        configuration,
                        futureExecutor,
                        ioExecutor,
                        userCodeClassLoader,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        rpcTimeout,
                        blobWriter,
                        log,
                        shuffleMaster,
                        jobMasterPartitionTracker,
                        partitionLocationConstraint,
                        executionDeploymentListener,
                        combinedExecutionStateUpdateListener,
                        initializationTimestamp,
                        vertexAttemptNumberStore,
                        vertexParallelismStore,
                        checkpointStatsTracker,
                        isDynamicGraph,
                        executionJobVertexFactory,
                        markPartitionFinishedStrategy,
                        nonFinishedHybridPartitionShouldBeUnknown,
                        jobManagerJobMetricGroup);
        //检查点协调器
        // 从新创建的 ExecutionGraph 中获取 CheckpointCoordinator 实例。
        final CheckpointCoordinator checkpointCoordinator =
                newExecutionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator != null) {
            // check whether we find a valid checkpoint
            // 尝试从 Completed Checkpoint Store 中存储的最新 Checkpoint 恢复
            if (!checkpointCoordinator.restoreInitialCheckpointIfPresent(
                    new HashSet<>(newExecutionGraph.getAllVertices().values()))) {

                // check whether we can restore from a savepoint
                // 检查是否有配置 Savepoint 恢复
                tryRestoreExecutionGraphFromSavepoint(
                        newExecutionGraph, jobGraph.getSavepointRestoreSettings());
            }
        }
        // 返回完全初始化并可能已恢复状态的 ExecutionGraph
        return newExecutionGraph;
    }

    /**
     * Tries to restore the given {@link ExecutionGraph} from the provided {@link
     * SavepointRestoreSettings}, iff checkpointing is enabled.
     *
     * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
     * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about
     *     the savepoint to restore from

     * @throws Exception if the {@link ExecutionGraph} could not be restored
     */
    // 尝试根据配置从 Savepoint 恢复 ExecutionGraph。
    private void tryRestoreExecutionGraphFromSavepoint(
            ExecutionGraph executionGraphToRestore,
            SavepointRestoreSettings savepointRestoreSettings)
            throws Exception {
        // 检查 savepointRestoreSettings.restoreSavepoint() 是否为真（即作业配置了要从 Savepoint 启动）。
        if (savepointRestoreSettings.restoreSavepoint()) {
            // 如果配置了 Savepoint 且 CheckpointCoordinator 存在，
            // 则调用 checkpointCoordinator.restoreSavepoint(...) 方法，执行 Savepoint 恢复逻辑，将状态加载到 ExecutionGraph 的任务实例中。
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraphToRestore.getCheckpointCoordinator();
            if (checkpointCoordinator != null) {
                checkpointCoordinator.restoreSavepoint(
                        savepointRestoreSettings,
                        executionGraphToRestore.getAllVertices(),
                        userCodeClassLoader);
            }
        }
    }
}
