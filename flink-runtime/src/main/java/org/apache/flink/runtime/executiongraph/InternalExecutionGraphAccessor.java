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
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * This interface encapsulates all methods needed by ExecutionJobVertex / ExecutionVertices /
 * Execution from the DefaultExecutionGraph.
 */
// InternalExecutionGraphAccessor 接口是 Flink 执行图（ExecutionGraph）对外暴露的一个访问层/门面（Facade）
// 主要作用是将 ExecutionGraph 的核心功能和内部状态封装起来，并提供给 ExecutionGraph 内部组件（如 ExecutionJobVertex、ExecutionVertex 和 Execution 实例）进行访问和操作。
// 这遵循了依赖倒置原则，允许 Execution 组件在不直接持有或了解完整 DefaultExecutionGraph 复杂性的情况下，与图的其余部分进行交互。
// 提供上下文： 允许内部执行组件获取必要的服务、配置和运行时上下文（如类加载器、线程执行器、JobID 等）。
// 状态变更通知： 提供方法供 Execution 实例通知 ExecutionGraph 全局状态的变更（如任务状态转换、全局故障）。
public interface InternalExecutionGraphAccessor {
    // 获取用户类加载器。
    // 用于加载用户代码和依赖项。
    ClassLoader getUserClassLoader();

    JobID getJobID();
    // 获取 BLOB 写入器。
    // 用于将永久性文件（如 JAR 包、配置）上传到 BLOB 服务。
    BlobWriter getBlobWriter();

    /**
     * Returns the ExecutionContext associated with this ExecutionGraph.
     *
     * @return ExecutionContext associated with this ExecutionGraph
     */
    // 获取 Future 执行器。
    // 返回一个通用的线程池，用于执行异步操作（如 Future 的回调）。
    Executor getFutureExecutor();
    // 获取 JobMaster 主线程执行器。
    // 返回一个执行器，用于在 JobMaster 的主调度线程上安全地执行操作，确保状态一致性。
    @Nonnull
    ComponentMainThreadExecutor getJobMasterMainThreadExecutor();
    // 获取 ShuffleMaster。
    // 返回与外部 Shuffle 服务交互的组件，用于注册和查询 Shuffle 描述符。
    ShuffleMaster<? extends ShuffleDescriptor> getShuffleMaster();
    // 获取分区跟踪器。
    // 返回用于跟踪和管理结果分区生命周期的组件。
    JobMasterPartitionTracker getPartitionTracker();
    // 注册执行实例。
    // 当一个新的 Execution 对象创建并开始运行时，将其注册到 ExecutionGraph 的全局跟踪结构中。
    void registerExecution(Execution exec);
    // 注销执行实例。
    // 当一个 Execution 结束其生命周期时（例如，完成或失败），将其从全局跟踪中移除。
    void deregisterExecution(Execution exec);
    // 获取分区组释放策略。
    // 返回决定何时释放上游中间结果分区组（ConsumedPartitionGroup）的策略。
    PartitionGroupReleaseStrategy getPartitionGroupReleaseStrategy();
    // JobVertex 完成通知。
    // 通知 ExecutionGraph 有一个 ExecutionJobVertex 已经完成了所有子任务的执行。
    void jobVertexFinished();

    // JobVertex 取消完成通知。
    // 通知 ExecutionGraph 有一个 ExecutionJobVertex 因为重启等原因不再处于完成状态。
    void jobVertexUnFinished();
    // 获取执行部署监听器。
    // 用于监听任务部署生命周期事件（例如，当任务被部署或取消部署时）。
    ExecutionDeploymentListener getExecutionDeploymentListener();

    /**
     * Fails the execution graph globally.
     *
     * <p>This global failure is meant to be triggered in cases where the consistency of the
     * execution graph' state cannot be guaranteed any more (for example when catching unexpected
     * exceptions that indicate a bug or an unexpected call race), and where a full restart is the
     * safe way to get consistency back.
     *
     * @param t The exception that caused the failure.
     */
    // 触发全局故障。
    // 在 ExecutionGraph 状态一致性无法保证的严重情况下，触发整个作业的全局性故障，通常会导致整个作业重启。
    void failGlobal(Throwable t);
    // 通知任务状态变更。
    // 当某个 Execution 实例的状态（如从 RUNNING 到 FINISHED）发生变化时，通知 ExecutionGraph 进行处理。
    void notifyExecutionChange(
            Execution execution, ExecutionState previousState, ExecutionState newExecutionState);
    // 通知调度器内部任务故障。
    // 当任务因内部异常失败时，通知调度器进行处理，可以携带是否取消任务和是否释放分区的指令。
    void notifySchedulerNgAboutInternalTaskFailure(
            ExecutionAttemptID attemptId,
            Throwable t,
            boolean cancelTask,
            boolean releasePartitions);
    // 获取边管理器。
    // 返回用于管理执行图任务间数据依赖（边）的组件。
    EdgeManager getEdgeManager();
    // 获取执行顶点。
    // 根据 ID 查找并返回对应的 ExecutionVertex 实例，如果找不到则抛出异常。
    ExecutionVertex getExecutionVertexOrThrow(ExecutionVertexID id);
    // 获取结果分区。
    // 根据 ID 查找并返回对应的 IntermediateResultPartition 实例，如果找不到则抛出异常。
    IntermediateResultPartition getResultPartitionOrThrow(final IntermediateResultPartitionID id);
    // 删除 BLOB 文件。
    // 请求删除与给定永久 BLOB Key 关联的文件，通常用于作业完成后的清理。
    void deleteBlobs(List<PermanentBlobKey> blobKeys);
    // 获取作业顶点。
    // 根据 ID 查找并返回对应的 ExecutionJobVertex 实例。
    ExecutionJobVertex getJobVertex(JobVertexID id);
    // 检查是否为动态图。
    // 返回执行图是否启用了动态图模式。
    boolean isDynamic();
    // 获取 ExecutionGraph ID。
    // 返回当前执行图实例的唯一 ID。
    ExecutionGraphID getExecutionGraphID();

    /** Get the shuffle descriptors of the cluster partitions ordered by partition number. */
    // 获取集群分区的 Shuffle 描述符。
    // 用于获取持久化（集群）分区的数据连接信息。
    List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateResultPartition);

    // 获取标记分区完成策略。
    // 返回一个策略，用于决定何时将上游结果分区标记为“已完成”。
    MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy();

    /**
     * Get the input info of a certain input of a certain job vertex.
     *
     * @param jobVertexId the job vertex id
     * @param resultId the input(intermediate result) id
     * @return the input info
     */
    // 获取 JobVertex 输入信息。
    // 查询特定 JobVertex 对特定中间结果的输入信息。
    JobVertexInputInfo getJobVertexInputInfo(
            JobVertexID jobVertexId, IntermediateDataSetID resultId);
    // 获取任务部署描述符工厂。
    // 用于创建 TaskDeploymentDescriptor，这些描述符最终会被发送给 TaskExecutor 来启动任务。
    TaskDeploymentDescriptorFactory getTaskDeploymentDescriptorFactory();
}
