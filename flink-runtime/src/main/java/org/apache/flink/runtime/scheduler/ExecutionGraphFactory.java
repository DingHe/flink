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

import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.VertexAttemptNumberStore;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;

// ExecutionGraphFactory（执行图工厂）是 Flink 框架中负责创建并从历史状态恢复 ExecutionGraph 核心对象的工厂接口。
// 执行图 (ExecutionGraph)： 是 Flink 作业的核心运行时表示，它是逻辑作业图 (JobGraph) 在运行时环境中的实例化版本，包含了所有任务（Execution Vertices）、它们之间的依赖关系、当前状态、分配的资源等信息。
// 接收一个逻辑 JobGraph 和所有必需的运行时服务组件，然后创建一个完整的、准备好被调度的 ExecutionGraph 实例。如果存在历史状态，它还会负责将 Checkpoint 状态信息关联到新创建的 ExecutionGraph 上，完成恢复工作
/** Factory for creating an {@link ExecutionGraph}. */
public interface ExecutionGraphFactory {

    /**
     * Create and restore {@link ExecutionGraph} from the given {@link JobGraph} and services.
     *
     * @param jobGraph jobGraph to initialize the ExecutionGraph with
     * @param completedCheckpointStore completedCheckpointStore to pass to the CheckpointCoordinator
     * @param checkpointsCleaner checkpointsCleaner to pass to the CheckpointCoordinator
     * @param checkpointIdCounter checkpointIdCounter to pass to the CheckpointCoordinator
     * @param checkpointStatsTracker The {@link CheckpointStatsTracker} that's used for collecting
     *     the checkpoint-related statistics.
     * @param partitionLocationConstraint partitionLocationConstraint for this job
     * @param initializationTimestamp initializationTimestamp when the ExecutionGraph was created
     * @param vertexAttemptNumberStore vertexAttemptNumberStore keeping information about the vertex
     *     attempts of previous runs
     * @param vertexParallelismStore vertexMaxParallelismStore keeping information about the vertex
     *     max parallelism settings
     * @param executionStateUpdateListener listener for state transitions of the individual
     *     executions
     * @param log log to use for logging
     * @return restored {@link ExecutionGraph}
     * @throws Exception if the {@link ExecutionGraph} could not be created and restored
     */
    ExecutionGraph createAndRestoreExecutionGraph(
            // 逻辑作业图。
            // 包含作业的拓扑结构、配置和算子（Operator）信息。它是构建 ExecutionGraph 的蓝图。
            JobGraph jobGraph,
            // 已完成检查点存储。
            // 用于在 JobMaster 故障恢复时，获取最新的成功 Checkpoint 元数据，并将其传递给 CheckpointCoordinator。
            CompletedCheckpointStore completedCheckpointStore,
            // 检查点清理器。
            // 用于异步清理不再需要的旧 Checkpoint 文件，传递给 CheckpointCoordinator。
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            // 检查点统计跟踪器。
            // 用于收集和报告 Checkpoint 相关的性能指标。
            CheckpointStatsTracker checkpointStatsTracker,
            // 结果分区位置约束。
            // 定义了结果分区（如 IntermediateResultPartition）在 TaskManager 上的存储和管理策略，影响调度和数据恢复。
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            long initializationTimestamp,
            // 顶点尝试次数存储。
            // 跟踪作业不同运行尝试中，每个任务顶点 (ExecutionVertex) 的失败尝试次数。
            VertexAttemptNumberStore vertexAttemptNumberStore,
            // 顶点并行度存储。
            // 存储关于 JobVertex 的最大并行度等配置信息。
            VertexParallelismStore vertexParallelismStore,
            // 执行状态更新监听器。
            // 允许外部组件（如调度器）监听并响应每个任务实例 (Execution) 的状态转换（例如，从 RUNNING 到 FAILED）。
            ExecutionStateUpdateListener executionStateUpdateListener,
            // 标记分区完成策略。
            // 定义了何时将结果分区标记为完成（例如，在所有上游生产者完成后）。
            // 这对于 JobGraph 的阶段性完成和数据释放至关重要。
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            Logger log)
            throws Exception;
}
