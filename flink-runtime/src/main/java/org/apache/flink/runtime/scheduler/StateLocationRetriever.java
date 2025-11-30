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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Optional;

/** Component to retrieve the state location of an execution vertex. */
// 核心作用是为 Flink 调度器提供任务状态的存储位置信息，以支持本地化调度 (Locality Scheduling)
// 本地化优化： 在 Flink 的某些部署场景（如 JobManager 故障恢复或 Task 重新启动）中，
// 如果一个任务的状态（例如，在 RocksDB 中保存的数据或历史 Checkpoint 文件）存储在某个特定的 TaskManager 上，
// 那么将该任务重新调度到这个 TaskManager 上运行，可以避免昂贵的网络数据传输，从而显著提高恢复速度和效率。
@FunctionalInterface
public interface StateLocationRetriever {

    /**
     * Returns state location of an execution vertex.
     *
     * @param executionVertexId id of the execution vertex
     * @return optional that is assigned with the vertex's state location if the location exists,
     *     otherwise empty
     */
    // 返回指定执行顶点 (Execution Vertex) 状态的存储位置。
    // 参数 executionVertexId： 需要查询状态位置的任务实例的唯一 ID。
    Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId);
}
