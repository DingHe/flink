/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Records modifications of {@link org.apache.flink.runtime.executiongraph.ExecutionVertex
 * ExecutionVertices}, and allows for checking whether a vertex was modified.
 *
 * <p>Examples for modifications include:
 *
 * <ul>
 *   <li>cancellation of the underlying execution
 *   <li>deployment of the execution vertex
 * </ul>
 *
 * @see DefaultScheduler
 */
// ExecutionVertexVersioner（执行顶点版本管理器）是 Flink 调度器（Scheduler）内部使用的一个工具类，其核心作用是为作业中的每个执行顶点 (ExecutionVertex) 维护一个版本号。
// 版本跟踪： 当一个执行顶点（即一个任务实例）的状态发生关键性变化时（例如，从 CREATED 变为 RUNNING，或者被取消、重新部署等），它的版本号就会增加。
// 并发控制与一致性： 这个版本机制主要用于解决调度器内部的并发问题和确保状态一致性。例如，当一个调度行为是基于任务的旧状态或旧版本执行的，而任务在操作过程中被外部因素修改（比如被取消或失败）时，版本检查可以快速发现并拒绝过期的操作，避免基于陈旧信息做出错误的调度决策。
public class ExecutionVertexVersioner {
    // 版本映射表。
    // 核心数据结构。
    // 存储了每个 ExecutionVertexID（执行顶点 ID）与其当前最新版本号（Long 类型）之间的映射关系。
    private final Map<ExecutionVertexID, Long> executionVertexToVersion = new HashMap<>();

    // 记录单个执行顶点的修改，并返回该顶点的新版本对象。
    public ExecutionVertexVersion recordModification(final ExecutionVertexID executionVertexId) {
        final Long newVersion = executionVertexToVersion.merge(executionVertexId, 1L, Long::sum);
        return new ExecutionVertexVersion(executionVertexId, newVersion);
    }
    // 批量记录多个执行顶点的修改。
    public Map<ExecutionVertexID, ExecutionVertexVersion> recordVertexModifications(
            final Collection<ExecutionVertexID> vertices) {
        return vertices.stream()
                .map(this::recordModification)
                .collect(
                        Collectors.toMap(
                                ExecutionVertexVersion::getExecutionVertexId, Function.identity()));
    }
    // 检查给定版本的执行顶点是否已被修改。
    public boolean isModified(final ExecutionVertexVersion executionVertexVersion) {
        final Long currentVersion =
                getCurrentVersion(executionVertexVersion.getExecutionVertexId());
        return currentVersion != executionVertexVersion.getVersion();
    }

    // 获取指定执行顶点的当前最新版本号。
    private Long getCurrentVersion(ExecutionVertexID executionVertexId) {
        final Long currentVersion = executionVertexToVersion.get(executionVertexId);
        Preconditions.checkState(
                currentVersion != null,
                "Execution vertex %s does not have a recorded version",
                executionVertexId);
        return currentVersion;
    }
    // 从一组带有版本信息的顶点中，筛选出未被修改的执行顶点 ID 集合。
    public Set<ExecutionVertexID> getUnmodifiedExecutionVertices(
            final Set<ExecutionVertexVersion> executionVertexVersions) {
        return executionVertexVersions.stream()
                .filter(executionVertexVersion -> !isModified(executionVertexVersion))
                .map(ExecutionVertexVersion::getExecutionVertexId)
                .collect(Collectors.toSet());
    }
    // 批量获取一组执行顶点的当前版本对象。
    public Map<ExecutionVertexID, ExecutionVertexVersion> getExecutionVertexVersions(
            Collection<ExecutionVertexID> executionVertexIds) {
        return executionVertexIds.stream()
                .map(id -> new ExecutionVertexVersion(id, getCurrentVersion(id)))
                .collect(
                        Collectors.toMap(
                                ExecutionVertexVersion::getExecutionVertexId, Function.identity()));
    }
    // 获取单个执行顶点的当前版本对象。
    public ExecutionVertexVersion getExecutionVertexVersion(ExecutionVertexID executionVertexId) {
        final long currentVersion = getCurrentVersion(executionVertexId);
        return new ExecutionVertexVersion(executionVertexId, currentVersion);
    }
}
