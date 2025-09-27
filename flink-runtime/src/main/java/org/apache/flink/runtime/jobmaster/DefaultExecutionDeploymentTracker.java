/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
//跟踪任务执行部署状态的默认实现类
/** Default {@link ExecutionDeploymentTracker} implementation. */
public class DefaultExecutionDeploymentTracker implements ExecutionDeploymentTracker {
    //用于存储当前处于挂起状态（尚未完成部署）的任务执行 ID（ExecutionAttemptID）。这些任务执行已经被调度，但尚未成功部署。
    private final Set<ExecutionAttemptID> pendingDeployments = new HashSet<>();
    private final Map<ResourceID, Set<ExecutionAttemptID>> executionsByHost = new HashMap<>(); //该映射用于存储任务执行器上承载的所有任务执行，方便根据任务执行器查询其部署的任务
    private final Map<ExecutionAttemptID, ResourceID> hostByExecution = new HashMap<>(); //每个任务执行应该部署在哪个任务执行器上

    @Override  //开始跟踪一个任务执行的部署状态，当任务执行被调度并准备部署时，调用该方法开始追踪它
    public void startTrackingPendingDeploymentOf(
            ExecutionAttemptID executionAttemptId, ResourceID host) {
        pendingDeployments.add(executionAttemptId);
        hostByExecution.put(executionAttemptId, host);
        executionsByHost.computeIfAbsent(host, ignored -> new HashSet<>()).add(executionAttemptId);
    }

    @Override //标记一个任务执行的部署已完成
    public void completeDeploymentOf(ExecutionAttemptID executionAttemptId) {
        pendingDeployments.remove(executionAttemptId);
    }

    @Override //用于停止跟踪一个任务执行的部署
    public void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId) {
        pendingDeployments.remove(executionAttemptId);
        ResourceID host = hostByExecution.remove(executionAttemptId);
        if (host != null) {
            executionsByHost.computeIfPresent(
                    host,
                    (resourceID, executionAttemptIds) -> {
                        executionAttemptIds.remove(executionAttemptId);

                        return executionAttemptIds.isEmpty() ? null : executionAttemptIds;
                    });
        }
    }

    @Override //获取指定任务执行器上所有的任务执行及其部署状态
    public Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host) {
        return executionsByHost.getOrDefault(host, Collections.emptySet()).stream()
                .collect(
                        Collectors.toMap(
                                x -> x,
                                x ->
                                        pendingDeployments.contains(x)
                                                ? ExecutionDeploymentState.PENDING
                                                : ExecutionDeploymentState.DEPLOYED));
    }
}
