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

import java.util.Collection;

/** Interface for triggering actions in case of state mismatches. */
public interface ExecutionDeploymentReconciliationHandler {
    /**
     * Called if some executions are expected to be hosted on a task executor, but aren't.
     *  当 Flink 检测到某些任务执行没有部署到预期的任务执行器上时，会调用该方法。该方法可以触发一些操作来修复这种状态不匹配，比如重新调度任务或触发错误处理等
     * @param executionAttemptIds ids of the missing deployments
     * @param hostingTaskExecutor expected hosting task executor
     */
    void onMissingDeploymentsOf(
            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID hostingTaskExecutor);
     //hostingTaskExecutor 表示预计应该承载这些执行任务的任务执行器
    /**
     * Called if some executions are hosted on a task executor, but we don't expect them.
     *  当 Flink 检测到任务执行器上存在未预期的任务执行时，会调用该方法。该方法可以触发一些操作来管理或停止这些意外的任务执行
     * @param executionAttemptIds ids of the unknown executions
     * @param hostingTaskExecutor hosting task executor
     */
    void onUnknownDeploymentsOf(
            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID hostingTaskExecutor);
}
