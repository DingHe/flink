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

import org.apache.flink.api.common.JobStatus;

/** Interface for querying the state of a job and the timestamps of state transitions. */
// JobStatusProvider 接口的作用是提供一个统一的机制，用于查询 Flink 作业的当前状态（JobStatus）以及该作业进入各个状态的时间戳。
// 在 Flink 运行时，例如 ExecutionGraph（执行图）或更高级别的调度组件通常会实现此接口，以便外部或内部组件能够获取作业的健康状况和生命周期信息。
public interface JobStatusProvider {

    /**
     * Returns the current {@link JobStatus} for this execution graph.
     *
     * @return job status for this execution graph
     */
    // 获取当前作业状态。
    // 返回作业当前的 JobStatus 枚举值。这是最常用的方法，用于检查作业是否正在运行、已完成或发生故障。
    JobStatus getState();

    /**
     * Returns the timestamp for the given {@link JobStatus}.
     *
     * @param status status for which the timestamp should be returned
     * @return timestamp for the given job status
     */
    // 获取状态时间戳。
    // 返回作业第一次进入给定 JobStatus 状态时的时间戳（通常是 Unix 毫秒时间）。
    // 例如，可以查询作业何时进入 RUNNING 状态或 FINISHED 状态。
    long getStatusTimestamp(JobStatus status);
}
