/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Serializable;

/** This class encapsulates the data from the job manager to restore a task. */
// JobManagerTaskRestore 类封装了 JobManager 提供给 TaskManager 的所有必要数据，
// 用于让一个特定的任务（Task/Subtask）从一个 检查点 (Checkpoint) 恢复其状态。
// 主要作用是作为一个容器，保存一个任务从上一个成功检查点恢复所需要的信息。
// JobManager 决定从哪个检查点进行恢复，并为每个失败或重新启动的任务确定其应恢复的状态数据。
// JobManager 为每个任务创建一个 JobManagerTaskRestore 实例。
// 这个实例被序列化，并包含在 任务部署描述符 (TaskDeploymentDescriptor) 中，发送给承载该任务的 TaskManager。
// TaskManager 在启动任务之前，会解包这个对象，并使用其中的 TaskStateSnapshot 来加载任务的历史状态，确保任务可以从上次成功的状态继续执行，实现精确一次 (Exactly-Once) 的处理语义。
public class JobManagerTaskRestore implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The id of the checkpoint from which we restore. */
    // 恢复检查点 ID。
    // 记录了任务将从中恢复的那个检查点的唯一 ID。
    // 这个 ID 是一个递增的数字，用于标识特定的检查点。
    private final long restoreCheckpointId;

    /** The state for this task to restore. */
    // 任务状态快照。
    // 包含了该特定任务的实际状态元数据。
    // 它指向了任务的托管状态 (Managed State) 和原始状态 (Raw State) 在状态后端（如 HDFS、RocksDB）中的存储位置和结构信息。
    private final TaskStateSnapshot taskStateSnapshot;

    public JobManagerTaskRestore(
            @Nonnegative long restoreCheckpointId, @Nonnull TaskStateSnapshot taskStateSnapshot) {
        this.restoreCheckpointId = restoreCheckpointId;
        this.taskStateSnapshot = taskStateSnapshot;
    }

    public long getRestoreCheckpointId() {
        return restoreCheckpointId;
    }

    @Nonnull
    public TaskStateSnapshot getTaskStateSnapshot() {
        return taskStateSnapshot;
    }

    @Override
    public String toString() {
        return "JobManagerTaskRestore{"
                + "restoreCheckpointId="
                + restoreCheckpointId
                + ", taskStateSnapshot="
                + taskStateSnapshot
                + '}';
    }
}
