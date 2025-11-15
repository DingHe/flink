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
package org.apache.flink.runtime.state;

import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class contains the combined results from the snapshot of a state backend:
 *
 * <ul>
 *   <li>A state object representing the state that will be reported to the Job Manager to
 *       acknowledge the checkpoint.
 *   <li>A state object that represents the state for the {@link TaskLocalStateStoreImpl}.
 * </ul>
 *
 * Both state objects are optional and can be null, e.g. if there was no state to snapshot in the
 * backend. A local state object that is not null also requires a state to report to the job manager
 * that is not null, because the Job Manager always owns the ground truth about the checkpointed
 * state.
 */
// SnapshotResult 类用于封装 Flink 状态后端（State Backend）执行快照操作后返回的组合结果。
// 它的主要目的是将同一个 Checkpoint 的远程（JobManager 所有） 和本地（TaskLocalStateStore 所有） 两种状态句柄（StateObject）打包在一起。
// 组合状态句柄： Checkpoint 结果通常包含两部分：一份是需要报告给 JobManager 的状态句柄（JobManager 负责容错和协调），另一份是可以存储在 TaskManager 本地以供快速恢复的状态句柄。
// 强制一致性： 它强制执行一个规则：如果存在本地状态快照，则必须存在对应的 JobManager 所有状态快照，因为 JobManager 始终拥有关于 Checkpoint 状态的“事实真相”（Ground Truth）。
public class SnapshotResult<T extends StateObject> implements StateObject {

    private static final long serialVersionUID = 1L;

    /** An singleton instance to represent an empty snapshot result. */
    // 空结果单例。
    // 一个静态的单例对象，用于表示 Checkpoint 操作返回了一个空快照（即没有状态需要保存）
    private static final SnapshotResult<?> EMPTY = new SnapshotResult<>(null, null);

    /**
     * This is the state snapshot that will be reported to the Job Manager to acknowledge a
     * checkpoint.
     */
    // JobManager 所有快照。
    // 存储将报告给 JobManager 的状态句柄（Handle）。
    // JobManager 将此句柄用于全局容错、协调和状态恢复。
    // 通常指向： 远程存储（如 HDFS 或 S3）中的状态数据。
    private final T jobManagerOwnedSnapshot;

    /**
     * This is the state snapshot that will be reported to the Job Manager to acknowledge a
     * checkpoint.
     */
    // 任务本地快照。
    // 作用： 存储将报告给 TaskLocalStateStore 的状态句柄。此句柄用于快速本地恢复，避免从远程存储下载数据。
    private final T taskLocalSnapshot;

    /**
     * Creates a {@link SnapshotResult} for the given jobManagerOwnedSnapshot and taskLocalSnapshot.
     * If the jobManagerOwnedSnapshot is null, taskLocalSnapshot must also be null.
     *
     * @param jobManagerOwnedSnapshot Snapshot for report to job manager. Can be null.
     * @param taskLocalSnapshot Snapshot for report to local state manager. This is optional and
     *     requires jobManagerOwnedSnapshot to be not null if this is not also null.
     */
    private SnapshotResult(T jobManagerOwnedSnapshot, T taskLocalSnapshot) {

        if (jobManagerOwnedSnapshot == null && taskLocalSnapshot != null) {
            throw new IllegalStateException(
                    "Cannot report local state snapshot without corresponding remote state!");
        }

        this.jobManagerOwnedSnapshot = jobManagerOwnedSnapshot;
        this.taskLocalSnapshot = taskLocalSnapshot;
    }

    @Nullable
    public T getJobManagerOwnedSnapshot() {
        return jobManagerOwnedSnapshot;
    }

    @Nullable
    public T getTaskLocalSnapshot() {
        return taskLocalSnapshot;
    }

    @Override
    public void discardState() throws Exception {

        Exception aggregatedExceptions = null;

        if (jobManagerOwnedSnapshot != null) {
            try {
                jobManagerOwnedSnapshot.discardState();
            } catch (Exception remoteDiscardEx) {
                aggregatedExceptions = remoteDiscardEx;
            }
        }

        if (taskLocalSnapshot != null) {
            try {
                taskLocalSnapshot.discardState();
            } catch (Exception localDiscardEx) {
                aggregatedExceptions =
                        ExceptionUtils.firstOrSuppressed(localDiscardEx, aggregatedExceptions);
            }
        }

        if (aggregatedExceptions != null) {
            throw aggregatedExceptions;
        }
    }

    @Override
    public long getStateSize() {
        return jobManagerOwnedSnapshot != null ? jobManagerOwnedSnapshot.getStateSize() : 0L;
    }

    @SuppressWarnings("unchecked")
    public static <T extends StateObject> SnapshotResult<T> empty() {
        return (SnapshotResult<T>) EMPTY;
    }

    public static <T extends StateObject> SnapshotResult<T> of(@Nullable T jobManagerState) {
        return jobManagerState != null ? new SnapshotResult<>(jobManagerState, null) : empty();
    }

    public static <T extends StateObject> SnapshotResult<T> withLocalState(
            @Nonnull T jobManagerState, @Nonnull T localState) {
        return new SnapshotResult<>(jobManagerState, localState);
    }
}
