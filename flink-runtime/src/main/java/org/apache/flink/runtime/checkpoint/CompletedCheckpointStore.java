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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** A bounded LIFO-queue of {@link CompletedCheckpoint} instances. */
// CompletedCheckpointStore（已完成检查点存储）是 Flink 检查点协调器 (Checkpoint Coordinator) 的核心组件。
// 它的作用是作为所有成功完成的 CompletedCheckpoint 实例的持久化和有界存储库。
// 存储和检索： 它负责安全地存储最新的 N 个已完成的检查点，以便 Flink 在作业失败时可以从中检索并恢复状态。
// 有界队列 (Bounded Queue)： 它的特性是 LIFO 队列，但容量受限。当达到最大容量时，它会自动丢弃（Subsume）最旧的检查点，并触发对旧检查点相关状态的清理，从而控制状态存储占用的空间。
// 高可用性 (HA)： 在 JobManager 发生故障时，如果配置了高可用性，实现类（如基于 Zookeeper 的存储）会确保检查点元数据不会丢失。

public interface CompletedCheckpointStore {

    Logger LOG = LoggerFactory.getLogger(CompletedCheckpointStore.class);

    /**
     * Adds a {@link CompletedCheckpoint} instance to the list of completed checkpoints.
     *
     * <p>Only a bounded number of checkpoints is kept. When exceeding the maximum number of
     * retained checkpoints, the oldest one will be discarded.
     *
     * <p>After <a href="https://issues.apache.org/jira/browse/FLINK-24611">FLINK-24611</a>, {@link
     * SharedStateRegistry#unregisterUnusedState} should be called here to subsume unused state.
     * <font color="#FF0000"><strong>Note</strong></font>, the {@link CompletedCheckpoint} passed to
     * {@link SharedStateRegistry#registerAllAfterRestored} or {@link
     * SharedStateRegistryFactory#create} must be the same object as the input parameter, otherwise
     * the state may be deleted by mistake.
     *
     * <p>After <a href="https://issues.apache.org/jira/browse/FLINK-25872">FLINK-25872</a>, {@link
     * CheckpointsCleaner#cleanSubsumedCheckpoints} should be called explicitly here.
     *
     * @return the subsumed oldest completed checkpoint if possible, return null if no checkpoint
     *     needs to be discarded on subsume.
     */
    // 将一个新完成的检查点添加到存储中，并根据容量限制自动丢弃最旧的一个检查点（如果需要）。
    // 参数 checkpoint： 新完成的检查点实例。
    // 参数 checkpointsCleaner： 负责实际清理被丢弃检查点数据的工具类
    // 参数 postCleanup： 在清理工作完成后执行的回调函数（Java 8 Runnable）。
    @Nullable
    CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception;

    /**
     * Returns the latest {@link CompletedCheckpoint} instance or <code>null</code> if none was
     * added.
     */
    // 返回存储中最新的（ID最大的）已完成检查点。
    default CompletedCheckpoint getLatestCheckpoint() {
        List<CompletedCheckpoint> allCheckpoints = getAllCheckpoints();
        if (allCheckpoints.isEmpty()) {
            return null;
        }

        return allCheckpoints.get(allCheckpoints.size() - 1);
    }

    /** Returns the id of the latest completed checkpoints. */
    // 返回最新已完成检查点的 ID。
    // 基于 getLatestCheckpoint() 获取 ID。
    // 用于快速查询当前最新的 Checkpoint 序列号。
    default long getLatestCheckpointId() {
        try {
            List<CompletedCheckpoint> allCheckpoints = getAllCheckpoints();
            if (allCheckpoints.isEmpty()) {
                return 0;
            }

            return allCheckpoints.get(allCheckpoints.size() - 1).getCheckpointID();
        } catch (Throwable throwable) {
            LOG.warn("Get the latest completed checkpoints failed", throwable);
            return 0;
        }
    }

    /**
     * Shuts down the store.
     *
     * <p>The job status is forwarded and used to decide whether state should actually be discarded
     * or kept. {@link SharedStateRegistry#unregisterUnusedState} and {@link
     * CheckpointsCleaner#cleanSubsumedCheckpoints} should be called here to subsume unused state.
     *
     * @param jobStatus Job state on shut down
     * @param checkpointsCleaner that will cleanup completed checkpoints if needed
     */
    // 关闭检查点存储，
    // 并根据作业的最终状态决定是否清理所有状态。
    void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner) throws Exception;

    /**
     * Returns all {@link CompletedCheckpoint} instances.
     *
     * <p>Returns an empty list if no checkpoint has been added yet.
     */
    // 返回存储中当前保留的所有已完成检查点的列表
    List<CompletedCheckpoint> getAllCheckpoints();

    /** Returns the current number of retained checkpoints. */
    // 返回当前存储中实际保留的检查点数量。
    int getNumberOfRetainedCheckpoints();

    /** Returns the max number of retained checkpoints. */
    // 返回该存储允许保留的最大检查点数量限制。
    int getMaxNumberOfRetainedCheckpoints();

    /**
     * This method returns whether the completed checkpoint store requires checkpoints to be
     * externalized. Externalized checkpoints have their meta data persisted, which the checkpoint
     * store can exploit (for example by simply pointing the persisted metadata).
     *
     * @return True, if the store requires that checkpoints are externalized before being added,
     *     false if the store stores the metadata itself.
     */
    // 指示该存储是否要求在添加之前，检查点元数据必须已经被外部化 (Externalized)。
    // 如果返回 True，表示实现类（如基于 Zookeeper 的存储）只存储外部化检查点文件的引用（路径），而不是自己序列化完整的元数据。
    boolean requiresExternalizedCheckpoints();

    /** Returns the {@link SharedStateRegistry} used to register the shared state. */
    // 返回用于注册和管理共享状态的注册表。
    // SharedStateRegistry 负责跟踪哪些 Checkpoint 正在使用哪些共享状态（如 RocksDB 的文件块）。
    // 这确保了只有在所有引用共享状态的 Checkpoint 都被丢弃后，共享状态才会被安全删除。
    SharedStateRegistry getSharedStateRegistry();
}
