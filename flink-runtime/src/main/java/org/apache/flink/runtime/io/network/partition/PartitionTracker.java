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

package org.apache.flink.runtime.io.network.partition;

import java.util.Collection;

/**
 * Utility for tracking partitions.
 *
 * <p>This interface deliberately does not have a method to start tracking partitions, so that
 * implementation are flexible in their definitions for this method (otherwise one would end up with
 * multiple methods, with one part likely being unused).
 */
// 在 Flink 的调度和数据传输子系统中，尤其是涉及到 Shuffle（数据混洗）服务的组件（如 JobMaster 或 TaskManager 上的某些服务），
// 需要知道哪些上游结果分区是可用的、正在被消费的或已完成的。
// PartitionTracker 作为一个抽象工具，它的核心作用是维护一个结果分区到其元数据或状态的映射关系，并提供停止跟踪和查询状态的方法
// 管理映射： 维护一个从抽象的键（K，通常是 JobID 或 ExecutionAttemptID）到结果分区（ResultPartitionID）的集合的映射。
// K	键类型 (Key)	通常是用于分组和清理的唯一标识符，例如 JobID (作业 ID) 或 ExecutionAttemptID (执行尝试 ID)。
// M	元数据类型 (Metadata)	与被跟踪分区关联的额外信息，例如存储在本地的文件路径、网络连接地址或分区状态。
public interface PartitionTracker<K, M> {

    /** Stops the tracking of all partitions for the given key. */
    // 停止跟踪与给定键关联的所有分区。
    Collection<PartitionTrackerEntry<K, M>> stopTrackingPartitionsFor(K key);

    /** Stops the tracking of the given partitions. */
    // 停止跟踪指定的单个或多个分区。
    Collection<PartitionTrackerEntry<K, M>> stopTrackingPartitions(
            Collection<ResultPartitionID> resultPartitionIds);

    /** Returns whether any partition is being tracked for the given key. */
    // 查询给定键下是否有分区正在被跟踪。
    boolean isTrackingPartitionsFor(K key);

    /** Returns whether the given partition is being tracked. */
    // 查询给定键下是否有分区正在被跟踪。
    boolean isPartitionTracked(ResultPartitionID resultPartitionID);
}
