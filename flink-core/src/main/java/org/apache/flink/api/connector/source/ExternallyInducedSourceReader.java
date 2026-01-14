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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Sources that implement this interface delay checkpoints when receiving a trigger message from the
 * checkpoint coordinator to the point when their input data/events indicate that a checkpoint
 * should be triggered.
 *
 * <p>The ExternallyInducedSourceReader tells the Flink runtime that a checkpoint needs to be made
 * by returning a checkpointId when {@link #shouldTriggerCheckpoint()} is invoked.
 *
 * <p>The implementations typically works together with the {@link SplitEnumerator} which informs
 * the external system to trigger a checkpoint. The external system also needs to forward the
 * Checkpoint ID to the source, so the source knows which checkpoint to trigger.
 *
 * <p><b>Important:</b> It is crucial that all parallel source tasks trigger their checkpoints at
 * roughly the same time. Otherwise this leads to performance issues due to long checkpoint
 * alignment phases or large alignment data snapshots.
 *
 * @param <T> The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 */
// 在 Flink 的 Source 连接器架构（FLIP-27）中，ExternallyInducedSourceReader 是一个特殊的接口。
// 它打破了 Flink 传统的“由 Checkpoint 协调器统一触发”的模式，赋予了数据源主动触发 Checkpoint 的能力。
// 在 Flink 默认的机制中，Checkpoint 是由 JobManager 端的 Checkpoint Coordinator 定期发起的。但在某些特殊场景下（例如从消息队列 Pravega 或某些具有事务特性的外部系统读取数据时），数据源（Source）本身更清楚什么时候该做快照。
// 外部诱导/触发： 它允许 Source Reader 根据从外部系统接收到的特定消息（如 Checkpoint 标记位、特定的 Event 等）来告知 Flink 运行环境：“现在应该开始做 Checkpoint 了”。
// 同步外部事务： 确保 Flink 的 Checkpoint 边界与外部存储系统的事务边界或数据分块边界严格一致。

@Experimental
@PublicEvolving
public interface ExternallyInducedSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {

    /**
     * A method that informs the Flink runtime whether a checkpoint should be triggered on this
     * Source.
     *
     * <p>This method is invoked when the previous {@link #pollNext(ReaderOutput)} returns {@link
     * org.apache.flink.core.io.InputStatus#NOTHING_AVAILABLE}, to check if the source needs to be
     * checkpointed.
     *
     * <p>If a CheckpointId is returned, a checkpoint will be triggered on this source reader.
     * Otherwise, Flink runtime will continue to process the records.
     *
     * @return An optional checkpoint ID that Flink runtime should take a checkpoint for.
     */
    // 调用时机： Flink 运行时会在 pollNext(ReaderOutput) 返回 InputStatus.NOTHING_AVAILABLE（即当前没有新数据可读）时，
    // 主动调用此方法进行检查。
    // 返回 Optional.of(checkpointId)：Source Reader 告诉 Flink ：“请立刻以这个 checkpointId 发起一次 Checkpoint”。
    // Flink 接收到这个信号后，会启动快照流程。
    Optional<Long> shouldTriggerCheckpoint();
}
