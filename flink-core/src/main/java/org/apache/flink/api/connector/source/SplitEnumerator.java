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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.state.CheckpointListener;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * The interface for a split enumerator responsible for discovering the source splits, and assigning
 * them to the {@link SourceReader}.
 */
// SplitEnumerator 接口是 Flink 统一 Source API 的核心组件，它负责 发现、管理 和 分配 数据分片（Splits）给 Flink 的并行 SourceReader。
// 它运行在 Flink 的 JobManager 上（或一个特殊的协调者任务中），充当整个数据源的协调者。
// 分片发现和分配： 持续监控外部数据源（如新增的文件、Kafka 分区），并将新发现的 SourceSplit 分配给空闲的 SourceReader。
// 容错和状态管理： 参与 Flink 的检查点机制，通过 snapshotState 保存其分配进度，并在 addSplitsBack 中处理读取器失败后返回的分片。
// Reader 生命周期管理： 追踪当前运行的 SourceReader 实例，处理它们的注册 (addReader) 和分片请求 (handleSplitRequest)。
// SplitEnumerator 是 Source 的大脑，负责动态管理数据源的拓扑结构和分片工作负载的均衡分配。
// <SplitT>	分片类型	SourceSplit 的具体类型，表示数据源中的一个可分配数据单元（例如 FileSourceSplit）。
// <CheckpointT>	检查点类型	枚举器状态的类型。 用于在检查点中保存和恢复 SplitEnumerator 的内部状态（如已分配分片、待分配分片）。
@Public
public interface SplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        extends AutoCloseable, CheckpointListener {

    /**
     * Start the split enumerator.
     *
     * <p>The default behavior does nothing.
     */
    // 启动枚举器。
    // 当 Flink 任务启动时调用，用于初始化枚举器的内部资源，例如启动线程或连接外部系统以发现分片。
    // 默认实现为空。
    void start();

    /**
     * Handles the request for a split. This method is called when the reader with the given subtask
     * id calls the {@link SourceReaderContext#sendSplitRequest()} method.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param requesterHostname Optional, the hostname where the requesting task is running. This
     *     can be used to make split assignments locality-aware.
     */
    // 处理分片请求。
    // 当一个 SourceReader 缺乏工作（需要新的分片）时，它会向 SplitEnumerator 发送请求，此方法被调用。
    // subtaskId 标识请求的读取器。
    // requesterHostname（可选）允许枚举器进行本地化感知的分配（将 Split 分配给存储该 Split 数据的 TaskManager），以优化网络传输。
    void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);

    /**
     * Add splits back to the split enumerator. This will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The splits to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.将失败的源切分重新添加回分片枚举器
     */
    // 重新添加分片。
    // 当一个 SourceReader 失败或发生迁移时，它在上次成功检查点之后被分配但尚未完成的分片会被返回给枚举器。
    void addSplitsBack(List<SplitT> splits, int subtaskId);

    /**
     * Add a new source reader with the given subtask ID.
     *
     * @param subtaskId the subtask ID of the new source reader. 添加新的源读取器
     */
    // 添加新的读取器。
    // 当一个新的 SourceReader 实例启动或重新启动时，此方法被调用以通知枚举器。
    void addReader(int subtaskId);

    /**
     * Creates a snapshot of the state of this split enumerator, to be stored in a checkpoint.
     *
     * <p>The snapshot should contain the latest state of the enumerator: It should assume that all
     * operations that happened before the snapshot have successfully completed. For example all
     * splits assigned to readers via {@link SplitEnumeratorContext#assignSplit(SourceSplit, int)}
     * and {@link SplitEnumeratorContext#assignSplits(SplitsAssignment)}) don't need to be included
     * in the snapshot anymore.
     *
     * <p>This method takes the ID of the checkpoint for which the state is snapshotted. Most
     * implementations should be able to ignore this parameter, because for the contents of the
     * snapshot, it doesn't matter for which checkpoint it gets created. This parameter can be
     * interesting for source connectors with external systems where those systems are themselves
     * aware of checkpoints; for example in cases where the enumerator notifies that system about a
     * specific checkpoint being triggered.
     *
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return an object containing the state of the split enumerator.
     * @throws Exception when the snapshot cannot be taken.
     */
    // 创建状态快照。
    // 在 Flink 检查点触发时被调用。
    // 枚举器必须返回一个包含其最新状态的对象 (CheckpointT)。
    // 该状态应假设所有在快照触发前已完成的操作（如分片分配）均已成功，并仅包含需要恢复的关键信息（如尚未分配的分片）
    CheckpointT snapshotState(long checkpointId) throws Exception;

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    // 关闭枚举器。
    // 在 Flink Job 停止时调用，用于释放枚举器持有的所有资源（如线程、网络连接）。
    @Override
    void close() throws IOException;

    /**
     * We have an empty default implementation here because most source readers do not have to
     * implement the method.
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    // 通知检查点完成。
    // 默认实现为空。
    // 如果枚举器需要通知外部系统某个 Flink 检查点已经成功完成（例如，用于清理外部资源），则可以重写此方法。
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * Handles a custom source event from the source reader.
     *
     * <p>This method has a default implementation that does nothing, because it is only required to
     * be implemented by some sources, which have a custom event protocol between reader and
     * enumerator. The common events for reader registration and split requests are not dispatched
     * to this method, but rather invoke the {@link #addReader(int)} and {@link
     * #handleSplitRequest(int, String)} methods.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param sourceEvent the source event from the source reader.
     */
    // 处理自定义 Source 事件。
    // 这是一个钩子（Hook），允许 SourceReader 和 SplitEnumerator 之间发送自定义通信事件。
    // 默认实现为空。
    // subtaskId 标识发送事件的读取器。
    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}
}
