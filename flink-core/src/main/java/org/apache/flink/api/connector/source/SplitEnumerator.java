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
@Public
public interface SplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        extends AutoCloseable, CheckpointListener {

    /**
     * Start the split enumerator.
     *
     * <p>The default behavior does nothing.
     */
    void start();

    /**
     * Handles the request for a split. This method is called when the reader with the given subtask
     * id calls the {@link SourceReaderContext#sendSplitRequest()} method.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param requesterHostname Optional, the hostname where the requesting task is running. This
     *     can be used to make split assignments locality-aware.当一个源读取器通过 SourceReaderContext#sendSplitRequest() 请求分片时，这个方法会被调用。subtaskId 表示请求分片的读取器的子任务 ID，requesterHostname 是请求的源读取器所在的主机名（可选），可以根据主机名做本地化分配
     */
    void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);

    /**
     * Add splits back to the split enumerator. This will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The splits to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.将失败的源切分重新添加回分片枚举器
     */
    void addSplitsBack(List<SplitT> splits, int subtaskId);

    /**
     * Add a new source reader with the given subtask ID.
     *
     * @param subtaskId the subtask ID of the new source reader. 添加新的源读取器
     */
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
     * @throws Exception when the snapshot cannot be taken.  该方法是 Flink 的容错机制的一部分，当作业触发检查点时，它会调用该方法来持久化分片枚举器的状态，以便恢复操作能够从失败或重启中恢复
     */
    CheckpointT snapshotState(long checkpointId) throws Exception;

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    void close() throws IOException;

    /**
     * We have an empty default implementation here because most source readers do not have to
     * implement the method.
     * 这是 CheckpointListener 接口的一部分，默认实现为空。大多数分片枚举器不需要处理此方法，但如果分片枚举器依赖于外部系统或资源进行检查点的通知（例如外部存储或数据库），则可以覆盖此方法来实现自定义逻辑
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
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
    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}
}
