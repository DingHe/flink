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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * General interface for state snapshots that should be written partitioned by key-groups. All
 * snapshots should be released after usage. This interface outlines the asynchronous snapshot
 * life-cycle, which typically looks as follows. In the synchronous part of a checkpoint, an
 * instance of {@link StateSnapshot} is produced for a state and captures the state at this point in
 * time. Then, in the asynchronous part of the checkpoint, the user calls {@link
 * #getKeyGroupWriter()} to ensure that the snapshot is partitioned into key-groups. For state that
 * is already partitioned, this can be a NOP. The returned {@link StateKeyGroupWriter} can be used
 * by the caller to write the state by key-group. As a last step, when the state is completely
 * written, the user calls {@link #release()}.
 */
// StateSnapshot 接口定义了 Flink 中任何可被快照的状态对象（如 KeyedState 或 OperatorState 的内部实现）
// 在 Checkpoint 或 Savepoint 时的生命周期和操作契约。
// 同步阶段： 在 Checkpoint Barrier 到达时，状态后端会同步地生成一个 StateSnapshot 实例，捕获当前状态数据。
// 异步阶段： 异步写入线程通过调用 getKeyGroupWriter() 获取写入器，然后分 Key Group 写入数据。
// 分 Key Group 写入： 它的核心目的是支持将状态按照 Key Group 进行分区写入，这是 Flink 分布式快照和恢复的基石。
@Internal
public interface StateSnapshot {

    /**
     * This method returns {@link StateKeyGroupWriter} and should be called in the asynchronous part
     * of the snapshot.
     */
    // 获取 Key Group 写入器。
    // 这是异步快照阶段调用的主要方法。
    // 它返回一个 StateKeyGroupWriter 实例，该实例知道如何将快照数据按 Key Group 划分并写入输出流。
    @Nonnull
    StateKeyGroupWriter getKeyGroupWriter();

    /** Returns a snapshot of the state's meta data. */
    @Nonnull
    StateMetaInfoSnapshot getMetaInfoSnapshot();

    /**
     * Release the snapshot. All snapshots should be released when they are no longer used because
     * some implementation can only release resources after a release. Produced {@link
     * StateKeyGroupWriter} should no longer be used after calling this method.
     */
    void release();

    /** Interface for writing a snapshot that is partitioned into key-groups. */
    interface StateKeyGroupWriter {
        /**
         * Writes the data for the specified key-group to the output. You must call {@link
         * #getKeyGroupWriter()} once before first calling this method.
         *
         * @param dov the output.
         * @param keyGroupId the key-group to write.
         * @throws IOException on write-related problems.
         */
        // 写入指定 Key Group 的状态数据。
        // 这是异步写入线程实际执行数据写入的操作。它将属于指定 keyGroupId 的所有状态数据写入到提供的 DataOutputView (dov) 中。
        void writeStateInKeyGroup(@Nonnull DataOutputView dov, @Nonnegative int keyGroupId)
                throws IOException;
    }
}
