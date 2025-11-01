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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;

import java.util.List;

/**
 * {@link StateObject Handle} to a {@link
 * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition} state.
 */
// 专门用于封装和引用 Flink 任务的输出结果子分区（Result Subpartition）中，在检查点时被持久化的飞行中数据（In-flight Data）状态
// 在 Flink 的非对齐检查点（Unaligned Checkpointing）机制中：
// ResultSubpartition： 是 TaskManager 中用于向下游任务发送数据的输出缓冲区和逻辑通道。
// 状态保存： 当检查点屏障（Checkpoint Barrier）经过一个任务的输出端时，该任务的 ResultSubpartition 中所有尚未被下游任务接收的数据（即飞行中的数据）必须被保存起来。
@Internal
public class ResultSubpartitionStateHandle
        extends AbstractChannelStateHandle<ResultSubpartitionInfo> {

    private static final long serialVersionUID = 1L;

    public ResultSubpartitionStateHandle(
            int subtaskIndex,
            ResultSubpartitionInfo info,
            StreamStateHandle delegate,
            StateContentMetaInfo contentMetaInfo) {
        this(subtaskIndex, info, delegate, contentMetaInfo.getOffsets(), contentMetaInfo.getSize());
    }

    @VisibleForTesting
    public ResultSubpartitionStateHandle(
            ResultSubpartitionInfo info, StreamStateHandle delegate, List<Long> offset) {
        this(0, info, delegate, offset, delegate.getStateSize());
    }

    public ResultSubpartitionStateHandle(
            int subtaskIndex,
            ResultSubpartitionInfo info,
            StreamStateHandle delegate,
            List<Long> offset,
            long size) {
        super(delegate, offset, subtaskIndex, info, size);
    }
}
