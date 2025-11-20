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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.util.Collection;

/**
 * A {@link Sink} for exactly-once semantics using a two-phase commit protocol. The {@link Sink}
 * consists of a {@link SinkWriter} that performs the precommits and a {@link Committer} that
 * actually commits the data. To facilitate the separation the {@link SinkWriter} creates
 * <i>committables</i> on checkpoint or end of input and the sends it to the {@link Committer}.
 *
 * <p>The {@link TwoPhaseCommittingSink} needs to be serializable. All configuration should be
 * validated eagerly. The respective sink writers and committers are transient and will only be
 * created in the subtasks on the taskmanagers.
 *
 * @param <InputT> The type of the sink's input
 * @param <CommT> The type of the committables.
 * @deprecated Please implement {@link Sink} {@link SupportsCommitter} instead.
 */
// TwoPhaseCommittingSink 接口是 Flink Sink API V2 早期设计中用于实现基于两阶段提交（2PC）的精确一次（Exactly-Once）语义的专用接口。
// 注意： 该接口目前已被标记为 @Deprecated。官方推荐的做法是：
//对于 Sink 的基本功能，实现 Sink<InputT> 接口。
//对于 2PC 提交能力，通过混入（Mixin） SupportsCommitter<CommT> 接口来实现。
// 强制 2PC 组合： 通过继承 Sink<InputT> 和 SupportsCommitter<CommT> 两个接口，确保任何实现此接口的类都必须同时提供基本的 Sink 功能和 Committer 创建能力，从而实现完整的 2PC 提交流。
// 定义预提交 Writer： 包含了一个嵌套的、已废弃的接口 PrecommittingSinkWriter，用于标识执行 2PC 第一阶段的写入器。
@PublicEvolving
@Deprecated
public interface TwoPhaseCommittingSink<InputT, CommT>
        extends Sink<InputT>, SupportsCommitter<CommT> {

    /**
     * Creates a {@link Committer} that permanently makes the previously written data visible
     * through {@link Committer#commit(Collection)}.
     *
     * @return A committer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     * @deprecated Please use {@link #createCommitter(CommitterInitContext)}
     */
    @Deprecated
    default Committer<CommT> createCommitter() throws IOException {
        throw new UnsupportedOperationException(
                "Deprecated, please use createCommitter(CommitterInitContext)");
    }

    /**
     * Creates a {@link Committer} that permanently makes the previously written data visible
     * through {@link Committer#commit(Collection)}.
     *
     * @param context The context information for the committer initialization.
     * @return A committer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     */
    default Committer<CommT> createCommitter(CommitterInitContext context) throws IOException {
        return createCommitter();
    }

    /** A {@link SinkWriter} that performs the first part of a two-phase commit protocol. */
    @PublicEvolving
    @Deprecated
    interface PrecommittingSinkWriter<InputT, CommT> extends CommittingSinkWriter<InputT, CommT> {}
}
