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

import org.apache.flink.annotation.Public;

import java.io.IOException;
import java.util.Collection;

/** A {@link SinkWriter} that performs the first part of a two-phase commit protocol. */
// CommittingSinkWriter 接口是 Flink SinkV2 API 的一个扩展，它定义了支持 两阶段提交（Two-Phase Commit, 2PC） 协议的 SinkWriter
// 实现 2PC 的第一阶段： 此接口的目的是让 SinkWriter 在 Flink 的检查点（Checkpoint）过程中执行 2PC 协议的第一阶段：预提交（Pre-Commit）
// 生成 Committable： 它负责在检查点完成之前，将所有已写入但尚未最终确认（提交）的数据状态打包成一个或多个 Committable 对象 (CommittableT)
// 精确一次语义（Exactly-Once）： 通过与 Flink 的 Committer 组件配合，CommittingSinkWriter 是实现 Sink 端精确一次语义的关键，确保只有在检查点成功后，数据才会被永久写入外部系统。
// CommittableT	提交类型	两阶段提交第二阶段所需数据的类型。 这是一个用于封装提交所需元数据的自定义类型，例如文件路径、分区信息或事务 ID。
@Public
public interface CommittingSinkWriter<InputT, CommittableT> extends SinkWriter<InputT> {
    /**
     * Prepares for a commit.
     *
     * <p>This method will be called after {@link #flush(boolean)} and before {@link
     * StatefulSinkWriter#snapshotState(long)}.
     *
     * @return The data to commit as the second step of the two-phase commit protocol.
     * @throws IOException if fail to prepare for a commit.
     */
    // 准备提交。
    // 在检查点过程中，此方法会在 flush(boolean) 之后、状态快照 (snapshotState) 之前被调用。
    Collection<CommittableT> prepareCommit() throws IOException, InterruptedException;
}
