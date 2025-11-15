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

import javax.annotation.Nonnull;

/** Interface to deal with state snapshot and restore of state. TODO find better name? */
// StateSnapshotRestore 接口的作用是定义一个契约，
// 任何实现了它的组件（通常是 Flink 状态后端中的具体状态实例，如 InternalKvState）都必须能够执行状态快照并将状态从快照中恢复。
// 统一快照/恢复入口： 它将状态的快照创建能力 (stateSnapshot()) 和基于 Key Group 的恢复读取能力 (keyGroupReader()) 集中在一个接口中。
// 生命周期抽象： 它代表了单个状态对象（如一个 ValueState）在整个 Flink 容错机制中的完整生命周期能力。
@Internal
public interface StateSnapshotRestore {

    /** Returns a snapshot of the state. */
    // 返回状态快照。
    // 核心作用
    // 在 Checkpoint 的同步阶段被调用，用于创建当前状态的瞬时快照。
    @Nonnull
    StateSnapshot stateSnapshot();

    /**
     * This method returns a {@link StateSnapshotKeyGroupReader} that can be used to restore the
     * state on a per-key-group basis. This method tries to return a reader for the given version
     * hint.
     *
     * @param readVersionHint the required version of the state to read.
     * @return a reader that reads state by key-groups, for the given read version.
     */
    // 返回 Key Group 读取器
    // 核心作用
    // 在状态恢复时被调用，用于获取一个专门的读取器，该读取器能够将 Checkpoint/Savepoint 中的状态数据按 Key Group 进行反序列化和加载。
    @Nonnull
    StateSnapshotKeyGroupReader keyGroupReader(int readVersionHint);
}
