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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * An interface with methods related to the interplay between the {@link BroadcastState Broadcast
 * State} and the {@link OperatorStateBackend}.
 *
 * @param <K> The key type of the elements in the {@link BroadcastState Broadcast State}.
 * @param <V> The value type of the elements in the {@link BroadcastState Broadcast State}.
 */
// 定义一个可供状态后端写入的广播状态的契约。
// 它充当了用户可见的 BroadcastState 与 底层状态后端 (OperatorStateBackend) 之间进行** Checkpoint 和状态恢复**时的桥梁。
// Checkpointing 桥梁： 它为 OperatorStateBackend 提供了执行 Checkpoint 时所需的底层操作，例如将状态数据写入文件系统（FSDataOutputStream） 和执行深度复制。
// 元信息管理： 它允许 Flink 运行时设置和获取关于该状态的后端元数据，如序列化器、分区信息等。
public interface BackendWritableBroadcastState<K, V> extends BroadcastState<K, V> {

    BackendWritableBroadcastState<K, V> deepCopy();

    long write(FSDataOutputStream out) throws IOException;

    void setStateMetaInfo(RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo);

    RegisteredBroadcastStateBackendMetaInfo<K, V> getStateMetaInfo();
}
