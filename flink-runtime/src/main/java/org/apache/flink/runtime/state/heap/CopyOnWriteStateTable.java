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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

/**
 * This implementation of {@link StateTable} uses {@link CopyOnWriteStateMap}. This implementation
 * supports asynchronous snapshots.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
// CopyOnWriteStateTable 是 Flink 堆内存状态后端（Heap State Backend）中用于管理键值状态的核心容器。
// 键组分片管理：它充当了一个顶级容器，将所有的键值状态根据 Key Group（键组）划分到一系列底层的 StateMap 实例中。Flink 的状态是按键组（Key Group）分片的，这是实现并行和重分布的基础。
// 启用异步快照：这个类继承自抽象类 StateTable，但它特意通过其 createStateMap() 方法指定使用 CopyOnWriteStateMap（或其相关实现如 CopyOnWriteSkipListStateMap）作为底层存储。
// 这意味着它为整个状态表提供了 写时复制 (Copy-On-Write, COW) 能力，从而支持 异步快照。
// CopyOnWriteStateTable 是一个支持异步快照的、按键组分片的键值状态管理器。
public class CopyOnWriteStateTable<K, N, S> extends StateTable<K, N, S> {

    /**
     * Constructs a new {@code CopyOnWriteStateTable}.
     *
     * @param keyContext the key context.
     * @param metaInfo the meta information, including the type serializer for state copy-on-write.
     * @param keySerializer the serializer of the key.
     */
    CopyOnWriteStateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
    }
    // 返回一个基于写时复制原理实现的 CopyOnWriteStateMap 实例。
    @Override
    protected CopyOnWriteStateMap<K, N, S> createStateMap() {
        return new CopyOnWriteStateMap<>(getStateSerializer());
    }

    @Override
    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        super.setMetaInfo(metaInfo);
        for (StateMap<K, N, S> keyGroupedStateMap : keyGroupedStateMaps) {
            ((CopyOnWriteStateMap<K, N, S>) keyGroupedStateMap)
                    .setStateSerializer(metaInfo.getStateSerializer());
        }
    }

    // Snapshotting
    // ----------------------------------------------------------------------------------------------------

    /**
     * Creates a snapshot of this {@link CopyOnWriteStateTable}, to be written in checkpointing.
     *
     * @return a snapshot from this {@link CopyOnWriteStateTable}, for checkpointing.
     */
    @Nonnull
    @Override
    public CopyOnWriteStateTableSnapshot<K, N, S> stateSnapshot() {
        return new CopyOnWriteStateTableSnapshot<>(
                this,
                getKeySerializer().duplicate(),
                getNamespaceSerializer().duplicate(),
                getStateSerializer().duplicate(),
                getMetaInfo()
                        .getStateSnapshotTransformFactory()
                        .createForDeserializedState()
                        .orElse(null));
    }

    @SuppressWarnings("unchecked")
    List<CopyOnWriteStateMapSnapshot<K, N, S>> getStateMapSnapshotList() {
        List<CopyOnWriteStateMapSnapshot<K, N, S>> snapshotList =
                new ArrayList<>(keyGroupedStateMaps.length);
        for (int i = 0; i < keyGroupedStateMaps.length; i++) {
            CopyOnWriteStateMap<K, N, S> stateMap =
                    (CopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[i];
            snapshotList.add(stateMap.stateSnapshot());
        }
        return snapshotList;
    }
}
