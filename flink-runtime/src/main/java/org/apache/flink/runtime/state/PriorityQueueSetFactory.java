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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

import javax.annotation.Nonnull;
// 核心作用是定义了如何创建 Flink 内部用于时间服务（Timer Service） 等机制的 键控（Keyed）优先级队列 实例
// 创建内部优先级队列： 它是创建 KeyGroupedInternalPriorityQueue 的抽象接口。这种队列用于存储和管理定时器（Timers），确保它们按照优先级（通常是时间戳）和 Key Group 结构进行管理和触发。
// 状态后端集成： 不同的状态后端（如 Heap 或 RocksDB）会提供不同的实现。例如，基于堆的状态后端可能会实现一个内存中的优先队列，而 RocksDB 状态后端则可能实现一个基于 RocksDB 后端存储的队列。

/** Factory for {@link KeyGroupedInternalPriorityQueue} instances. */
public interface PriorityQueueSetFactory {

    /**
     * Creates a {@link KeyGroupedInternalPriorityQueue}.
     *
     * @param stateName unique name for associated with this queue.
     * @param byteOrderedElementSerializer a serializer that with a format that is lexicographically
     *     ordered in alignment with elementPriorityComparator.
     * @param <T> type of the stored elements.
     * @return the queue with the specified unique name.
     */
    // 创建 Keyed 内部优先级队列。
    // 返回一个 KeyGroupedInternalPriorityQueue<T> 实例，它是 Flink 内部用于存储和按 Key Group 分区的优先级队列
    // HeapPriorityQueueElement： 队列元素必须具备优先级队列的内部管理属性。
    // PriorityComparable<? super T>： 元素必须能与自身或其父类型进行优先级比较（决定其在队列中的位置）
    // Keyed<?>： 元素必须与一个 Key 相关联，这是 Keyed State 的基础。
    @Nonnull
    <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer);

    /**
     * Creates a {@link KeyGroupedInternalPriorityQueue}.
     *
     * @param stateName unique name for associated with this queue.
     * @param byteOrderedElementSerializer a serializer that with a format that is lexicographically
     *     ordered in alignment with elementPriorityComparator.
     * @param allowFutureMetadataUpdates whether allow metadata to update in the future or not.
     * @param <T> type of the stored elements.
     * @return the queue with the specified unique name.
     */
    default <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        if (allowFutureMetadataUpdates) {
            throw new UnsupportedOperationException(
                    this.getClass().getName()
                            + " doesn't support to allow to update future metadata.");
        } else {
            return create(stateName, byteOrderedElementSerializer);
        }
    }
}
