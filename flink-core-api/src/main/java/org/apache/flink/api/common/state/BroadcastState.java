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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Iterator;
import java.util.Map;

/**
 * A type of state that can be created to store the state of a {@code BroadcastStream}. This state
 * assumes that <b>the same elements are sent to all instances of an operator.</b>
 *
 * <p><b>CAUTION:</b> the user has to guarantee that all task instances store the same elements in
 * this type of state.
 *
 * <p>Each operator instance individually maintains and stores elements in the broadcast state. The
 * fact that the incoming stream is a broadcast one guarantees that all instances see all the
 * elements. Upon recovery or re-scaling, the same state is given to each of the instances. To avoid
 * hotspots, each task reads its previous partition, and if there are more tasks (scale up), then
 * the new instances read from the old instances in a round robin fashion. This is why each instance
 * has to guarantee that it stores the same elements as the rest. If not, upon recovery or rescaling
 * you may have unpredictable redistribution of the partitions, thus unpredictable results.
 *
 * @param <K> The key type of the elements in the {@link BroadcastState}.
 * @param <V> The value type of the elements in the {@link BroadcastState}.
 */
// 定义了 Flink 中广播流（Broadcast Stream）算子用来存储和操作其状态的契约。它本质上是一个可以存储键值对的分布式 Map 状态。
// 全局共享状态： BroadcastState 存储的状态数据会被复制到连接它的所有下游算子（非广播侧）的所有并行实例中，实现全局共享。
// 它继承了 ReadOnlyBroadcastState 的所有读取能力，并在此基础上增加了修改（写入、更新、删除）状态的方法。
@PublicEvolving
public interface BroadcastState<K, V> extends ReadOnlyBroadcastState<K, V> {

    /**
     * Associates a new value with the given key.
     *
     * @param key The key of the mapping
     * @param value The new value of the mapping
     * @throws Exception Thrown if the system cannot access the state.
     */
    void put(K key, V value) throws Exception;

    /**
     * Copies all of the mappings from the given map into the state.
     *
     * @param map The mappings to be stored in this state
     * @throws Exception Thrown if the system cannot access the state.
     */
    void putAll(Map<K, V> map) throws Exception;

    /**
     * Deletes the mapping of the given key.
     *
     * @param key The key of the mapping
     * @throws Exception Thrown if the system cannot access the state.
     */
    void remove(K key) throws Exception;

    /**
     * Iterates over all the mappings in the state.
     *
     * @return An iterator over all the mappings in the state
     * @throws Exception Thrown if the system cannot access the state.
     */
    Iterator<Map.Entry<K, V>> iterator() throws Exception;

    /**
     * Returns all the mappings in the state.
     *
     * @return An iterable view of all the key-value pairs in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    Iterable<Map.Entry<K, V>> entries() throws Exception;
}
