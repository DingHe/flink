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
 * {@link State} interface for partitioned key-value state. The key-value pair can be added, updated
 * and retrieved.
 *
 * <p>The state is accessed and modified by user functions, and checkpointed consistently by the
 * system as part of the distributed snapshots.
 *
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the key of
 * the current element. That way, the system can handle stream and state partitioning consistently
 * together.
 *
 * @param <UK> Type of the keys in the state.
 * @param <UV> Type of the values in the state.
 */
// MapState 是 Flink 中用于管理分区键值状态的接口。
// 它允许用户在流处理中，为一个外部键（Flink Keyed Stream 的 Key）关联一个内部的、可操作的 Map 结构。
// MapState<UK, UV> 的作用是提供一个与 Java 标准 Map<UK, UV> 类似的数据结构，但这个 Map 是作为 Flink 算子状态的一部分，具有以下核心特性：
// 分区键控状态： 状态的使用被限制在 KeyedStream 上。每个 Keyed Stream 的主键（即 Flink Key $K$）都拥有一个独立的 MapState 实例。用户在操作 MapState 时，系统会自动将操作作用于当前正在处理元素的主键 $K$ 对应的 Map 上。
// 持久化和容错： MapState 的所有修改都会被 Flink 系统自动纳入**分布式快照（Checkpoint）**中，从而保证状态的持久性和容错性。
// 细粒度存储： 允许用户存储细粒度的键值对（$UK, UV$）。例如，你可以使用 MapState 存储一个用户在过去 7 天的所有会话 ID 及其持续时间。
@PublicEvolving
public interface MapState<UK, UV> extends State {

    /**
     * Returns the current value associated with the given key.
     *
     * @param key The key of the mapping
     * @return The value of the mapping with the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 获取：返回与给定内部键 key 关联的内部值。
    // 如果内部键不存在，通常返回 null。
    UV get(UK key) throws Exception;

    /**
     * Associates a new value with the given key.
     *
     * @param key The key of the mapping
     * @param value The new value of the mapping
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 添加/更新：将给定的内部键 key 与内部值 value 关联。如果键已存在，则更新其值。
    void put(UK key, UV value) throws Exception;

    /**
     * Copies all of the mappings from the given map into the state.
     *
     * @param map The mappings to be stored in this state
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 批量添加/更新：将给定 Map 中的所有键值对批量复制到当前状态 Map 中。
    void putAll(Map<UK, UV> map) throws Exception;

    /**
     * Deletes the mapping of the given key.
     *
     * @param key The key of the mapping
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 删除：从状态 Map 中移除与给定内部键 key 关联的映射。
    void remove(UK key) throws Exception;

    /**
     * Returns whether there exists the given mapping.
     *
     * @param key The key of the mapping
     * @return True if there exists a mapping whose key equals to the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 检查键是否存在：返回状态 Map 中是否包含给定的内部键 key 的映射。
    boolean contains(UK key) throws Exception;

    /**
     * Returns all the mappings in the state.
     *
     * @return An iterable view of all the key-value pairs in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 获取所有条目：
    // 返回一个可迭代视图，包含状态 Map 中的所有键值对 (Map.Entry)。
    Iterable<Map.Entry<UK, UV>> entries() throws Exception;

    /**
     * Returns all the keys in the state.
     *
     * @return An iterable view of all the keys in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 获取所有键：返回一个可迭代视图，包含状态 Map 中的所有内部键。
    Iterable<UK> keys() throws Exception;

    /**
     * Returns all the values in the state.
     *
     * @return An iterable view of all the values in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 获取所有值：返回一个可迭代视图，包含状态 Map 中的所有内部值。
    Iterable<UV> values() throws Exception;

    /**
     * Iterates over all the mappings in the state.
     *
     * @return An iterator over all the mappings in the state
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 获取迭代器：返回一个标准的 Iterator，用于遍历状态 Map 中的所有键值对。
    Iterator<Map.Entry<UK, UV>> iterator() throws Exception;

    /**
     * Returns true if this state contains no key-value mappings, otherwise false.
     *
     * @return True if this state contains no key-value mappings, otherwise false.
     * @throws Exception Thrown if the system cannot access the state.
     */
    // 检查是否为空：判断当前状态 Map 是否不包含任何键值映射。
    boolean isEmpty() throws Exception;
}
