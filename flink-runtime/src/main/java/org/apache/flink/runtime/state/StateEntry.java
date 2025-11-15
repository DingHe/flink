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

/**
 * Interface of entries in a state backend. Entries are triple of key, namespace, and state.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */

// Flink 状态后端（State Backend） 在底层存储和迭代状态数据时，用于抽象单个状态条目的关键结构。
// StateEntry 接口的作用是定义 Flink 键控状态（Keyed State） 的单个逻辑条目。
// 在 Flink 的状态后端中，一个完整的状态数据点总是由三元组 (Key, Namespace, State Value) 唯一标识。
// 统一抽象： 它将 Keyed State 的基本组成部分（Key、Namespace、状态值）抽象成一个单一的接口，方便状态后端在内部进行读写、迭代和快照操作。
// 迭代器基础： Flink 在进行全量快照（Full Snapshot）时，会通过迭代器遍历状态后端中的所有状态条目。StateEntry 就是这些迭代器返回的基本单元。
//
public interface StateEntry<K, N, S> {

    /** Returns the key of this entry. */
    K getKey();

    /** Returns the namespace of this entry. */
    N getNamespace();

    /** Returns the state of this entry. */
    S getState();
    // 过滤或转换状态条目。

    default StateEntry<K, N, S> filterOrTransform(StateSnapshotTransformer<S> transformer) {
        S newState = transformer.filterOrTransform(getState());
        if (newState != null) {
            return new SimpleStateEntry<>(getKey(), getNamespace(), newState);
        } else {
            return null;
        }
    }

    class SimpleStateEntry<K, N, S> implements StateEntry<K, N, S> {
        private final K key;
        private final N namespace;
        private final S value;

        public SimpleStateEntry(K key, N namespace, S value) {
            this.key = key;
            this.namespace = namespace;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public N getNamespace() {
            return namespace;
        }

        @Override
        public S getState() {
            return value;
        }
    }
}
