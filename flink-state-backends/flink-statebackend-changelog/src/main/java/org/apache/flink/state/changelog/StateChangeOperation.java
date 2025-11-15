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

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The operation applied to {@link ChangelogState}. */
// StateChangeOperation 的核心作用是为 状态变更日志 提供一个统一的、可序列化的标识符。
// 在 Flink 的状态变更日志模式下，每当 Keyed State 发生变化时，该变化会被序列化成一条日志记录。这条记录的首部通常会包含一个 StateChangeOperation 的字节码，用于标识这次变更的类型（例如是清除状态、设置新值还是添加元素）。


@Internal
public enum StateChangeOperation {
    /** Scope: key + namespace. */
    // 清除状态。清除当前 Key 和 Namespace 下的所有状态数据
    CLEAR((byte) 0),
    /** Scope: key + namespace. */
    // 设置值。用于 Value State，用新值完全替换当前 Key 和 Namespace 下的现有状态值。
    SET((byte) 1),
    /** Scope: key + namespace. */
    // 设置内部值。与 SET 类似，但可能用于设置状态后端内部使用的值（例如，在 LazyState 等延迟更新状态中的使用）。
    SET_INTERNAL((byte) 2),
    /** Scope: key + namespace. */
    // 添加/归约。用于 ReducingState 或 AggregatingState，将新元素与现有累积值进行合并或归约。
    ADD((byte) 3),
    /** Scope: key + namespace, also affecting other (source) namespaces. */
    // 合并命名空间。用于窗口合并（如 ReducingState），将多个源 Namespace 的状态合并到目标 Namespace 中。
    MERGE_NS((byte) 4),
    /** Scope: key + namespace + element (e.g. user list append). */
    // 添加元素。用于 ListState，将一个元素追加到列表中。
    ADD_ELEMENT((byte) 5),
    /** Scope: key + namespace + element (e.g. user map key put). */
    // 添加或更新元素。用于 MapState，添加或更新 Map 中的一个 Key-Value 对。
    ADD_OR_UPDATE_ELEMENT((byte) 6),
    /** Scope: key + namespace + element (e.g. user map remove or iterator remove). */
    // 移除元素。用于 MapState（移除一个键）或 ListState（迭代器移除元素）
    REMOVE_ELEMENT((byte) 7),
    /** State metadata (name, serializers, etc.). */
    // 元数据。用于记录状态的元数据信息（例如名称、序列化器等），而不是实际的数据变更。
    METADATA((byte) 8);
    // 操作码。用于在状态变更日志中紧凑地表示该操作的类型。
    private final byte code;

    StateChangeOperation(byte code) {
        this.code = code;
    }

    private static final Map<Byte, StateChangeOperation> BY_CODES =
            Arrays.stream(StateChangeOperation.values())
                    .collect(Collectors.toMap(o -> o.code, Function.identity()));

    public static StateChangeOperation byCode(byte opCode) {
        return checkNotNull(BY_CODES.get(opCode), Byte.toString(opCode));
    }

    public byte getCode() {
        return code;
    }
}
