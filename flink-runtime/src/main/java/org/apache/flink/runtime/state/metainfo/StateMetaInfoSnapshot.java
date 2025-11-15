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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Generalized snapshot for meta information about one state in a state backend (e.g. {@link
 * RegisteredKeyValueStateBackendMetaInfo}).
 */
// 主要作用是封装 Flink 单个状态（State） 在进行 Checkpoint 或 Savepoint 时所需的所有元数据信息的快照。
// 容错元数据持久化： 在创建 Checkpoint 时，不仅仅需要保存状态数据本身，还需要保存如何读取这些数据的信息。这个类就是用来保存这些“如何读取”的信息。
// 类型版本管理： 它记录了状态所使用的序列化器（TypeSerializer） 的快照 (TypeSerializerSnapshot)。这对于 Flink 在作业重启或升级时，正确地处理类型模式演进（Schema Evolution） 至关重要。

public class StateMetaInfoSnapshot {

    /** Enum that defines the different types of state that live in Flink backends. */
    // 后端状态类型
    public enum BackendStateType {
        KEY_VALUE(0), // 键值状态。 指的是 Keyed State，例如 ValueState, ListState 等。
        OPERATOR(1),  // 算子状态。 指的是 Operator State，例如 ListCheckpointed 实现的状态。
        BROADCAST(2), // 广播状态。 指的是 BroadcastState。
        PRIORITY_QUEUE(3); // 优先级队列状态。 通常用于 Flink 内部的时间服务（Timer Service）
        private final byte code;

        BackendStateType(int code) {
            this.code = (byte) code;
        }

        public byte getCode() {
            return code;
        }

        public static BackendStateType byCode(int code) {
            for (BackendStateType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown BackendStateType: " + code);
        }
    }

    /** Predefined keys for the most common options in the meta info. */
    // 常用选项键
    public enum CommonOptionsKeys {
        /** Key to define the {@link StateDescriptor.Type} of a key/value keyed-state */
        KEYED_STATE_TYPE, // 用于定义 Keyed State 的具体类型（例如，是 ValueState 还是 ListState）。
        /**
         * Key to define {@link org.apache.flink.runtime.state.OperatorStateHandle.Mode}, about how
         * operator state is distributed on restore
         */
        OPERATOR_STATE_DISTRIBUTION_MODE, // 用于定义 Operator State 在恢复时如何被分配到不同的并行实例（例如，EVENLY 或 UNION）
    }

    /** Predefined keys for the most common serializer types in the meta info. */
    // 常用序列化器键
    public enum CommonSerializerKeys {
        KEY_SERIALIZER, // 在 Keyed State 中，用于 Key 的序列化器。
        NAMESPACE_SERIALIZER, // 在 Keyed State 中，用于命名空间（Namespace）的序列化器（通常用于窗口操作）。
        VALUE_SERIALIZER  // 用于状态值（Value）的序列化器。
    }

    /** The name of the state. */
    // 状态名称。 例如，在 StateDescriptor 中定义的名称
    @Nonnull private final String name;

    // 后端状态类型。
    // 标识该快照描述的是哪种类型（如 KEY_VALUE）。
    @Nonnull private final BackendStateType backendStateType;

    /** Map of options (encoded as strings) for the state. */
    // 状态选项 Map。
    // 以字符串形式存储状态的各种配置选项（键值对），包括通过 CommonOptionsKeys 定义的那些选项。
    @Nonnull private final Map<String, String> options;

    /** The configurations of all the type serializers used with the state. */
    // 序列化器快照 Map（核心）。
    // 这是最重要的属性。它存储了所有用于状态的序列化器（Key, Value, Namespace）的快照。
    @Nonnull private final Map<String, TypeSerializerSnapshot<?>> serializerSnapshots;

    // TODO this will go away once all serializers have the restoreSerializer() factory method
    // properly implemented.
    /** The serializers used by the state. */
    // 序列化器 Map（已弃用/临时）。
    // 存储实际的 TypeSerializer 实例。
    @Nonnull private final Map<String, TypeSerializer<?>> serializers;

    public StateMetaInfoSnapshot(
            @Nonnull String name,
            @Nonnull BackendStateType backendStateType,
            @Nonnull Map<String, String> options,
            @Nonnull Map<String, TypeSerializerSnapshot<?>> serializerSnapshots) {
        this(name, backendStateType, options, serializerSnapshots, new HashMap<>());
    }

    /**
     * TODO this variant, which requires providing the serializers, TODO should actually be removed,
     * leaving only {@link #StateMetaInfoSnapshot(String, BackendStateType, Map, Map)}. TODO This is
     * still used by snapshot extracting methods (i.e. computeSnapshot() method of specific state
     * meta TODO info subclasses), and will be removed once all serializers have the
     * restoreSerializer() factory method implemented.
     */
    public StateMetaInfoSnapshot(
            @Nonnull String name,
            @Nonnull BackendStateType backendStateType,
            @Nonnull Map<String, String> options,
            @Nonnull Map<String, TypeSerializerSnapshot<?>> serializerSnapshots,
            @Nonnull Map<String, TypeSerializer<?>> serializers) {
        this.name = name;
        this.backendStateType = backendStateType;
        this.options = options;
        this.serializerSnapshots = serializerSnapshots;
        this.serializers = serializers;
    }

    @Nonnull
    public BackendStateType getBackendStateType() {
        return backendStateType;
    }

    @Nullable
    public TypeSerializerSnapshot<?> getTypeSerializerSnapshot(@Nonnull String key) {
        return serializerSnapshots.get(key);
    }

    @Nullable
    public TypeSerializerSnapshot<?> getTypeSerializerSnapshot(@Nonnull CommonSerializerKeys key) {
        return getTypeSerializerSnapshot(key.toString());
    }

    @Nullable
    public String getOption(@Nonnull String key) {
        return options.get(key);
    }

    @Nullable
    public String getOption(@Nonnull StateMetaInfoSnapshot.CommonOptionsKeys key) {
        return getOption(key.toString());
    }

    @Nonnull
    public Map<String, String> getOptionsImmutable() {
        return Collections.unmodifiableMap(options);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public Map<String, TypeSerializerSnapshot<?>> getSerializerSnapshotsImmutable() {
        return Collections.unmodifiableMap(serializerSnapshots);
    }

    /** TODO this method should be removed once the serializer map is removed. */
    @Nullable
    public TypeSerializer<?> getTypeSerializer(@Nonnull String key) {
        return serializers.get(key);
    }
}
