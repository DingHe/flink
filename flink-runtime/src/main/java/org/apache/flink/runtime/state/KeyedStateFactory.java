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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

/** This factory produces concrete internal state objects. */
// KeyedStateFactory 接口的核心作用是定义了如何创建或更新具体的内部键控状态对象。
// 状态实例化： 它是 KeyedStateBackend（键控状态后端，如 Heap 或 RocksDB）用于实例化具体状态类型（如 InternalValueState、InternalListState 等）的工厂
// 是 Flink 状态后端中负责将用户定义的状态描述符（Descriptor）转化为具体、可操作的内部状态对象的核心组件。
public interface KeyedStateFactory {

    /**
     * Creates or updates internal state and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    // **<N>**	Namespace Type	命名空间（Namespace）的数据类型。用于窗口操作等需要命名空间隔离的场景。
    // **<SV>**	Stored Value Type	状态描述符中定义的存储值的类型。
    // **<S extends State>**	Public API State Type	公共 API 状态接口的类型（如 ValueState、ListState）。
    // **<IS extends S>**	Internal State Type	具体的内部状态实现类的类型（如 InternalValueState）。它必须继承自公共 API 状态类型 S。
    // **<SEV>**	Stored Entry/Value Type	状态存储的值或条目类型。
    // 对于 ValueState，它与 SV 相同；对于 ListState 或 MapState 等集合类型，它指代集合的单个元素类型。
    @Nonnull
    default <N, SV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, StateSnapshotTransformFactory.noTransform());
    }

    /**
     * Creates or updates internal state and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param snapshotTransformFactory factory of state snapshot transformer.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <SEV> The type of the stored state value or entry for collection types (list or map).
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception;

    /**
     * Creates or updates internal state and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param snapshotTransformFactory factory of state snapshot transformer.
     * @param allowFutureMetadataUpdates whether allow metadata to update in the future or not.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <SEV> The type of the stored state value or entry for collection types (list or map).
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    default <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        if (allowFutureMetadataUpdates) {
            throw new UnsupportedOperationException(
                    this.getClass().getName() + "doesn't support to allow future metadata update");
        } else {
            return createOrUpdateInternalState(
                    namespaceSerializer, stateDesc, snapshotTransformFactory);
        }
    }
}
