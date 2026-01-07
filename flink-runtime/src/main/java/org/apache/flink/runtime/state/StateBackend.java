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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * A <b>State Backend</b> defines how the state of a streaming application is stored locally within
 * the cluster. Different State Backends store their state in different fashions, and use different
 * data structures to hold the state of a running application.
 *
 * <p>For example, the {@link org.apache.flink.runtime.state.hashmap.HashMapStateBackend hashmap
 * state backend} keeps working state in the memory of the TaskManager. The backend is lightweight
 * and without additional dependencies.
 *
 * <p>The {@code EmbeddedRocksDBStateBackend} stores working state in an embedded <a
 * href="http://rocksdb.org/">RocksDB</a> and is able to scale working state to many terabytes in
 * size, only limited by available disk space across all task managers.
 *
 * <h2>Raw Bytes Storage and Backends</h2>
 *
 * <p>The {@code StateBackend} creates services for <i>keyed state</i> and <i>operator state</i>.
 *
 * <p>The {@link CheckpointableKeyedStateBackend} and {@link OperatorStateBackend} created by this
 * state backend define how to hold the working state for keys and operators. They also define how
 * to checkpoint that state, frequently using the raw bytes storage (via the {@code
 * CheckpointStreamFactory}). However, it is also possible that for example a keyed state backend
 * simply implements the bridge to a key/value store, and that it does not need to store anything in
 * the raw byte storage upon a checkpoint.
 *
 * <h2>Serializability</h2>
 *
 * <p>State Backends need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application
 * code.
 *
 * <p>Because of that, {@code StateBackend} implementations (typically subclasses of {@link
 * AbstractStateBackend}) are meant to be like <i>factories</i> that create the proper states stores
 * that provide access to the persistent storage and hold the keyed- and operator state data
 * structures. That way, the State Backend can be very lightweight (contain only configurations)
 * which makes it easier to be serializable.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>State backend implementations have to be thread-safe. Multiple threads may be creating
 * keyed-/operator state backends concurrently.
 */
// StateBackend 接口定义了 Flink 流处理应用程序中状态（State）如何被存储在 TaskManager 内部以及如何被检查点（Checkpoint）到远程持久存储的机制。
// 状态存储定义： 它决定了 Flink 运行中的工作状态（Working State）使用哪种数据结构，以及存储在哪里。常见的实现有：
// HashMapStateBackend：将工作状态存储在 TaskManager 的 JVM 堆内存中。
// EmbeddedRocksDBStateBackend：将工作状态存储在 TaskManager 节点的本地磁盘上的 RocksDB 实例中。
// 状态后端工厂： StateBackend 实例本身是一个工厂（Factory）。它不直接存储状态，而是用于在 Task 启动时，创建实际负责存储和检查点操作的后端实例：
// 可序列化： StateBackend 必须是可序列化的 (java.io.Serializable)，因为它需要作为作业的一部分从 JobManager 分发到所有 TaskManager 实例上。

@PublicEvolving
public interface StateBackend extends java.io.Serializable {

    /**
     * Return the name of this backend, default is simple class name. {@link
     * org.apache.flink.runtime.state.delegate.DelegatingStateBackend} may return the simple class
     * name of the delegated backend.
     */
    // 获取后端名称。
    // 返回该状态后端的简单名称，用于日志记录和监控。默认实现返回类的简单名称。
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Creates a new {@link CheckpointableKeyedStateBackend} that is responsible for holding
     * <b>keyed state</b> and checkpointing it.
     *
     * <p><i>Keyed State</i> is state where each value is bound to a key.
     *
     * @param parameters The arguments bundle for creating {@link CheckpointableKeyedStateBackend}.
     * @param <K> The type of the keys by which the state is organized.
     * @return The Keyed State Backend for the given job, operator, and key group range.
     * @throws Exception This method may forward all exceptions that occur while instantiating the
     *     backend.
     */
    // 创建 Keyed State 后端。
    // 这是核心方法之一。
    // 用于创建并返回一个 CheckpointableKeyedStateBackend 实例，该实例负责管理键控状态（Keyed State，即每个 Key 独立维护的状态），并负责该状态的检查点操作。
    // 它接收包含恢复信息和配置的参数包。
    <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception;

    /**
     * Creates a new {@link AsyncKeyedStateBackend} which supports to access <b>keyed state</b>
     * asynchronously.
     *
     * <p><i>Keyed State</i> is state where each value is bound to a key.
     *
     * @param parameters The arguments bundle for creating {@link AsyncKeyedStateBackend}.
     * @param <K> The type of the keys by which the state is organized.
     * @return The Async Keyed State Backend for the given job, operator.
     * @throws Exception This method may forward all exceptions that occur while instantiating the
     *     backend.
     */
    // 创建异步 Keyed State 后端。
    // 用于创建支持异步访问键控状态的后端。默认实现抛出 UnsupportedOperationException，意味着大多数后端默认不支持此功能。
    @Experimental
    default <K> AsyncKeyedStateBackend createAsyncKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception {
        throw new UnsupportedOperationException(
                "Don't support createAsyncKeyedStateBackend by default");
    }

    /**
     * Tells if a state backend supports the {@link AsyncKeyedStateBackend}.
     *
     * <p>If a state backend supports {@code AsyncKeyedStateBackend}, it could use {@link
     * #createAsyncKeyedStateBackend(KeyedStateBackendParameters)} to create an async keyed state
     * backend to access <b>keyed state</b> asynchronously.
     *
     * @return If the state backend supports {@link AsyncKeyedStateBackend}.
     */
    // 检查是否支持异步 Keyed State。
    // 标识该状态后端是否提供了 AsyncKeyedStateBackend 的实现。
    @Experimental
    default boolean supportsAsyncKeyedStateBackend() {
        return false;
    }

    /**
     * Creates a new {@link OperatorStateBackend} that can be used for storing operator state.
     *
     * <p>Operator state is state that is associated with parallel operator (or function) instances,
     * rather than with keys.
     *
     * @param parameters The arguments bundle for creating {@link OperatorStateBackend}.
     * @return The OperatorStateBackend for operator identified by the job and operator identifier.
     * @throws Exception This method may forward all exceptions that occur while instantiating the
     *     backend.
     */
    // 创建 Operator State 后端。
    // 另一个核心方法。用于创建并返回一个 OperatorStateBackend 实例，该实例负责管理操作符状态（Operator State，即与操作符的并行实例关联的状态，如 Source 中的偏移量）。它接收包含配置和恢复句柄的参数包。
    OperatorStateBackend createOperatorStateBackend(OperatorStateBackendParameters parameters)
            throws Exception;

    /** Whether the state backend uses Flink's managed memory. */
    // 检查是否使用托管内存。
    // 返回该状态后端是否使用 Flink 的托管内存（Managed Memory，即 Flink 自行管理和分配的堆外或堆上内存）。例如，RocksDBStateBackend 通常会使用托管内存。
    default boolean useManagedMemory() {
        return false;
    }

    /**
     * Tells if a state backend supports the {@link RecoveryClaimMode#NO_CLAIM} mode.
     *
     * <p>If a state backend supports {@code NO_CLAIM} mode, it should create an independent
     * snapshot when it receives {@link CheckpointType#FULL_CHECKPOINT} in {@link
     * Snapshotable#snapshot(long, long, CheckpointStreamFactory, CheckpointOptions)}.
     *
     * @return If the state backend supports {@link RecoveryClaimMode#NO_CLAIM} mode.
     */
    // 检查是否支持 NO_CLAIM 恢复模式。
    // 如果返回 true，表示该后端在执行全量检查点 (FULL_CHECKPOINT) 时，会创建独立的快照，允许不声明所有权地恢复，支持更灵活的故障恢复和 Savepoint 流程。
    default boolean supportsNoClaimRestoreMode() {
        return false;
    }
    // 检查是否支持指定的 Savepoint 格式。
    // 默认只支持 CANONICAL（标准）格式。用于在进行 Savepoint 时，确认状态后端能够正确地序列化和恢复该格式的数据。
    default boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return formatType == SavepointFormatType.CANONICAL;
    }

    /**
     * Parameters passed to {@link
     * StateBackend#createKeyedStateBackend(KeyedStateBackendParameters)}.
     *
     * @param <K> The type of the keys by which the state is organized.
     */
    @PublicEvolving
    interface KeyedStateBackendParameters<K> {
        /** @return The runtime environment of the executing task. */
        Environment getEnv();

        JobID getJobID();

        String getOperatorIdentifier();

        TypeSerializer<K> getKeySerializer();

        int getNumberOfKeyGroups();

        /** @return Range of key-groups for which the to-be-created backend is responsible. */
        KeyGroupRange getKeyGroupRange();

        TaskKvStateRegistry getKvStateRegistry();

        /** @return Provider for TTL logic to judge about state expiration. */
        TtlTimeProvider getTtlTimeProvider();

        MetricGroup getMetricGroup();

        @Nonnull
        Collection<KeyedStateHandle> getStateHandles();

        /**
         * @return The registry to which created closeable objects will be * registered during
         *     restore.
         */
        CloseableRegistry getCancelStreamRegistry();

        double getManagedMemoryFraction();

        CustomInitializationMetrics getCustomInitializationMetrics();
    }

    /**
     * Parameters passed to {@link
     * StateBackend#createOperatorStateBackend(OperatorStateBackendParameters)}.
     */
    @PublicEvolving
    interface OperatorStateBackendParameters {
        /** @return The runtime environment of the executing task. */
        Environment getEnv();

        String getOperatorIdentifier();

        @Nonnull
        Collection<OperatorStateHandle> getStateHandles();

        /**
         * @return The registry to which created closeable objects will be * registered during
         *     restore.
         */
        CloseableRegistry getCancelStreamRegistry();

        CustomInitializationMetrics getCustomInitializationMetrics();
    }

    @Experimental
    interface CustomInitializationMetrics {
        void addMetric(String name, long value);
    }
}
