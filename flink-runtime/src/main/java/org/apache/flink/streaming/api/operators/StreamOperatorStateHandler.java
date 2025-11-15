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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SavepointSnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.v2.DefaultKeyedStateStoreV2;
import org.apache.flink.runtime.state.v2.KeyedStateStoreV2;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.guava32.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class encapsulating various state backend handling logic for {@link StreamOperator}
 * implementations.
 */
// Flink 中用于管理和操作流式算子（Stream Operator）状态的核心辅助类。它封装了所有与 状态后端（State Backend） 交互的逻辑
// 状态后端封装： 集中管理和抽象了两种主要状态后端类型：算子状态（Operator State） 和 键控状态（Keyed State） 的底层访问接口 (OperatorStateBackend 和 KeyedStateBackend)
// 状态生命周期管理： 负责算子状态的初始化（Initialization）、**快照（Snapshotting）和清理（Disposal）**的整个生命周期。
// 提供状态访问接口： 为上层算子和 RuntimeContext 提供清晰的接口，用于访问和创建不同类型的状态（V1 和 V2 API），以及设置当前的键（Key）。
// Checkpointing 协调： 在 Checkpoint/Savepoint 发生时，协调算子状态和键控状态的同步或异步快照过程，包括处理定时器状态的快照逻辑。
// Flink 算子中状态管理和持久化逻辑的中央调度器
@Internal
public class StreamOperatorStateHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(StreamOperatorStateHandler.class);
    // 异步键控状态后端。
    // 一个可选的（@Nullable）字段，用于支持 V2 状态 API，例如 RocksDBStateBackend 这种支持异步操作的状态后端实现。
    @Nullable private final AsyncKeyedStateBackend asyncKeyedStateBackend;
    // 键控状态存储（V2 API）。
    // 如果 asyncKeyedStateBackend 存在，则创建这个 V2 状态存储接口，供新的状态 API 使用。
    @Nullable private final KeyedStateStoreV2 keyedStateStoreV2;

    /** Backend for keyed state. This might be empty if we're not on a keyed stream. */
    // 键控状态后端。一个可选的（@Nullable）字段，用于管理和持久化键控数据。仅在算子作用于 Keyed Stream 时存在。
    @Nullable private final CheckpointableKeyedStateBackend<?> keyedStateBackend;
    // 可关闭资源注册中心。
    // 用于在算子关闭或清理时，安全地跟踪和关闭所有状态相关的资源（如输入流、状态后端实例）
    private final CloseableRegistry closeableRegistry;
    // 键控状态存储（V1 API）。
    // 如果 keyedStateBackend 存在，则创建这个 V1 状态存储接口，供旧的 Flink 状态 API（如 RuntimeContext.getState()）使用。
    @Nullable private final DefaultKeyedStateStore keyedStateStore;
    // 算子状态后端。负责管理和持久化算子状态（非键控、与并行度相关的状态）
    private final OperatorStateBackend operatorStateBackend;
    // 算子状态上下文。一个提供所有状态初始化信息的对象，包括恢复的 Checkpoint ID、原始键控和算子状态输入流，以及实际的后端实例。
    private final StreamOperatorStateContext context;

    public StreamOperatorStateHandler(
            StreamOperatorStateContext context,
            ExecutionConfig executionConfig,
            CloseableRegistry closeableRegistry) {
        this.context = context;
        operatorStateBackend = context.operatorStateBackend();
        keyedStateBackend = context.keyedStateBackend();
        this.closeableRegistry = closeableRegistry;

        if (keyedStateBackend != null) {
            keyedStateStore =
                    new DefaultKeyedStateStore(
                            keyedStateBackend,
                            new SerializerFactory() {
                                @Override
                                public <T> TypeSerializer<T> createSerializer(
                                        TypeInformation<T> typeInformation) {
                                    return typeInformation.createSerializer(
                                            executionConfig.getSerializerConfig());
                                }
                            });
        } else {
            keyedStateStore = null;
        }

        this.asyncKeyedStateBackend = context.asyncKeyedStateBackend();
        this.keyedStateStoreV2 =
                asyncKeyedStateBackend != null
                        ? new DefaultKeyedStateStoreV2(asyncKeyedStateBackend)
                        : null;
    }
    // 初始化算子状态。这是恢复状态的关键步骤。
    public void initializeOperatorState(CheckpointedStreamOperator streamOperator)
            throws Exception {
        CloseableIterable<KeyGroupStatePartitionStreamProvider> keyedStateInputs =
                context.rawKeyedStateInputs();
        CloseableIterable<StatePartitionStreamProvider> operatorStateInputs =
                context.rawOperatorStateInputs();

        try {
            OptionalLong checkpointId = context.getRestoredCheckpointId();
            StateInitializationContext initializationContext =
                    new StateInitializationContextImpl(
                            checkpointId.isPresent() ? checkpointId.getAsLong() : null,
                            operatorStateBackend, // access to operator state backend
                            keyedStateStore, // access to keyed state backend
                            keyedStateInputs, // access to keyed state stream
                            operatorStateInputs); // access to operator state stream

            streamOperator.initializeState(initializationContext);
        } finally {
            closeFromRegistry(operatorStateInputs, closeableRegistry);
            closeFromRegistry(keyedStateInputs, closeableRegistry);
        }
    }

    private static void closeFromRegistry(Closeable closeable, CloseableRegistry registry) {
        if (registry.unregisterCloseable(closeable)) {
            IOUtils.closeQuietly(closeable);
        }
    }

    public void dispose() throws Exception {
        try (Closer closer = Closer.create()) {
            if (closeableRegistry.unregisterCloseable(operatorStateBackend)) {
                closer.register(operatorStateBackend);
            }
            if (closeableRegistry.unregisterCloseable(keyedStateBackend)) {
                closer.register(keyedStateBackend);
            }
            if (closeableRegistry.unregisterCloseable(asyncKeyedStateBackend)) {
                closer.register(asyncKeyedStateBackend);
            }
            if (operatorStateBackend != null) {
                closer.register(operatorStateBackend::dispose);
            }
            if (keyedStateBackend != null) {
                closer.register(keyedStateBackend::dispose);
            }
            if (asyncKeyedStateBackend != null) {
                closer.register(asyncKeyedStateBackend::dispose);
            }
        }
    }

    public OperatorSnapshotFutures snapshotState(
            CheckpointedStreamOperator streamOperator,
            Optional<InternalTimeServiceManager<?>> timeServiceManager,
            String operatorName,
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory,
            boolean isUsingCustomRawKeyedState)
            throws CheckpointException {
        KeyGroupRange keyGroupRange =
                null != keyedStateBackend
                        ? keyedStateBackend.getKeyGroupRange()
                        : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

        OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();

        StateSnapshotContextSynchronousImpl snapshotContext =
                new StateSnapshotContextSynchronousImpl(
                        checkpointId, timestamp, factory, keyGroupRange, closeableRegistry);

        snapshotState(
                streamOperator,
                timeServiceManager,
                operatorName,
                checkpointId,
                timestamp,
                checkpointOptions,
                factory,
                snapshotInProgress,
                snapshotContext,
                isUsingCustomRawKeyedState);

        return snapshotInProgress;
    }

    @VisibleForTesting
    void snapshotState(
            CheckpointedStreamOperator streamOperator,
            Optional<InternalTimeServiceManager<?>> timeServiceManager,
            String operatorName,
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory,
            OperatorSnapshotFutures snapshotInProgress,
            StateSnapshotContextSynchronousImpl snapshotContext,
            boolean isUsingCustomRawKeyedState)
            throws CheckpointException {
        try {
            if (timeServiceManager.isPresent()) {
                checkState(
                        keyedStateBackend != null,
                        "keyedStateBackend should be available with timeServiceManager");
                final InternalTimeServiceManager<?> manager = timeServiceManager.get();

                boolean requiresLegacyRawKeyedStateSnapshots =
                        keyedStateBackend instanceof AbstractKeyedStateBackend
                                && ((AbstractKeyedStateBackend<?>) keyedStateBackend)
                                        .requiresLegacySynchronousTimerSnapshots(
                                                checkpointOptions.getCheckpointType());

                if (requiresLegacyRawKeyedStateSnapshots) {
                    checkState(
                            !isUsingCustomRawKeyedState,
                            "Attempting to snapshot timers to raw keyed state, but this operator has custom raw keyed state to write.");
                    manager.snapshotToRawKeyedState(
                            snapshotContext.getRawKeyedOperatorStateOutput(), operatorName);
                }
            }
            streamOperator.snapshotState(snapshotContext);

            snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
            snapshotInProgress.setOperatorStateRawFuture(
                    snapshotContext.getOperatorStateStreamFuture());

            if (null != operatorStateBackend) {
                snapshotInProgress.setOperatorStateManagedFuture(
                        operatorStateBackend.snapshot(
                                checkpointId, timestamp, factory, checkpointOptions));
            }

            if (null != keyedStateBackend) {
                if (isCanonicalSavepoint(checkpointOptions.getCheckpointType())) {
                    SnapshotStrategyRunner<KeyedStateHandle, ? extends FullSnapshotResources<?>>
                            snapshotRunner =
                                    prepareCanonicalSavepoint(keyedStateBackend, closeableRegistry);

                    snapshotInProgress.setKeyedStateManagedFuture(
                            snapshotRunner.snapshot(
                                    checkpointId, timestamp, factory, checkpointOptions));

                } else {
                    snapshotInProgress.setKeyedStateManagedFuture(
                            keyedStateBackend.snapshot(
                                    checkpointId, timestamp, factory, checkpointOptions));
                }
            }
        } catch (Exception snapshotException) {
            try {
                snapshotInProgress.cancel();
            } catch (Exception e) {
                snapshotException.addSuppressed(e);
            }

            String snapshotFailMessage =
                    "Could not complete snapshot "
                            + checkpointId
                            + " for operator "
                            + operatorName
                            + ".";

            try {
                snapshotContext.closeExceptionally();
            } catch (IOException e) {
                snapshotException.addSuppressed(e);
            }
            throw new CheckpointException(
                    snapshotFailMessage,
                    CheckpointFailureReason.CHECKPOINT_DECLINED,
                    snapshotException);
        }
    }

    private boolean isCanonicalSavepoint(SnapshotType snapshotType) {
        return snapshotType.isSavepoint()
                && ((SavepointType) snapshotType).getFormatType() == SavepointFormatType.CANONICAL;
    }

    @Nonnull
    public static SnapshotStrategyRunner<KeyedStateHandle, ? extends FullSnapshotResources<?>>
            prepareCanonicalSavepoint(
                    CheckpointableKeyedStateBackend<?> keyedStateBackend,
                    CloseableRegistry closeableRegistry)
                    throws Exception {
        SavepointResources<?> savepointResources = keyedStateBackend.savepoint();

        SavepointSnapshotStrategy<?> savepointSnapshotStrategy =
                new SavepointSnapshotStrategy<>(savepointResources.getSnapshotResources());

        return new SnapshotStrategyRunner<>(
                "Asynchronous full Savepoint",
                savepointSnapshotStrategy,
                closeableRegistry,
                savepointResources.getPreferredSnapshotExecutionType());
    }

    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (keyedStateBackend instanceof CheckpointListener) {
            ((CheckpointListener) keyedStateBackend).notifyCheckpointComplete(checkpointId);
        }
    }

    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (keyedStateBackend instanceof CheckpointListener) {
            ((CheckpointListener) keyedStateBackend).notifyCheckpointAborted(checkpointId);
        }
    }

    @SuppressWarnings("unchecked")
    public <K> KeyedStateBackend<K> getKeyedStateBackend() {
        return (KeyedStateBackend<K>) keyedStateBackend;
    }

    @Nullable
    public AsyncKeyedStateBackend getAsyncKeyedStateBackend() {
        return asyncKeyedStateBackend;
    }

    public OperatorStateBackend getOperatorStateBackend() {
        return operatorStateBackend;
    }

    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {

        if (keyedStateBackend != null) {
            return keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        } else {
            throw new IllegalStateException(
                    "Cannot create partitioned state. "
                            + "The keyed state backend has not been set."
                            + "This indicates that the operator is not partitioned/keyed.");
        }
    }

    /** Create new state (v2) based on new state descriptor. */
    public <N, S extends org.apache.flink.api.common.state.v2.State, T> S getOrCreateKeyedState(
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            org.apache.flink.runtime.state.v2.StateDescriptor<T> stateDescriptor)
            throws Exception {

        if (asyncKeyedStateBackend != null) {
            return asyncKeyedStateBackend.createState(
                    defaultNamespace, namespaceSerializer, stateDescriptor);
        } else {
            throw new IllegalStateException(
                    "Cannot create partitioned state. "
                            + "The keyed state backend has not been set."
                            + "This indicates that the operator is not partitioned/keyed.");
        }
    }

    /**
     * Creates a partitioned state handle, using the state backend configured for this task.
     *
     * @throws IllegalStateException Thrown, if the key/value state was already initialized.
     * @throws Exception Thrown, if the state backend cannot create the key/value state.
     */
    protected <S extends State, N> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {

        /*
        TODO: NOTE: This method does a lot of work caching / retrieving states just to update the namespace.
        This method should be removed for the sake of namespaces being lazily fetched from the keyed
        state backend, or being set on the state directly.
        */

        if (keyedStateBackend != null) {
            return keyedStateBackend.getPartitionedState(
                    namespace, namespaceSerializer, stateDescriptor);
        } else {
            throw new RuntimeException(
                    "Cannot create partitioned state. The keyed state "
                            + "backend has not been set. This indicates that the operator is not "
                            + "partitioned/keyed.");
        }
    }

    @SuppressWarnings({"unchecked"})
    public void setCurrentKey(Object key) {
        if (keyedStateBackend != null) {
            try {
                // need to work around type restrictions
                @SuppressWarnings("rawtypes")
                CheckpointableKeyedStateBackend rawBackend = keyedStateBackend;

                rawBackend.setCurrentKey(key);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Exception occurred while setting the current key context.", e);
            }
        }
    }

    public Object getCurrentKey() {
        if (keyedStateBackend != null) {
            return keyedStateBackend.getCurrentKey();
        } else {
            throw new UnsupportedOperationException("Key can only be retrieved on KeyedStream.");
        }
    }

    public Optional<KeyedStateStore> getKeyedStateStore() {
        return Optional.ofNullable(keyedStateStore);
    }

    public Optional<KeyedStateStoreV2> getKeyedStateStoreV2() {
        return Optional.ofNullable(keyedStateStoreV2);
    }

    /** Custom state handling hooks to be invoked by {@link StreamOperatorStateHandler}. */
    // 为 Flink 的 StreamOperator 提供了自定义状态处理的钩子（hooks）
    // 容错机制集成： 任何需要保存状态以实现容错的 Flink 算子（无论是使用托管状态还是原始状态），都需要通过实现或间接实现此接口，来定义其状态的初始化和快照逻辑。
    // initializeState： 算子启动或从故障恢复时，如何从 Checkpoint 中恢复/初始化状态。
    // napshotState： Flink 进行 Checkpoint 时，算子如何将当前状态写入快照
    // 与 StreamOperatorStateHandler 协作： 如 Javadoc 所述，这些钩子是由 Flink 内部的 StreamOperatorStateHandler 调用的。
    // StreamOperatorStateHandler 负责协调所有算子的状态操作，并与底层的状态后端（State Backend）进行交互。
    public interface CheckpointedStreamOperator {
        void initializeState(StateInitializationContext context) throws Exception;

        void snapshotState(StateSnapshotContext context) throws Exception;
    }
}
