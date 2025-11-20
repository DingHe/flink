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

package org.apache.flink.api.dag;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.core.memory.ManagedMemoryUseCase;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code Transformation} represents the operation that creates a DataStream. Every DataStream has
 * an underlying {@code Transformation} that is the origin of said DataStream.
 *
 * <p>API operations such as DataStream#map create a tree of {@code Transformation}s underneath.
 * When the stream program is to be executed this graph is translated to a StreamGraph using
 * StreamGraphGenerator.
 *
 * <p>A {@code Transformation} does not necessarily correspond to a physical operation at runtime.
 * Some operations are only logical concepts. Examples of this are union, split/select data stream,
 * partitioning.
 *
 * <p>The following graph of {@code Transformations}:
 *
 * <pre>{@code
 *   Source              Source
 *      +                   +
 *      |                   |
 *      v                   v
 *  Rebalance          HashPartition
 *      +                   +
 *      |                   |
 *      |                   |
 *      +------>Union<------+
 *                +
 *                |
 *                v
 *              Split
 *                +
 *                |
 *                v
 *              Select
 *                +
 *                v
 *               Map
 *                +
 *                |
 *                v
 *              Sink
 * }</pre>
 *
 * <p>Would result in this graph of operations at runtime:
 *
 * <pre>{@code
 * Source              Source
 *   +                   +
 *   |                   |
 *   |                   |
 *   +------->Map<-------+
 *             +
 *             |
 *             v
 *            Sink
 * }</pre>
 *
 * <p>The information about partitioning, union, split/select end up being encoded in the edges that
 * connect the sources to the map operation.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}
 */
// Transformation 是 Flink DataStream API 中用于表示逻辑数据流操作的基类
// 在用户编写 Flink 程序时，如 stream.map(...).keyBy(...).window(...)，每一个操作（map、keyBy、window 等）在 Flink 内部都对应一个或多个具体的 Transformation 实例。
// 构建逻辑图： Transformation 实例通过 getInputs() 方法相互连接，共同构成了 Flink 逻辑执行图（Transformation Graph）。
// 这个图是 Flink 运行时优化和生成物理执行图（JobGraph/StreamGraph）的基础。
// 配置元数据： 存储了 Flink 运行时所需的所有关键配置信息，例如并行度、名称、输出数据类型、资源规格、UID 和内存管理设置等。
// 抽象操作： 作为一个抽象基类，它将具体的业务逻辑实现（如 MapTransformation、KeyedTransformation）与通用的配置和图结构管理逻辑分离。
// Transformation 是 Flink 描述用户程序的最小逻辑单元，是 Flink 将高级 API 代码转换为可执行 Job 的关键中间步骤。
@Internal
public abstract class Transformation<T> {

    // Has to be equal to StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM
    // 最大并行度上限。
    // 定义了 Flink 中最大并行度（也是 Key Group 数量）的硬性上限值，即 $2^{15} = 32768$。
    public static final int UPPER_BOUND_MAX_PARALLELISM = 1 << 15;

    // This is used to assign a unique ID to every Transformation
    // ID 计数器。
    // 一个原子整数，用于为每个新创建的 Transformation 分配一个唯一的 ID。
    private static final AtomicInteger ID_COUNTER = new AtomicInteger(0);

    // If true, the parallelism of the transformation is explicitly set and should be respected.
    // Otherwise the parallelism can be changed at runtime.
    // 并行度是否已配置。
    // 如果为 true，表示并行度是用户显式设置的，运行时应尊重此值，不应随意更改。
    private boolean parallelismConfigured;
    // 获取新节点 ID。
    // 调用 ID_COUNTER.incrementAndGet()，返回一个递增的、全局唯一的 id。
    public static int getNewNodeId() {
        return ID_COUNTER.incrementAndGet();
    }
    // 唯一 ID。
    // 由 getNewNodeId() 分配的、在整个 Flink Job 内唯一的 ID，用于标识这个逻辑操作。
    protected final int id;
    // 用户可读名称。
    // 显示在 Flink Web UI、可视化工具和日志中的名称（例如：Source, Map）。
    protected String name;
    // 操作描述。
    // 提供对操作的更详细描述。
    protected String description;
    // 输出类型信息。
    // 描述此 Transformation 产生的元素的数据类型，对于序列化和类型检查至关重要。
    protected TypeInformation<T> outputType;
    // This is used to handle MissingTypeInfo. As long as the outputType has not been queried
    // it can still be changed using setOutputType(). Afterwards an exception is thrown when
    // trying to change the output type.
    // 类型是否已使用。
    // 标记 outputType 是否已经被查询过。
    // 一旦为 true，outputType 就不能再被修改，以保证类型安全。
    protected boolean typeUsed;
    // 当前并行度。
    // 此操作符实例在运行时将拥有的并行子任务数量。
    private int parallelism;

    /**
     * The maximum parallelism for this stream transformation. It defines the upper limit for
     * dynamic scaling and the number of key groups used for partitioned state.
     */
    // 最大并行度。
    // 定义了动态伸缩的上限和 Keyed State 分区（Key Groups）的数量。默认为 -1。
    private int maxParallelism = -1;

    /**
     * The minimum resources for this stream transformation. It defines the lower limit for dynamic
     * resources resize in future plan.
     */
    // 最小资源规格。
    // 定义了此操作符子任务所需的最小资源（如 CPU、内存）。
    private ResourceSpec minResources = ResourceSpec.DEFAULT;

    /**
     * The preferred resources for this stream transformation. It defines the upper limit for
     * dynamic resource resize in future plan.
     */
    // 首选资源规格。
    // 定义了此操作符子任务所需的理想资源。
    private ResourceSpec preferredResources = ResourceSpec.DEFAULT;
    /**
     * Each entry in this map represents a operator scope use case that this transformation needs
     * managed memory for. The keys indicate the use cases, while the values are the
     * use-case-specific weights for this transformation. Managed memory reserved for a use case
     * will be shared by all the declaring transformations within a slot according to this weight.
     */
    // 操作符范围托管内存权重。
    // 存储了此操作符在不同用途（如排序、哈希）中对托管内存的需求权重。
    // 用于在同一个 Slot 内多个操作符竞争内存时进行公平分配。
    private final Map<ManagedMemoryUseCase, Integer> managedMemoryOperatorScopeUseCaseWeights =
            new EnumMap<>(ManagedMemoryUseCase.class);

    /**
     * This map is a cache that stores transitive predecessors and used in {@code
     * getTransitivePredecessors()}.
     */
    // 祖先缓存。
    // 缓存了当前 Transformation 的所有传递性祖先（即上游的所有操作符），用于加速复杂的图遍历和检查（如迭代中的反馈边）。
    private final Map<Transformation<T>, List<Transformation<?>>> predecessorsCache =
            new HashMap<>();
     //这两个属性是相辅相成的。managedMemorySlotScopeUseCases定义了每个Transformation的内存用途，而managedMemoryOperatorScopeUseCaseWeights则决定了当多个Transformation竞争内存时，如何分配
    /** Slot scope use cases that this transformation needs managed memory for.*/

    // 槽位范围托管内存用途。
    // 存储了此操作符使用的托管内存用途（如 STATE_BACKEND）。
    // Slot 范围的内存用于整个 Slot 内共享。
    private final Set<ManagedMemoryUseCase> managedMemorySlotScopeUseCases = new HashSet<>();
    /**
     * User-specified ID for this transformation. This is used to assign the same operator ID across
     * job restarts. There is also the automatically generated {@link #id}, which is assigned from a
     * static counter. That field is independent from this.
     */
    // 用户提供的唯一 ID。
    // 用户为确保 Job 重启/升级时状态能正确匹配而指定的 ID（用于生成 JobVertexID）
    private String uid;
    // 用户提供的哈希值。
    // 用于 JobVertexID 的备用哈希，主要用于 Flink 版本迁移和故障排除。
    private String userProvidedNodeHash;
    // 网络缓冲区超时时间。
    // 定义数据在发送到网络前可以停留在部分满的缓冲区中的最长时间，影响延迟和吞吐量。
    protected long bufferTimeout = -1;
    // 槽位共享组。
    // 定义了哪些操作符实例可以共享同一个 TaskManager Slot。
    private Optional<SlotSharingGroup> slotSharingGroup;
    // 共置组 Key。
    // 调度器将具有相同 Key 的子任务放置在同一 Slot 中，以确保数据局部性（内部特性）。
    @Nullable private String coLocationGroupKey;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    public Transformation(String name, TypeInformation<T> outputType, int parallelism) {
        this(name, outputType, parallelism, true);
    }

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     * @param parallelismConfigured If true, the parallelism of the transformation is explicitly set
     *     and should be respected. Otherwise the parallelism can be changed at runtime.
     */
    public Transformation(
            String name,
            TypeInformation<T> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        this.id = getNewNodeId();
        this.name = checkNotNull(name);
        this.outputType = outputType;
        this.parallelism = parallelism;
        this.slotSharingGroup = Optional.empty();
        this.parallelismConfigured =
                parallelismConfigured && parallelism != ExecutionConfig.PARALLELISM_DEFAULT;
    }

    /** Returns the unique ID of this {@code Transformation}. */
    public int getId() {
        return id;
    }

    /** Changes the name of this {@code Transformation}. */
    public void setName(String name) {
        this.name = name;
    }

    /** Returns the name of this {@code Transformation}. */
    public String getName() {
        return name;
    }

    /** Returns the predecessorsCache of this {@code Transformation}. */
    @VisibleForTesting
    Map<Transformation<T>, List<Transformation<?>>> getPredecessorsCache() {
        return predecessorsCache;
    }

    /** Changes the description of this {@code Transformation}. */
    public void setDescription(String description) {
        this.description = checkNotNull(description);
    }

    /** Returns the description of this {@code Transformation}. */
    public String getDescription() {
        return description;
    }

    /** Returns the parallelism of this {@code Transformation}. */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Sets the parallelism of this {@code Transformation}.
     *
     * @param parallelism The new parallelism to set on this {@code Transformation}.
     */
    public void setParallelism(int parallelism) {
        setParallelism(parallelism, true);
    }

    public void setParallelism(int parallelism, boolean parallelismConfigured) {
        OperatorValidationUtils.validateParallelism(parallelism);
        this.parallelism = parallelism;
        this.parallelismConfigured =
                parallelismConfigured && parallelism != ExecutionConfig.PARALLELISM_DEFAULT;
    }

    public boolean isParallelismConfigured() {
        return parallelismConfigured;
    }

    /**
     * Gets the maximum parallelism for this stream transformation.
     *
     * @return Maximum parallelism of this transformation.
     */
    public int getMaxParallelism() {
        return maxParallelism;
    }

    /**
     * Sets the maximum parallelism for this stream transformation.
     *
     * @param maxParallelism Maximum parallelism for this stream transformation.
     */
    public void setMaxParallelism(int maxParallelism) {
        OperatorValidationUtils.validateMaxParallelism(maxParallelism, UPPER_BOUND_MAX_PARALLELISM);
        this.maxParallelism = maxParallelism;
    }

    /**
     * Sets the minimum and preferred resources for this stream transformation.
     *
     * @param minResources The minimum resource of this transformation.
     * @param preferredResources The preferred resource of this transformation.
     */
    public void setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
        OperatorValidationUtils.validateMinAndPreferredResources(minResources, preferredResources);
        this.minResources = minResources;
        this.preferredResources = preferredResources;
    }

    /**
     * Gets the minimum resource of this stream transformation.
     *
     * @return The minimum resource of this transformation.
     */
    public ResourceSpec getMinResources() {
        return minResources;
    }

    /**
     * Gets the preferred resource of this stream transformation.
     *
     * @return The preferred resource of this transformation.
     */
    public ResourceSpec getPreferredResources() {
        return preferredResources;
    }

    /**
     * Declares that this transformation contains certain operator scope managed memory use case.
     *
     * @param managedMemoryUseCase The use case that this transformation declares needing managed
     *     memory for.
     * @param weight Use-case-specific weights for this transformation. Used for sharing managed
     *     memory across transformations for OPERATOR scope use cases. Check the individual {@link
     *     ManagedMemoryUseCase} for the specific weight definition.
     * @return The previous weight, if exist.
     */
    // 声明此操作符需要操作符范围的托管内存，并指定竞争内存时的权重。
    public Optional<Integer> declareManagedMemoryUseCaseAtOperatorScope(
            ManagedMemoryUseCase managedMemoryUseCase, int weight) {
        checkNotNull(managedMemoryUseCase);
        checkArgument(
                managedMemoryUseCase.scope == ManagedMemoryUseCase.Scope.OPERATOR,
                "Use case is not operator scope.");
        checkArgument(weight > 0, "Weights for operator scope use cases must be greater than 0.");

        return Optional.ofNullable(
                managedMemoryOperatorScopeUseCaseWeights.put(managedMemoryUseCase, weight));
    }

    /**
     * Declares that this transformation contains certain slot scope managed memory use case.
     *
     * @param managedMemoryUseCase The use case that this transformation declares needing managed
     *     memory for.
     */
    // 声明此操作符需要槽位范围的托管内存。
    public void declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase managedMemoryUseCase) {
        checkNotNull(managedMemoryUseCase);
        checkArgument(managedMemoryUseCase.scope == ManagedMemoryUseCase.Scope.SLOT);

        managedMemorySlotScopeUseCases.add(managedMemoryUseCase);
    }
    // 根据任务是否有状态后端，动态添加或移除 STATE_BACKEND 这一 Slot 范围的托管内存用途。
    protected void updateManagedMemoryStateBackendUseCase(boolean hasStateBackend) {
        if (hasStateBackend) {
            managedMemorySlotScopeUseCases.add(ManagedMemoryUseCase.STATE_BACKEND);
        } else {
            managedMemorySlotScopeUseCases.remove(ManagedMemoryUseCase.STATE_BACKEND);
        }
    }

    /**
     * Get operator scope use cases that this transformation needs managed memory for, and the
     * use-case-specific weights for this transformation. The weights are used for sharing managed
     * memory across transformations for the use cases. Check the individual {@link
     * ManagedMemoryUseCase} for the specific weight definition.
     */
    // 返回操作符范围内存用途及其权重的不可修改 Map。
    public Map<ManagedMemoryUseCase, Integer> getManagedMemoryOperatorScopeUseCaseWeights() {
        return Collections.unmodifiableMap(managedMemoryOperatorScopeUseCaseWeights);
    }

    /** Get slot scope use cases that this transformation needs managed memory for. */
    // 返回槽位范围内存用途的不可修改 Set。
    public Set<ManagedMemoryUseCase> getManagedMemorySlotScopeUseCases() {
        return Collections.unmodifiableSet(managedMemorySlotScopeUseCases);
    }

    /**
     * Sets an user provided hash for this operator. This will be used AS IS the create the
     * JobVertexID.
     *
     * <p>The user provided hash is an alternative to the generated hashes, that is considered when
     * identifying an operator through the default hash mechanics fails (e.g. because of changes
     * between Flink versions).
     *
     * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting.
     * The provided hash needs to be unique per transformation and job. Otherwise, job submission
     * will fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an
     * operator chain and trying so will let your job fail.
     *
     * <p>A use case for this is in migration between Flink versions or changing the jobs in a way
     * that changes the automatically generated hashes. In this case, providing the previous hashes
     * directly through this method (e.g. obtained from old logs) can help to reestablish a lost
     * mapping from states to their target operator.
     *
     * @param uidHash The user provided hash for this operator. This will become the JobVertexID,
     *     which is shown in the logs and web ui.
     */
    public void setUidHash(String uidHash) {

        checkNotNull(uidHash);
        checkArgument(
                uidHash.matches("^[0-9A-Fa-f]{32}$"),
                "Node hash must be a 32 character String that describes a hex code. Found: "
                        + uidHash);

        this.userProvidedNodeHash = uidHash;
    }

    /**
     * Gets the user provided hash.
     *
     * @return The user provided hash.
     */
    public String getUserProvidedNodeHash() {
        return userProvidedNodeHash;
    }

    /**
     * Sets an ID for this {@link Transformation}. This is will later be hashed to a uidHash which
     * is then used to create the JobVertexID (that is shown in logs and the web ui).
     *
     * <p>The specified ID is used to assign the same operator ID across job submissions (for
     * example when starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per transformation and job.
     * Otherwise, job submission will fail.
     *
     * @param uid The unique user-specified ID of this transformation.
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * Returns the user-specified ID of this transformation.
     *
     * @return The unique user-specified ID of this transformation.
     */
    public String getUid() {
        return uid;
    }

    /**
     * Returns the slot sharing group of this transformation if present.
     *
     * @see #setSlotSharingGroup(SlotSharingGroup)
     */
    public Optional<SlotSharingGroup> getSlotSharingGroup() {
        return slotSharingGroup;
    }

    /**
     * Sets the slot sharing group of this transformation. Parallel instances of operations that are
     * in the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Initially, an operation is in the default slot sharing group. This can be explicitly set
     * using {@code setSlotSharingGroup("default")}.
     *
     * @param slotSharingGroupName The slot sharing group's name.
     */
    public void setSlotSharingGroup(String slotSharingGroupName) {
        this.slotSharingGroup =
                Optional.of(SlotSharingGroup.newBuilder(slotSharingGroupName).build());
    }

    /**
     * Sets the slot sharing group of this transformation. Parallel instances of operations that are
     * in the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Initially, an operation is in the default slot sharing group. This can be explicitly set
     * with constructing a {@link SlotSharingGroup} with name {@code "default"}.
     *
     * @param slotSharingGroup which contains name and its resource spec.
     */
    public void setSlotSharingGroup(SlotSharingGroup slotSharingGroup) {
        this.slotSharingGroup = Optional.of(slotSharingGroup);
    }

    /**
     * <b>NOTE:</b> This is an internal undocumented feature for now. It is not clear whether this
     * will be supported and stable in the long term.
     *
     * <p>Sets the key that identifies the co-location group. Operators with the same co-location
     * key will have their corresponding subtasks placed into the same slot by the scheduler.
     *
     * <p>Setting this to null means there is no co-location constraint.
     */
    public void setCoLocationGroupKey(@Nullable String coLocationGroupKey) {
        this.coLocationGroupKey = coLocationGroupKey;
    }

    /**
     * <b>NOTE:</b> This is an internal undocumented feature for now. It is not clear whether this
     * will be supported and stable in the long term.
     *
     * <p>Gets the key that identifies the co-location group. Operators with the same co-location
     * key will have their corresponding subtasks placed into the same slot by the scheduler.
     *
     * <p>If this is null (which is the default), it means there is no co-location constraint.
     */
    @Nullable
    public String getCoLocationGroupKey() {
        return coLocationGroupKey;
    }

    /**
     * Tries to fill in the type information. Type information can be filled in later when the
     * program uses a type hint. This method checks whether the type information has ever been
     * accessed before and does not allow modifications if the type was accessed already. This
     * ensures consistency by making sure different parts of the operation do not assume different
     * type information.
     *
     * @param outputType The type information to fill in.
     * @throws IllegalStateException Thrown, if the type information has been accessed before.
     */
    public void setOutputType(TypeInformation<T> outputType) {
        if (typeUsed) {
            throw new IllegalStateException(
                    "TypeInformation cannot be filled in for the type after it has been used. "
                            + "Please make sure that the type info hints are the first call after"
                            + " the transformation function, "
                            + "before any access to types or semantic properties, etc.");
        }
        this.outputType = outputType;
    }

    /**
     * Returns the output type of this {@code Transformation} as a {@link TypeInformation}. Once
     * this is used once the output type cannot be changed anymore using {@link #setOutputType}.
     *
     * @return The output type of this {@code Transformation}
     */
    public TypeInformation<T> getOutputType() {
        if (outputType instanceof MissingTypeInfo) {
            MissingTypeInfo typeInfo = (MissingTypeInfo) this.outputType;
            throw new InvalidTypesException(
                    "The return type of function '"
                            + typeInfo.getFunctionName()
                            + "' could not be determined automatically, due to type erasure. "
                            + "You can give type information hints by using the returns(...) "
                            + "method on the result of the transformation call, or by letting "
                            + "your function implement the 'ResultTypeQueryable' "
                            + "interface.",
                    typeInfo.getTypeException());
        }
        typeUsed = true;
        return this.outputType;
    }

    /**
     * Set the buffer timeout of this {@code Transformation}. The timeout defines how long data may
     * linger in a partially full buffer before being sent over the network.
     *
     * <p>Lower timeouts lead to lower tail latencies, but may affect throughput. For Flink 1.5+,
     * timeouts of 1ms are feasible for jobs with high parallelism.
     *
     * <p>A value of -1 means that the default buffer timeout should be used. A value of zero
     * indicates that no buffering should happen, and all records/events should be immediately sent
     * through the network, without additional buffering.
     */
    public void setBufferTimeout(long bufferTimeout) {
        checkArgument(bufferTimeout >= -1);
        this.bufferTimeout = bufferTimeout;
    }

    /**
     * Returns the buffer timeout of this {@code Transformation}.
     *
     * @see #setBufferTimeout(long)
     */
    public long getBufferTimeout() {
        return bufferTimeout;
    }

    /**
     * Returns all transitive predecessor {@code Transformation}s of this {@code Transformation}.
     * This is, for example, used when determining whether a feedback edge of an iteration actually
     * has the iteration head as a predecessor.
     *
     * @return The list of transitive predecessors.
     */
    // 抽象方法。
    // 由子类实现，用于递归地计算并返回此 Transformation 的所有传递性祖先（包括上游的上游）。
    protected abstract List<Transformation<?>> getTransitivePredecessorsInternal();

    /**
     * Returns all transitive predecessor {@code Transformation}s of this {@code Transformation}.
     * This is, for example, used when determining whether a feedback edge of an iteration actually
     * has the iteration head as a predecessor. This method is just a wrapper on top of {@code
     * getTransitivePredecessorsInternal} method with public access. It uses caching internally.
     *
     * @return The list of transitive predecessors.
     */
    //获取传递性祖先。
    // 对 getTransitivePredecessorsInternal() 的封装，使用 predecessorsCache 缓存结果，避免重复计算。
    public final List<Transformation<?>> getTransitivePredecessors() {
        return predecessorsCache.computeIfAbsent(this, key -> getTransitivePredecessorsInternal());
    }

    /**
     * Returns the {@link Transformation transformations} that are the immediate predecessors of the
     * current transformation in the transformation graph.
     */
    // 抽象方法。
    // 由子类实现，返回当前 Transformation 的直接上游 Transformation 列表。这是构建逻辑图的基础。
    public abstract List<Transformation<?>> getInputs();

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{"
                + "id="
                + id
                + ", name='"
                + name
                + '\''
                + ", outputType="
                + outputType
                + ", parallelism="
                + parallelism
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Transformation)) {
            return false;
        }

        Transformation<?> that = (Transformation<?>) o;
        return Objects.equals(bufferTimeout, that.bufferTimeout)
                && Objects.equals(id, that.id)
                && Objects.equals(parallelism, that.parallelism)
                && Objects.equals(name, that.name)
                && Objects.equals(outputType, that.outputType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, outputType, parallelism, bufferTimeout);
    }
}
