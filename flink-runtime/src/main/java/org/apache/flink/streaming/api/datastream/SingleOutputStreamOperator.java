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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@code SingleOutputStreamOperator} represents a user defined transformation applied on a {@link
 * DataStream} with one predefined output type.
 * 代表一个已应用了用户自定义转换（如 map、filter、process 等）并且只产生一个主要输出流的算子。
 * 它封装了对这个特定转换算子的所有配置选项，比如设置并行度、名称、UID、资源、链式策略等
 * @param <T> The type of the elements in this stream.
 */
@Public
public class SingleOutputStreamOperator<T> extends DataStream<T> {

    /** Indicate this is a non-parallel operator and cannot set a non-1 degree of parallelism. * */
    //用于指示该算子是否是非并行的。如果设置为 true，则该算子的并行度只能是1
    protected boolean nonParallel = false;

    /**
     * We keep track of the side outputs that were already requested and their types. With this, we
     * can catch the case when a side output with a matching id is requested for a different type
     * because this would lead to problems at runtime.
     */
    //用于记录所有已经被请求的侧输出及其对应的类型信息
    private Map<OutputTag<?>, TypeInformation<?>> requestedSideOutputs = new HashMap<>();

    protected SingleOutputStreamOperator(
            StreamExecutionEnvironment environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    /**
     * Gets the name of the current data stream. This name is used by the visualization and logging
     * during runtime.
     * 获取当前算子的名称
     * @return Name of the stream.
     */
    public String getName() {
        return transformation.getName();
    }

    /**
     * Sets the name of the current data stream. This name is used by the visualization and logging
     * during runtime.
     * 设置当前算子的名称。这是一个链式调用方法，返回当前实例，方便进行链式编程
     * @return The named operator.
     */
    public SingleOutputStreamOperator<T> name(String name) {
        transformation.setName(name);
        return this;
    }

    /**
     * Sets an ID for this operator.
     *
     * <p>The specified ID is used to assign the same operator ID across job submissions (for
     * example when starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per transformation and job.
     * Otherwise, job submission will fail.
     * 设置算子的唯一 ID（UID）。UID 是在保存点（Savepoint）和故障恢复时用于标识算子的重要属性
     * @param uid The unique user-specified ID of this transformation.
     * @return The operator with the specified ID.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> uid(String uid) {
        transformation.setUid(uid);
        return this;
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
     * 设置一个用户提供的哈希值作为 JobVertexID
     * @param uidHash The user provided hash for this operator. This will become the JobVertexID,
     *     which is shown in the logs and web ui.
     * @return The operator with the user provided hash.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> setUidHash(String uidHash) {
        transformation.setUidHash(uidHash);
        return this;
    }

    /**
     * Sets the parallelism for this operator.
     * 设置当前算子的并行度。这个值决定了 Flink 集群中运行该算子的任务实例数量
     * @param parallelism The parallelism for this operator.
     * @return The operator with set parallelism.
     */
    public SingleOutputStreamOperator<T> setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, canBeParallel());
        transformation.setParallelism(parallelism);

        return this;
    }

    /**
     * Sets the maximum parallelism of this operator.
     *
     * <p>The maximum parallelism specifies the upper bound for dynamic scaling. It also defines the
     * number of key groups used for partitioned state.
     * 设置当前算子的最大并行度。
     * 该值定义了动态扩缩容的上限，也决定了分区状态（Partitioned State）的键组（key groups）数量
     * @param maxParallelism Maximum parallelism
     * @return The operator with set maximum parallelism
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> setMaxParallelism(int maxParallelism) {
        OperatorValidationUtils.validateMaxParallelism(maxParallelism, canBeParallel());
        transformation.setMaxParallelism(maxParallelism);

        return this;
    }

    //	---------------------------------------------------------------------------
    //	 Fine-grained resource profiles are an incomplete work-in-progress feature
    //	 The setters are hence private at this point.
    //	---------------------------------------------------------------------------

    /**
     * Sets the minimum and preferred resources for this operator, and the lower and upper resource
     * limits will be considered in dynamic resource resize feature for future plan.
     *
     * @param minResources The minimum resources for this operator.
     * @param preferredResources The preferred resources for this operator.
     * @return The operator with set minimum and preferred resources.
     */
    private SingleOutputStreamOperator<T> setResources(
            ResourceSpec minResources, ResourceSpec preferredResources) {
        transformation.setResources(minResources, preferredResources);

        return this;
    }

    /**
     * Sets the resources for this operator, the minimum and preferred resources are the same by
     * default.
     *
     * @param resources The resources for this operator.
     * @return The operator with set minimum and preferred resources.
     */
    private SingleOutputStreamOperator<T> setResources(ResourceSpec resources) {
        transformation.setResources(resources, resources);

        return this;
    }

    private boolean canBeParallel() {
        return !nonParallel;
    }

    /**
     * Sets the parallelism and maximum parallelism of this operator to one. And mark this operator
     * cannot set a non-1 degree of parallelism.
     * 强制将算子的并行度和最大并行度都设置为1，并将其标记为非并行算子
     * @return The operator with only one parallelism.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> forceNonParallel() {
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        nonParallel = true;
        return this;
    }

    /**
     * Sets the buffering timeout for data produced by this operation. The timeout defines how long
     * data may linger in a partially full buffer before being sent over the network.
     *
     * <p>Lower timeouts lead to lower tail latencies, but may affect throughput. Timeouts of 1 ms
     * still sustain high throughput, even for jobs with high parallelism.
     *
     * <p>A value of '-1' means that the default buffer timeout should be used. A value of '0'
     * indicates that no buffering should happen, and all records/events should be immediately sent
     * through the network, without additional buffering.
     * 设置数据缓冲区的超时时间
     * @param timeoutMillis The maximum time between two output flushes.
     * @return The operator with buffer timeout set.
     */
    public SingleOutputStreamOperator<T> setBufferTimeout(long timeoutMillis) {
        checkArgument(timeoutMillis >= -1, "timeout must be >= -1");
        transformation.setBufferTimeout(timeoutMillis);
        return this;
    }

    /**
     * Sets the {@link ChainingStrategy} for the given operator affecting the way operators will
     * possibly be co-located on the same thread for increased performance.
     * 设置算子的算子链策略。该策略决定了 Flink 是否以及如何将多个算子合并到同一个物理任务中，以提高性能
     * @param strategy The selected {@link ChainingStrategy}
     * @return The operator with the modified chaining strategy
     */
    @PublicEvolving
    private SingleOutputStreamOperator<T> setChainingStrategy(ChainingStrategy strategy) {
        if (transformation instanceof PhysicalTransformation) {
            ((PhysicalTransformation<T>) transformation).setChainingStrategy(strategy);
        } else {
            throw new UnsupportedOperationException(
                    "Cannot set chaining strategy on " + transformation);
        }
        return this;
    }

    /**
     * Turns off chaining for this operator so thread co-location will not be used as an
     * optimization.
     *
     * <p>Chaining can be turned off for the whole job by {@link
     * StreamExecutionEnvironment#disableOperatorChaining()} however it is not advised for
     * performance considerations.
     * 禁用当前算子的算子链。这意味着该算子将不会与前后的算子合并，而是在独立的任务线程中执行
     * @return The operator with chaining disabled
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> disableChaining() {
        return setChainingStrategy(ChainingStrategy.NEVER);
    }

    /**
     * Starts a new task chain beginning at this operator. This operator will not be chained (thread
     * co-located for increased performance) to any previous tasks even if possible.
     * 强制当前算子开启一个新的算子链。这意味着它不会与之前的算子合并，但可以与后面的算子合并
     * @return The operator with chaining set.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> startNewChain() {
        return setChainingStrategy(ChainingStrategy.HEAD);
    }

    // ------------------------------------------------------------------------
    //  Type hinting
    // ------------------------------------------------------------------------

    /**
     * Adds a type information hint about the return type of this operator. This method can be used
     * in cases where Flink cannot determine automatically what the produced type of a function is.
     * That can be the case if the function uses generic type variables in the return type that
     * cannot be inferred from the input type.
     *
     * <p>Classes can be used as type hints for non-generic types (classes without generic
     * parameters), but not for generic types like for example Tuples. For those generic types,
     * please use the {@link #returns(TypeHint)} method.
     * 为算子的输出类型提供类型提示。当 Flink 的类型推断系统无法自动确定输出类型时（例如，当使用泛型时），可以使用此方法来显式指定
     * @param typeClass The class of the returned data type.
     * @return This operator with the type information corresponding to the given type class.
     */
    public SingleOutputStreamOperator<T> returns(Class<T> typeClass) {
        requireNonNull(typeClass, "type class must not be null.");

        try {
            return returns(TypeInformation.of(typeClass));
        } catch (InvalidTypesException e) {
            throw new InvalidTypesException(
                    "Cannot infer the type information from the class alone."
                            + "This is most likely because the class represents a generic type. In that case,"
                            + "please use the 'returns(TypeHint)' method instead.");
        }
    }

    /**
     * Adds a type information hint about the return type of this operator. This method can be used
     * in cases where Flink cannot determine automatically what the produced type of a function is.
     * That can be the case if the function uses generic type variables in the return type that
     * cannot be inferred from the input type.
     *
     * <p>Use this method the following way:
     *
     * <pre>{@code
     * DataStream<Tuple2<String, Double>> result =
     *     stream.flatMap(new FunctionWithNonInferrableReturnType())
     *           .returns(new TypeHint<Tuple2<String, Double>>(){});
     * }</pre>
     * 为算子的输出类型提供类型提示。与上一个方法类似，但使用 TypeHint，这更适用于泛型类型
     * @param typeHint The type hint for the returned data type.
     * @return This operator with the type information corresponding to the given type hint.
     */
    public SingleOutputStreamOperator<T> returns(TypeHint<T> typeHint) {
        requireNonNull(typeHint, "TypeHint must not be null");

        try {
            return returns(TypeInformation.of(typeHint));
        } catch (InvalidTypesException e) {
            throw new InvalidTypesException(
                    "Cannot infer the type information from the type hint. "
                            + "Make sure that the TypeHint does not use any generic type variables.");
        }
    }

    /**
     * Adds a type information hint about the return type of this operator. This method can be used
     * in cases where Flink cannot determine automatically what the produced type of a function is.
     * That can be the case if the function uses generic type variables in the return type that
     * cannot be inferred from the input type.
     * 为算子的输出类型提供类型信息。这是类型提示的通用底层方法，接受一个 TypeInformation 对象
     * <p>In most cases, the methods {@link #returns(Class)} and {@link #returns(TypeHint)} are
     * preferable.
     *
     * @param typeInfo type information as a return type hint
     * @return This operator with a given return type hint.
     */
    public SingleOutputStreamOperator<T> returns(TypeInformation<T> typeInfo) {
        requireNonNull(typeInfo, "TypeInformation must not be null");

        transformation.setOutputType(typeInfo);
        return this;
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    /**
     * Sets the slot sharing group of this operation. Parallel instances of operations that are in
     * the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Operations inherit the slot sharing group of input operations if all input operations are
     * in the same slot sharing group and no slot sharing group was explicitly specified.
     *
     * <p>Initially an operation is in the default slot sharing group. An operation can be put into
     * the default group explicitly by setting the slot sharing group to {@code "default"}.
     *
     * 设置算子的槽位共享组（Slot Sharing Group）。
     * 属于同一组的算子可以共享同一个 TaskManager 的任务槽（Task Slot），从而节省资源
     *
     * @param slotSharingGroup The slot sharing group name.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> slotSharingGroup(String slotSharingGroup) {
        transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    /**
     * Sets the slot sharing group of this operation. Parallel instances of operations that are in
     * the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Operations inherit the slot sharing group of input operations if all input operations are
     * in the same slot sharing group and no slot sharing group was explicitly specified.
     *
     * <p>Initially an operation is in the default slot sharing group. An operation can be put into
     * the default group explicitly by setting the slot sharing group with name {@code "default"}.
     *
     * @param slotSharingGroup Which contains name and its resource spec.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> slotSharingGroup(SlotSharingGroup slotSharingGroup) {
        transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    /**
     * Gets the {@link DataStream} that contains the elements that are emitted from an operation
     * into the side output with the given {@link OutputTag}.
     * 根据给定的 OutputTag，获取当前算子的侧输出流。这允许你将数据发送到主输出流之外的其他流，并对这些流进行单独处理
     * @see org.apache.flink.streaming.api.functions.ProcessFunction.Context#output(OutputTag,
     *     Object)
     */
    public <X> SideOutputDataStream<X> getSideOutput(OutputTag<X> sideOutputTag) {
        sideOutputTag = clean(requireNonNull(sideOutputTag));

        // make a defensive copy
        sideOutputTag = new OutputTag<X>(sideOutputTag.getId(), sideOutputTag.getTypeInfo());

        TypeInformation<?> type = requestedSideOutputs.get(sideOutputTag);
        if (type != null && !type.equals(sideOutputTag.getTypeInfo())) {
            throw new UnsupportedOperationException(
                    "A side output with a matching id was "
                            + "already requested with a different type. This is not allowed, side output "
                            + "ids need to be unique.");
        }

        requestedSideOutputs.put(sideOutputTag, sideOutputTag.getTypeInfo());

        SideOutputTransformation<X> sideOutputTransformation =
                new SideOutputTransformation<>(this.getTransformation(), sideOutputTag);
        return new SideOutputDataStream<>(this.getExecutionEnvironment(), sideOutputTransformation);
    }

    /**
     * Sets the description for this operation.
     *
     * <p>Description is used in json plan and web ui, but not in logging and metrics where only
     * name is available. Description is expected to provide detailed information about the sink,
     * while name is expected to be more simple, providing summary information only, so that we can
     * have more user-friendly logging messages and metric tags without losing useful messages for
     * debugging.
     * 为算子设置描述信息。与 name 不同，description 用于提供更详细的信息，主要用于 JSON 计划和 Web UI，而不是日志和度量指标
     * @param description The description for this operation.
     * @return The operation with new description.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<T> setDescription(String description) {
        transformation.setDescription(description);
        return this;
    }

    /**
     * Cache the intermediate result of the transformation. Only support bounded streams and
     * currently only block mode is supported. The cache is generated lazily at the first time the
     * intermediate result is computed. The cache will be clear when {@link
     * CachedDataStream#invalidate()} called or the {@link StreamExecutionEnvironment} close.
     * 根据给定的 OutputTag，获取当前算子的侧输出流。这允许你将数据发送到主输出流之外的其他流，并对这些流进行单独处理
     * @return CachedDataStream that can use in later job to reuse the cached intermediate result.
     */
    @PublicEvolving
    public CachedDataStream<T> cache() {
        if (!(this.transformation instanceof PhysicalTransformation)) {
            throw new IllegalStateException(
                    "Cache can only be called with physical transformation or side output transformation");
        }

        return new CachedDataStream<>(this.environment, this.transformation);
    }
}
