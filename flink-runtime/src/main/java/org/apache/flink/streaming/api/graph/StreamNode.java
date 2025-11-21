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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Class representing the operators in the streaming programs, with all their properties. */
// StreamNode 类是 Flink 流式拓扑图（StreamGraph）中的基本元素，代表了 Flink 流式程序中的一个算子（Operator）或用户定义的逻辑操作。
// 算子抽象： 它封装了 Flink 拓扑图中一个节点（即一个操作，如 map, filter, keyBy 等）所需的所有配置和元数据。
// 图结构： 它通过管理入边 (inEdges) 和出边 (outEdges) 来定义 Flink StreamGraph 的结构。
// 运行时配置： 它存储了执行该操作所需的关键运行时配置，包括并行度、资源规格、Slot 共享组、算子工厂以及数据序列化器等。
// StreamNode 是 Flink 逻辑执行图中的一个“操作符”节点，负责将用户代码中的操作转换为 Flink 运行时所需的详细配置。


@Internal
public class StreamNode {
    // 节点唯一 ID。
    // 在整个 StreamGraph 中，用于唯一标识这个 StreamNode。
    private final int id;
    // 并行度。
    // 定义该算子将运行的并行任务实例数量。
    private int parallelism;
    /**
     * Maximum parallelism for this stream node. The maximum parallelism is the upper limit for
     * dynamic scaling and the number of key groups used for partitioned state.
     */
    // 最大并行度。
    // 该算子并行度的上限，同时决定了用于分区状态的 Key Group 数量。
    private int maxParallelism;
    // 最小资源规格。
    // 运行该节点所需的最小 CPU、内存等资源。
    private ResourceSpec minResources = ResourceSpec.DEFAULT;
    // 首选资源规格。
    // 运行该节点的推荐资源配置。
    private ResourceSpec preferredResources = ResourceSpec.DEFAULT;
    // 操作符范围管理内存权重。
    // 定义该算子对 Flink 管理内存（Managed Memory）的需求及其权重。
    private final Map<ManagedMemoryUseCase, Integer> managedMemoryOperatorScopeUseCaseWeights =
            new HashMap<>();
    // Slot 范围管理内存类型。
    // 包含该算子在 Slot 范围内需要的管理内存使用类型。
    private final Set<ManagedMemoryUseCase> managedMemorySlotScopeUseCases = new HashSet<>();
    // 缓冲区超时时间。
    // 毫秒，定义数据在发送到下游前在缓冲区中等待的最长时间。
    private long bufferTimeout;
    // 操作名称。
    // 算子在代码中或默认的名称（如 map, filter），用于日志和监控。
    private final String operatorName;
    // 操作描述。
    // 更详细的描述信息。
    private String operatorDescription;
    // Slot 共享组。
    // 如果设置，该节点的所有实例将与同一组中的其他节点实例共享 Slot，以优化资源利用。
    private @Nullable String slotSharingGroup;
    // 共同定位组。
    //如果设置，该组中的所有任务必须部署在同一个 TaskManager 上，用于保证数据本地性（例如用于迭代操作）。
    private @Nullable String coLocationGroup;
    // 状态分区函数。
    // 针对有状态操作，定义如何对键控状态进行分区。
    private KeySelector<?, ?>[] statePartitioners = new KeySelector[0];
    // 状态键序列化器。
    // 用于序列化和反序列化键控状态的 Key。
    private TypeSerializer<?> stateKeySerializer;
    // 算子工厂。
    // 用于在运行时创建 StreamOperator 实例的工厂。
    private @Nullable StreamOperatorFactory<?> operatorFactory;
    // 输入序列化器。
    // 用于序列化和反序列化该节点接收到的输入数据的序列化器数组（支持多输入）。
    private TypeSerializer<?>[] typeSerializersIn = new TypeSerializer[0];
    // 输出序列化器。
    // 用于序列化和反序列化该节点输出数据的序列化器。
    private TypeSerializer<?> typeSerializerOut;
    // 入边列表。
    // 连接到该节点的上游 StreamEdge 列表。
    private List<StreamEdge> inEdges = new ArrayList<StreamEdge>();
    // 出边列表。
    // 从该节点连接到下游的 StreamEdge 列表。
    private List<StreamEdge> outEdges = new ArrayList<StreamEdge>();
    // 任务调用类。
    // 指定了该节点在 TaskManager 上运行时实际执行的 Flink 任务类（如 StreamTask）。
    private final Class<? extends TaskInvokable> jobVertexClass;
    // 输入格式
    // 如果该节点是 Source 任务（例如批处理中的文件 Source），则包含其输入格式。
    private InputFormat<?, ?> inputFormat;
    // 输出格式。
    // 如果该节点是 Sink 任务，则包含其输出格式。
    private OutputFormat<?> outputFormat;
    // 用户为 Transformation 指定的唯一标识符，用于状态管理。
    private String transformationUID;
    // 用户哈希值。
    // 用户定义的哈希值，用于控制算子链化和任务分配。
    private String userHash;
    // 输入要求。
    // 定义了该算子对特定输入的特殊要求（例如，是否需要全量或仅增量输入）。
    private final Map<Integer, StreamConfig.InputRequirement> inputRequirements = new HashMap<>();
    // 消费集群数据集 ID。
    // 如果该节点消费的是 Flink 集群中持久化的数据集（例如批处理中的缓存数据），则为其 ID。
    private @Nullable IntermediateDataSetID consumeClusterDatasetId;
    // 支持并发执行尝试。
    // 标识该任务是否支持并发地启动多个执行尝试（通常用于推测执行）。
    private boolean supportsConcurrentExecutionAttempts = true;
    // 并行度是否已配置。
    // 标识并行度是否由用户或系统明确设置，而不是使用默认值。
    private boolean parallelismConfigured = false;

    @VisibleForTesting
    public StreamNode(
            Integer id,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            @Nullable StreamOperator<?> operator,
            String operatorName,
            Class<? extends TaskInvokable> jobVertexClass) {
        this(
                id,
                slotSharingGroup,
                coLocationGroup,
                operator == null ? null : SimpleOperatorFactory.of(operator),
                operatorName,
                jobVertexClass);
    }

    public StreamNode(
            Integer id,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            @Nullable StreamOperatorFactory<?> operatorFactory,
            String operatorName,
            Class<? extends TaskInvokable> jobVertexClass) {
        this.id = id;
        this.operatorName = operatorName;
        this.operatorDescription = operatorName;
        this.operatorFactory = operatorFactory;
        this.jobVertexClass = jobVertexClass;
        this.slotSharingGroup = slotSharingGroup;
        this.coLocationGroup = coLocationGroup;
    }

    public void addInEdge(StreamEdge inEdge) {
        checkState(
                inEdges.stream().noneMatch(inEdge::equals),
                "Adding not unique edge = %s to existing inEdges = %s",
                inEdge,
                inEdges);
        if (inEdge.getTargetId() != getId()) {
            throw new IllegalArgumentException("Destination id doesn't match the StreamNode id");
        } else {
            inEdges.add(inEdge);
        }
    }

    public void addOutEdge(StreamEdge outEdge) {
        checkState(
                outEdges.stream().noneMatch(outEdge::equals),
                "Adding not unique edge = %s to existing outEdges = %s",
                outEdge,
                outEdges);
        if (outEdge.getSourceId() != getId()) {
            throw new IllegalArgumentException("Source id doesn't match the StreamNode id");
        } else {
            outEdges.add(outEdge);
        }
    }

    public List<StreamEdge> getOutEdges() {
        return outEdges;
    }

    public List<StreamEdge> getInEdges() {
        return inEdges;
    }

    public List<Integer> getOutEdgeIndices() {
        List<Integer> outEdgeIndices = new ArrayList<Integer>();

        for (StreamEdge edge : outEdges) {
            outEdgeIndices.add(edge.getTargetId());
        }

        return outEdgeIndices;
    }

    public List<Integer> getInEdgeIndices() {
        List<Integer> inEdgeIndices = new ArrayList<Integer>();

        for (StreamEdge edge : inEdges) {
            inEdgeIndices.add(edge.getSourceId());
        }

        return inEdgeIndices;
    }

    public int getId() {
        return id;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        setParallelism(parallelism, true);
    }

    void setParallelism(Integer parallelism, boolean parallelismConfigured) {
        this.parallelism = parallelism;
        this.parallelismConfigured =
                parallelismConfigured && parallelism != ExecutionConfig.PARALLELISM_DEFAULT;
    }

    /**
     * Get the maximum parallelism for this stream node.
     *
     * @return Maximum parallelism
     */
    int getMaxParallelism() {
        return maxParallelism;
    }

    /**
     * Set the maximum parallelism for this stream node.
     *
     * @param maxParallelism Maximum parallelism to be set
     */
    void setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    public ResourceSpec getMinResources() {
        return minResources;
    }

    public ResourceSpec getPreferredResources() {
        return preferredResources;
    }

    public void setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
        this.minResources = minResources;
        this.preferredResources = preferredResources;
    }

    public void setManagedMemoryUseCaseWeights(
            Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            Set<ManagedMemoryUseCase> slotScopeUseCases) {
        managedMemoryOperatorScopeUseCaseWeights.putAll(operatorScopeUseCaseWeights);
        managedMemorySlotScopeUseCases.addAll(slotScopeUseCases);
    }

    public Map<ManagedMemoryUseCase, Integer> getManagedMemoryOperatorScopeUseCaseWeights() {
        return Collections.unmodifiableMap(managedMemoryOperatorScopeUseCaseWeights);
    }

    public Set<ManagedMemoryUseCase> getManagedMemorySlotScopeUseCases() {
        return Collections.unmodifiableSet(managedMemorySlotScopeUseCases);
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public void setBufferTimeout(Long bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }

    @VisibleForTesting
    public StreamOperator<?> getOperator() {
        assert operatorFactory != null && operatorFactory instanceof SimpleOperatorFactory;
        return (StreamOperator<?>) ((SimpleOperatorFactory) operatorFactory).getOperator();
    }

    public @Nullable StreamOperatorFactory<?> getOperatorFactory() {
        return operatorFactory;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getOperatorDescription() {
        return operatorDescription;
    }

    public void setOperatorDescription(String operatorDescription) {
        this.operatorDescription = operatorDescription;
    }

    public void setSerializersIn(TypeSerializer<?>... typeSerializersIn) {
        checkArgument(typeSerializersIn.length > 0);
        // Unfortunately code above assumes type serializer can be null, while users of for example
        // getTypeSerializersIn would be confused by returning an array size of two with all
        // elements set to null...
        this.typeSerializersIn =
                Arrays.stream(typeSerializersIn)
                        .filter(typeSerializer -> typeSerializer != null)
                        .toArray(TypeSerializer<?>[]::new);
    }

    public TypeSerializer<?>[] getTypeSerializersIn() {
        return typeSerializersIn;
    }

    public TypeSerializer<?> getTypeSerializerOut() {
        return typeSerializerOut;
    }
    //反序列化
    public void setSerializerOut(TypeSerializer<?> typeSerializerOut) {
        this.typeSerializerOut = typeSerializerOut;
    }

    public Class<? extends TaskInvokable> getJobVertexClass() {
        return jobVertexClass;
    }

    public InputFormat<?, ?> getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(InputFormat<?, ?> inputFormat) {
        this.inputFormat = inputFormat;
    }

    public OutputFormat<?> getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(OutputFormat<?> outputFormat) {
        this.outputFormat = outputFormat;
    }

    public void setSlotSharingGroup(@Nullable String slotSharingGroup) {
        this.slotSharingGroup = slotSharingGroup;
    }

    @Nullable
    public String getSlotSharingGroup() {
        return slotSharingGroup;
    }

    public void setCoLocationGroup(@Nullable String coLocationGroup) {
        this.coLocationGroup = coLocationGroup;
    }

    public @Nullable String getCoLocationGroup() {
        return coLocationGroup;
    }

    public boolean isSameSlotSharingGroup(StreamNode downstreamVertex) {
        return (slotSharingGroup == null && downstreamVertex.slotSharingGroup == null)
                || (slotSharingGroup != null
                        && slotSharingGroup.equals(downstreamVertex.slotSharingGroup));
    }

    @Override
    public String toString() {
        return operatorName + "-" + id;
    }

    public KeySelector<?, ?>[] getStatePartitioners() {
        return statePartitioners;
    }
    //设置分区器
    public void setStatePartitioners(KeySelector<?, ?>... statePartitioners) {
        checkArgument(statePartitioners.length > 0);
        this.statePartitioners = statePartitioners;
    }

    public TypeSerializer<?> getStateKeySerializer() {
        return stateKeySerializer;
    }

    public void setStateKeySerializer(TypeSerializer<?> stateKeySerializer) {
        this.stateKeySerializer = stateKeySerializer;
    }

    public String getTransformationUID() {
        return transformationUID;
    }

    void setTransformationUID(String transformationId) {
        this.transformationUID = transformationId;
    }

    public String getUserHash() {
        return userHash;
    }

    public void setUserHash(String userHash) {
        this.userHash = userHash;
    }

    public void addInputRequirement(
            int inputIndex, StreamConfig.InputRequirement inputRequirement) {
        inputRequirements.put(inputIndex, inputRequirement);
    }

    public Map<Integer, StreamConfig.InputRequirement> getInputRequirements() {
        return inputRequirements;
    }

    public Optional<OperatorCoordinator.Provider> getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        if (operatorFactory != null && operatorFactory instanceof CoordinatedOperatorFactory) {
            return Optional.of(
                    ((CoordinatedOperatorFactory) operatorFactory)
                            .getCoordinatorProvider(operatorName, operatorID));
        } else {
            return Optional.empty();
        }
    }

    boolean isParallelismConfigured() {
        return parallelismConfigured;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamNode that = (StreamNode) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Nullable
    public IntermediateDataSetID getConsumeClusterDatasetId() {
        return consumeClusterDatasetId;
    }

    public void setConsumeClusterDatasetId(
            @Nullable IntermediateDataSetID consumeClusterDatasetId) {
        this.consumeClusterDatasetId = consumeClusterDatasetId;
    }

    public boolean isSupportsConcurrentExecutionAttempts() {
        return supportsConcurrentExecutionAttempts;
    }

    public void setSupportsConcurrentExecutionAttempts(
            boolean supportsConcurrentExecutionAttempts) {
        this.supportsConcurrentExecutionAttempts = supportsConcurrentExecutionAttempts;
    }

    public boolean isOutputOnlyAfterEndOfStream() {
        if (operatorFactory == null) {
            return false;
        }
        return operatorFactory.getOperatorAttributes().isOutputOnlyAfterEndOfStream();
    }
}
