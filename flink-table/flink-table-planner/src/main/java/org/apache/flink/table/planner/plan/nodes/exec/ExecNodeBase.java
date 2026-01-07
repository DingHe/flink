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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ConfigurationJsonSerializerFilter;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JacksonInject;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link ExecNode}.
 *
 * @param <T> The type of the elements that result from this node.
 */
// ExecNodeBase 是 Apache Flink Table Planner 中所有具体执行节点（如 StreamExecFilter、BatchExecJoin 等）的基类。
// 它实现了 ExecNode 接口，并提供了大多数节点共有的通用逻辑。
// 减少代码冗余：它统一处理了 ID 管理、类型定义、输入/输出属性、以及 JSON 序列化/反序列化的逻辑，让具体的算子只需关注核心的翻译逻辑。
// 统一翻译模板：它定义了 translateToPlan 的公共流程（如配置合并、缓存结果），而将具体的算子生成逻辑留给子类通过 translateToPlanInternal 实现。
// 支持持久化计划（JSON Plan）：它通过 Jackson 注解确保执行计划可以正确地在 JSON 格式和 Java 对象之间转换，这是 Flink SQL 实现 Compiled Plan 的基础。


@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecNodeBase<T> implements ExecNode<T> {

    /**
     * The default value of this flag is false. Other cases must set this flag accordingly via
     * {@link #setCompiled(boolean)}. It is not exposed via a constructor arg to avoid complex
     * constructor overloading for all {@link ExecNode}s. However, during deserialization this flag
     * will always be set to true.
     */
    // 标记该节点是否是通过已编译计划（JSON）加载而来的
    // 如果是反序列化得到的，某些元数据（如 UID）需要严格保持不变以兼容旧状态。
    @JacksonInject("isDeserialize")
    private boolean isCompiled;
    // 该节点的可读描述字符串
    private final String description;
    // 节点的输出 LogicalType（逻辑类型）
    private final LogicalType outputType;
    // 定义了输入端的属性约束（如：分布式要求、顺序要求）
    private final List<InputProperty> inputProperties;
    // 存储了指向此节点的输入边列表，建立了物理图的连接。
    private List<ExecEdge> inputEdges;
    // 缓存已翻译生成的 Transformation<T> 对象
    private transient Transformation<T> transformation;

    private @Nullable transient OpFusionCodegenSpecGenerator fusionCodegenSpecGenerator;

    /** Holds the context information (id, name, version) as deserialized from a JSON plan. */
    // 类型为 ExecNodeContext，持有节点的 ID、版本和类型名称，主要用于 JSON 序列化。
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.WRITE_ONLY)
    private final ExecNodeContext context;

    /**
     * Retrieves the default context from the {@link ExecNodeMetadata} annotation to be serialized
     * into the JSON plan.
     */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.READ_ONLY, index = 1)
    protected final ExecNodeContext getContextFromAnnotation() {
        return isCompiled ? context : ExecNodeContext.newContext(this.getClass()).withId(getId());
    }
    // 持久化的配置信息，存储了特定于该节点的参数设置
    @JsonProperty(value = FIELD_NAME_CONFIGURATION, access = JsonProperty.Access.WRITE_ONLY)
    private final ReadableConfig persistedConfig;

    @JsonProperty(
            value = FIELD_NAME_CONFIGURATION,
            access = JsonProperty.Access.READ_ONLY,
            index = 2)
    // Custom filter to exclude node configuration if no consumed options are used
    @JsonInclude(
            value = JsonInclude.Include.CUSTOM,
            valueFilter = ConfigurationJsonSerializerFilter.class)
    public ReadableConfig getPersistedConfig() {
        return persistedConfig;
    }

    protected ExecNodeBase(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        this.context = checkNotNull(context).withId(id);
        this.persistedConfig = persistedConfig == null ? new Configuration() : persistedConfig;
        this.inputProperties = checkNotNull(inputProperties);
        this.outputType = checkNotNull(outputType);
        this.description = checkNotNull(description);
    }

    @Override
    public final int getId() {
        return context.getId();
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public LogicalType getOutputType() {
        return outputType;
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return inputProperties;
    }

    @Override
    public List<ExecEdge> getInputEdges() {
        return checkNotNull(
                inputEdges,
                "inputEdges should not null, please call `setInputEdges(List<ExecEdge>)` first.");
    }

    @Override
    public void setInputEdges(List<ExecEdge> inputEdges) {
        checkNotNull(inputEdges, "inputEdges should not be null.");
        this.inputEdges = new ArrayList<>(inputEdges);
    }

    @Override
    public void replaceInputEdge(int index, ExecEdge newInputEdge) {
        List<ExecEdge> edges = getInputEdges();
        checkArgument(index >= 0 && index < edges.size());
        edges.set(index, newInputEdge);
    }
    // 外部调用的核心接口

    @Override
    public final Transformation<T> translateToPlan(Planner planner) {
        if (transformation == null) {
            transformation =
                    translateToPlanInternal(
                            (PlannerBase) planner,
                            ExecNodeConfig.of(
                                    ((PlannerBase) planner).getTableConfig(),
                                    persistedConfig,
                                    isCompiled));
            // 如果输入要求是 SINGLETON（单并发），它会自动强制设置当前算子的并行度为 1。
            if (this instanceof SingleTransformationTranslator) {
                if (inputsContainSingleton(transformation)) {
                    transformation.setParallelism(1);
                    transformation.setMaxParallelism(1);
                }
            }
        }
        return transformation;
    }

    @Override
    public void accept(ExecNodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void setCompiled(boolean compiled) {
        isCompiled = compiled;
    }

    /**
     * Internal method, translates this node into a Flink operator.
     *
     * @param planner The planner.
     * @param config per-{@link ExecNode} configuration that contains the merged configuration from
     *     various layers which all the nodes implementing this method should use, instead of
     *     retrieving configuration from the {@code planner}. For more details check {@link
     *     ExecNodeConfig}.
     */
    // 抽象方法
    // 必须由具体的子类（如 Filter 或 Join）实现，负责编写具体的 Flink 算子创建代码。
    protected abstract Transformation<T> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config);

    private boolean inputsContainSingleton(Transformation<T> transformation) {
        return inputsContainSingleton()
                || transformation.getInputs().stream()
                        .anyMatch(
                                input ->
                                        input instanceof PartitionTransformation
                                                && ((PartitionTransformation<?>) input)
                                                                .getPartitioner()
                                                        instanceof GlobalPartitioner);
    }

    /** Whether singleton distribution is required. */
    protected boolean inputsContainSingleton() {
        return getInputProperties().stream()
                .anyMatch(
                        p ->
                                p.getRequiredDistribution().getType()
                                        == InputProperty.DistributionType.SINGLETON);
    }

    @JsonIgnore
    protected String getSimplifiedName() {
        return getClass().getSimpleName().replace("StreamExec", "").replace("BatchExec", "");
    }

    protected String createTransformationUid(String operatorName, ExecNodeConfig config) {
        return context.generateUid(operatorName, config);
    }

    protected String createTransformationName(ReadableConfig config) {
        return createFormattedTransformationName(getDescription(), getSimplifiedName(), config);
    }

    protected String createTransformationDescription(ReadableConfig config) {
        return createFormattedTransformationDescription(getDescription(), config);
    }
    // 根据配置决定是否生成 UID，并返回包含 Name、UID 和 Description 的元数据包装类。
    protected TransformationMetadata createTransformationMeta(
            String operatorName, ExecNodeConfig config) {
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass()) || !config.shouldSetUid()) {
            return new TransformationMetadata(
                    createTransformationName(config), createTransformationDescription(config));
        } else {
            return new TransformationMetadata(
                    createTransformationUid(operatorName, config),
                    createTransformationName(config),
                    createTransformationDescription(config));
        }
    }

    protected TransformationMetadata createTransformationMeta(
            String operatorName, String detailName, String simplifiedName, ExecNodeConfig config) {
        final String name = createFormattedTransformationName(detailName, simplifiedName, config);
        final String desc = createFormattedTransformationDescription(detailName, config);
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass()) || !config.shouldSetUid()) {
            return new TransformationMetadata(name, desc);
        } else {
            return new TransformationMetadata(
                    createTransformationUid(operatorName, config), name, desc);
        }
    }

    protected String createFormattedTransformationDescription(
            String description, ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("[%d]:%s", getId(), description);
        }
        return description;
    }

    protected String createFormattedTransformationName(
            String detailName, String simplifiedName, ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("%s[%d]", simplifiedName, getId());
        }
        return detailName;
    }

    @VisibleForTesting
    @JsonIgnore
    public Transformation<T> getTransformation() {
        return this.transformation;
    }

    // 默认返回 false。
    // 支持融合的算子（如 Calc）需要重写此方法返回 true。
    @Override
    public boolean supportFusionCodegen() {
        return false;
    }
    // 翻译成融合代码
    @Override
    public OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(
            Planner planner, CodeGeneratorContext parentCtx) {
        if (fusionCodegenSpecGenerator == null) {
            fusionCodegenSpecGenerator =
                    translateToFusionCodegenSpecInternal(
                            (PlannerBase) planner,
                            ExecNodeConfig.of(
                                    ((PlannerBase) planner).getTableConfig(),
                                    persistedConfig,
                                    isCompiled),
                            parentCtx);
        }

        return fusionCodegenSpecGenerator;
    }

    /**
     * Internal method, translates this node into a operator codegen spec generator.
     *
     * @param planner The planner.
     * @param config per-{@link ExecNode} configuration that contains the merged configuration from
     *     various layers which all the nodes implementing this method should use, instead of
     *     retrieving configuration from the {@code planner}. For more details check {@link
     *     ExecNodeConfig}.
     */
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        throw new TableException("This ExecNode doesn't support operator fusion codegen now.");
    }
}
