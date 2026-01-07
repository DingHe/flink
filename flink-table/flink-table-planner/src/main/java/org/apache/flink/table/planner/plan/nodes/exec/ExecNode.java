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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.ExecNode.FIELD_NAME_TYPE;

/**
 * The representation of execution information for a {@link FlinkPhysicalRel}.
 *
 * @param <T> The type of the elements that result from this node.
 */
// 在 Apache Flink 的 Table Planner 中，ExecNode（执行节点）是整个查询计划从逻辑层向物理层过渡的核心组件。
// 如果你理解了 ExecNode，就理解了 Flink SQL 是如何从一段代码变成分布式任务的关键。
// ExecNode 处于 Flink 计划优化流程的最底层，它的主要作用可以概括为以下三点：
// 执行计划的“蓝图”：它是对 FlinkPhysicalRel（物理关系节点）转换后的结果。
// 物理关系节点还在 Calcite 的控制范围内，而 ExecNode 则是 Flink 内部定义的、描述“如何执行”的抽象模型
// 计划持久化的载体：它是 Flink SQL Compiled Plan（已编译计划） 的基础。
// 由于它带有丰富的 Jackson 注解（如 @JsonProperty），Flink 可以将整个 SQL 拓扑序列化为 JSON 格式保存。这样即使 Flink 版本升级，也可以通过加载 JSON 计划来保证作业拓扑的一致性。
// 翻译的统一接口：它继承了 ExecNodeTranslator 和 FusionCodegenExecNode。这意味着它既负责将自己翻译成 DataStream API 的 Transformation，也负责处理算子融合（Operator Fusion）的代码生成。

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = FIELD_NAME_TYPE,
        visible = true)
@JsonTypeIdResolver(ExecNodeTypeIdResolver.class)
@Internal
public interface ExecNode<T> extends ExecNodeTranslator<T>, FusionCodegenExecNode {
    // 这些常量主要用于 JSON 序列化和反序列化，定义了存储在 JSON 中的键名：
    // 节点在拓扑图中的唯一标识 ID
    String FIELD_NAME_ID = "id";
    // 节点类型（如 StreamExecLookupJoin 或 BatchExecHashJoin）
    String FIELD_NAME_TYPE = "type";
    // 节点特有的配置信息。
    String FIELD_NAME_CONFIGURATION = "configuration";
    // 节点的可读描述，通常显示在 Web UI 上
    String FIELD_NAME_DESCRIPTION = "description";
    // 描述输入端的特性（如：是否是阻塞式的、是否需要保持顺序等）。
    String FIELD_NAME_INPUT_PROPERTIES = "inputProperties";
    // 节点输出数据的逻辑类型。
    String FIELD_NAME_OUTPUT_TYPE = "outputType";
    // 如果是流处理，描述该节点维护的状态信息。
    String FIELD_NAME_STATE = "state";

    /** The unique ID of the node. */
    // 获取节点在当前 ExecNodeGraph 中的唯一整数 ID。
    @JsonProperty(value = FIELD_NAME_ID, index = 0)
    int getId();

    /** Returns a string which describes this node. */
    // 返回该节点的详细描述字符串
    @JsonProperty(value = FIELD_NAME_DESCRIPTION)
    String getDescription();

    /**
     * Returns the output {@link LogicalType} of this node, this type should be consistent with the
     * type parameter {@link T}.
     *
     * <p>Such as, if T is {@link RowData}, the output type should be {@link RowType}. please refer
     * to the JavaDoc of {@link RowData} for more info about mapping of logical types to internal
     * data structures.
     */
    // 获取该节点输出数据的 LogicalType
    //
    @JsonProperty(value = FIELD_NAME_OUTPUT_TYPE)
    LogicalType getOutputType();

    /**
     * Returns a list of this node's input properties.
     *
     * <p>NOTE: If there are no inputs, returns an empty list, not null.
     *
     * @return List of this node's input properties.
     */
    // 返回输入属性列表
    @JsonProperty(value = FIELD_NAME_INPUT_PROPERTIES)
    List<InputProperty> getInputProperties();

    /**
     * Returns a list of this node's input {@link ExecEdge}s.
     *
     * <p>NOTE: If there are no inputs, returns an empty list, not null.
     */
    // 获取连接到此节点的所有输入边（ExecEdge）
    @JsonIgnore
    List<ExecEdge> getInputEdges();

    /**
     * Sets the input {@link ExecEdge}s which connect this nodes and its input nodes.
     *
     * <p>NOTE: If there are no inputs, the given inputEdges should be empty, not null.
     *
     * @param inputEdges the input {@link ExecEdge}s.
     */
    // 设置当前节点的输入边
    @JsonIgnore
    void setInputEdges(List<ExecEdge> inputEdges);

    /**
     * Replaces the <code>ordinalInParent</code><sup>th</sup> input edge.
     *
     * @param index Position of the child input edge, 0 is the first.
     * @param newInputEdge New edge that should be put at position `index`.
     */
    // 替换指定位置（索引）的输入边
    void replaceInputEdge(int index, ExecEdge newInputEdge);

    /**
     * Accepts a visit from a {@link ExecNodeVisitor}.
     *
     * @param visitor ExecNodeVisitor.
     */
    // 接受一个访问者（Visitor）
    void accept(ExecNodeVisitor visitor);

    /**
     * Declares whether the node has been created as part of a plan compilation. Some translation
     * properties might be impacted by this (e.g. UID generation for transformations).
     */
    // 设置节点是否是通过“计划编译”创建的标记
    void setCompiled(boolean isCompiled);
}
