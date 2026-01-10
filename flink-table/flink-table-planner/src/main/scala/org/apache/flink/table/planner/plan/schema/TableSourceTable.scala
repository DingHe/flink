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
package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.catalog.ContextResolvedTable
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.connectors.DynamicSourceUtils
import org.apache.flink.table.planner.plan.abilities.source.{SourceAbilityContext, SourceAbilitySpec}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.rel.`type`.RelDataType

import java.util

/**
 * A [[FlinkPreparingTableBase]] implementation which defines the context variables required to
 * translate the Calcite [[org.apache.calcite.plan.RelOptTable]] to the Flink specific relational
 * expression with [[DynamicTableSource]].
 *
 * @param relOptSchema
 *   The RelOptSchema that this table comes from
 * @param rowType
 *   The table row type
 * @param statistic
 *   The table statistics
 * @param tableSource
 *   The [[DynamicTableSource]] for which is converted to a Calcite Table
 * @param isStreamingMode
 *   A flag that tells if the current table is in stream mode
 * @param contextResolvedTable
 *   Resolved catalog table where this table source table comes from
 * @param flinkContext
 *   The flink context which is used to generate extra digests based on abilitySpecs
 * @param abilitySpecs
 *   The abilitySpecs applied to the source
 */
// 在 Apache Flink 的 Table Planner 中，TableSourceTable 是连接 Calcite 优化器逻辑层与 Flink 物理数据源层的核心桥梁。
// 主要作用包括：
// 元数据映射：将 Flink Catalog 中的表信息、Schema 和 DynamicTableSource 封装成 Calcite 优化器能够理解的“表”对象。
// 能力支持（Abilities）：记录并管理应用在数据源上的优化策略。例如：谓词下推（Filter Pushdown）、列裁剪（Projection Pushdown）、水印生成下推等。
// 身份标识（Digest）：通过计算 abilitySpecs 的摘要（Digest），确保优化器能够区分“同一个表在应用不同下推策略后”的不同状态。
// 计划翻译：为后续将逻辑算子（Logical Scan）翻译成物理算子（Flink ExecNode/Transformation）提供必要的上下文。

class TableSourceTable(
    relOptSchema: RelOptSchema, // Calcite 的架构对象，表示该表所属的 Schema 空间。
    rowType: RelDataType, // 该表在优化器眼中的行类型。注意：应用列裁剪后，这个类型会变为裁剪后的字段集合
    statistic: FlinkStatistic, // 表的统计信息（如行数、字段分布等），优化器（CBO）根据它来计算最优执行计划。
    val tableSource: DynamicTableSource, // 核心属性。Flink 定义的连接器（Connector）接口，负责实际的数据读取逻辑。
    val isStreamingMode: Boolean, // 表示当前任务是流处理模式还是批处理模式。
    val contextResolvedTable: ContextResolvedTable, // 包含表的完整路径（Catalog/Database/Table）和原始解析后的元数据。
    val flinkContext: FlinkContext, // Flink 运行时的上下文环境，用于访问配置信息或类加载器。
    val flinkTypeFactory: FlinkTypeFactory, // 类型工厂。用于在 Flink 类型体系和 Calcite 类型体系之间进行转换。
    val abilitySpecs: Array[SourceAbilitySpec] = Array.empty) // 数组，记录了所有已经应用到该 Source 上的优化能力（如 FilterPushDownSpec）
  extends FlinkPreparingTableBase(
    relOptSchema,
    rowType,
    contextResolvedTable.getIdentifier.toList,
    statistic) {
  // 获取表的限定名（唯一标识符
  // 返回表名全路径（如 my_cat.my_db.my_table）外，还会追加 getSpecDigests 的内容。
  override def getQualifiedName: util.List[String] = {
    val builder = ImmutableList
      .builder[String]()
      .addAll(super.getQualifiedName)
    builder.addAll(getSpecDigests)
    builder.build()
  }
  // 获取优化能力的摘要列表。
  // 遍历所有的 abilitySpecs，利用 SourceAbilityContext 计算每一个下推操作的唯一字符串标识。这些标识会决定优化器缓存和节点等价性的判断。
  def getSpecDigests: util.List[String] = {
    val builder = ImmutableList.builder[String]()
    if (abilitySpecs != null && abilitySpecs.length != 0) {
      var newProducedType =
        DynamicSourceUtils.createProducedType(contextResolvedTable.getResolvedSchema, tableSource)

      for (spec <- abilitySpecs) {
        val sourceAbilityContext =
          new SourceAbilityContext(flinkContext, flinkTypeFactory, newProducedType)

        builder.add(spec.getDigests(sourceAbilityContext))
        newProducedType = spec.getProducedType.orElse(newProducedType)
      }
    }
    builder.build()
  }

  /** Adds the newSpec replacing any spec of the same class from existing ones. */
  // 当新应用一套能力（NewSpecs）时，它会检查现有的 abilitySpecs。如果新旧 Spec 的类类型一致（例如都是过滤下推），则用新的替换旧的；否则合并。
  private def mergeSpecs(
      original: Array[SourceAbilitySpec],
      newSpec: Array[SourceAbilitySpec]): Array[SourceAbilitySpec] = {
    original.filter(old => !newSpec.exists(n => old.getClass == n.getClass)) ++ newSpec
  }

  /**
   * Creates a copy of this table with specified digest.
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newRowType
   *   new row type
   * @return
   *   added TableSourceTable instance with specified digest
   */
  // 由于 Calcite 的不可变性，当优化器对表应用新的优化（下推）时，必须创建新对象
  def copy(
      newTableSource: DynamicTableSource,
      newRowType: RelDataType,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      mergeSpecs(abilitySpecs, newAbilitySpecs)
    )
  }

  /**
   * Creates a copy of this table with specified digest and context resolved table
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newResolveTable
   *   resolved table to replace
   * @param newRowType
   *   new row type
   * @return
   *   added TableSourceTable instance with specified digest
   */
  def copy(
      newTableSource: DynamicTableSource,
      newResolveTable: ContextResolvedTable,
      newRowType: RelDataType,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      newResolveTable,
      flinkContext,
      flinkTypeFactory,
      mergeSpecs(abilitySpecs, newAbilitySpecs)
    )
  }

  /**
   * Creates a copy of this table with replaced ability specs.
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newRowType
   *   new row type
   */
  // 与 Copy 的区别：它直接替换所有的能力列表，而不是调用 mergeSpecs 进行合并。用于需要重置能力的场景。
  def replace(
      newTableSource: DynamicTableSource,
      newRowType: RelDataType,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      newAbilitySpecs
    )
  }

  /**
   * Creates a copy of this table with specified digest and statistic.
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newStatistic
   *   statistic to replace
   * @return
   *   added TableSourceTable instance with specified digest and statistic
   */
  def copy(
      newTableSource: DynamicTableSource,
      newStatistic: FlinkStatistic,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      rowType,
      newStatistic,
      newTableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      mergeSpecs(abilitySpecs, newAbilitySpecs))
  }

  /**
   * Creates a copy of this table, changing the statistic
   *
   * @param newStatistic
   *   new table statistic
   * @return
   *   New TableSourceTable instance with new statistic
   */
  def copy(newStatistic: FlinkStatistic): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      rowType,
      newStatistic,
      tableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      abilitySpecs)
  }
}
