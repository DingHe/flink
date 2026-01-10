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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base {@link ExecNode} for temporal table join which shares most methods.
 *
 * <p>For a lookup join query:
 *
 * <pre>
 * SELECT T.id, T.content, D.age
 * FROM T JOIN userTable FOR SYSTEM_TIME AS OF T.proctime AS D
 * ON T.content = concat(D.name, '!') AND D.age = 11 AND T.id = D.id
 * WHERE D.name LIKE 'Jack%'
 * </pre>
 *
 * <p>The LookupJoin physical node encapsulates the following RelNode tree:
 *
 * <pre>
 *      Join (l.name = r.name)
 *    /     \
 * RelNode  Calc (concat(name, "!") as name, name LIKE 'Jack%')
 *           |
 *        DimTable (lookup-keys: age=11, id=l.id)
 *     (age, id, name)
 * </pre>
 *
 * <ul>
 *   <li>lookupKeys: [$0=11, $1=l.id] ($0 and $1 is the indexes of age and id in dim table)
 *   <li>calcOnTemporalTable: calc on temporal table rows before join
 *   <li>joinCondition: join condition on temporal table rows after calc
 * </ul>
 *
 * <p>The workflow of lookup join:
 *
 * <p>1) lookup records dimension table using the lookup-keys <br>
 * 2) project & filter on the lookup-ed records <br>
 * 3) join left input record and lookup-ed records <br>
 * 4) only outputs the rows which match to the condition <br>
 */
// Apache Flink Table Planner 中的一个核心抽象基类。它承载了 SQL 中 维表关联（Lookup Join / Temporal Table Join） 的物理执行逻辑。
// 在 Flink SQL 中，当你执行类似 SELECT ... FROM T JOIN dim_table FOR SYSTEM_TIME AS OF T.proctime ON ... 的查询时，系统不会将维表全量加载到状态中，而是针对主流（左表）的每一条数据，实时去外部系统（如 MySQL, HBase, Redis）查询匹配的行。
// 核心作用包括：
// 统一逻辑架构：它封装了“查询维表 -> 处理查询结果（Calc/Filter）-> 与左表合并”的通用流程。
// 支持多种执行模式：它同时支持**同步（Sync）和异步（Async）**两种查找方式。
// 处理代码生成（Codegen）：它利用代码生成技术生成高效的 ProcessFunction 或 AsyncFunction，减少虚函数调用开销。
// 处理容错与重试：它集成了查找重试（Retry）机制，以应对外部系统暂时的不可用或延迟。

public abstract class CommonExecLookupJoin extends ExecNodeBase<RowData> {

    public static final String LOOKUP_JOIN_TRANSFORMATION = "lookup-join";

    public static final String LOOKUP_JOIN_MATERIALIZE_TRANSFORMATION = "lookup-join-materialize";

    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_PRE_FILTER_CONDITION = "preFilterCondition";
    public static final String FIELD_NAME_REMAINING_JOIN_CONDITION = "joinCondition";
    public static final String FIELD_NAME_TEMPORAL_TABLE = "temporalTable";
    public static final String FIELD_NAME_LOOKUP_KEYS = "lookupKeys";
    public static final String FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE =
            "projectionOnTemporalTable";
    public static final String FIELD_NAME_FILTER_ON_TEMPORAL_TABLE = "filterOnTemporalTable";

    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";

    public static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";
    public static final String FIELD_NAME_RETRY_OPTIONS = "retryOptions";
    // 定义 Join 类型。维表关联目前仅支持 INNER JOIN 和 LEFT JOIN
    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    /**
     * lookup keys: the key is index in dim table. the value is source of lookup key either constant
     * or field from right table.
     */
    // Key 是维表列的索引，
    // Value 是查找键（可能是常量或左表的字段引用）。
    @JsonProperty(FIELD_NAME_LOOKUP_KEYS)
    private final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys;
    // 维表的元数据说明，包含了如何获取维表数据源（TableSource）的信息。
    @JsonProperty(FIELD_NAME_TEMPORAL_TABLE)
    private final TemporalTableSourceSpec temporalTableSourceSpec;
    // 如果 SQL 对维表进行了列裁剪，这里存储了对应的 RexNode 列表。
    @JsonProperty(FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE)
    private final @Nullable List<RexNode> projectionOnTemporalTable;
    // 如果在关联维表时有针对维表字段的简单过滤（如 D.age > 18），会存放在此。
    @JsonProperty(FIELD_NAME_FILTER_ON_TEMPORAL_TABLE)
    private final @Nullable RexNode filterOnTemporalTable;

    /** pre-filter condition on left input except lookup keys. */
    // 前置过滤条件。在发起外部查找之前，先对左表数据进行过滤，减少不必要的外部请求。
    @JsonProperty(FIELD_NAME_PRE_FILTER_CONDITION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable RexNode preFilterCondition;

    /** remaining join condition except pre-filter & equi-conditions except lookup keys. */
    // 残留关联条件。
    // 在外部查找返回结果后，进一步进行的条件判断（例如非等值 Join 条件）。
    @JsonProperty(FIELD_NAME_REMAINING_JOIN_CONDITION)
    private final @Nullable RexNode remainingJoinCondition;
    // 输入流的数据变更模式（如是否包含 Update/Delete）
    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE)
    private final ChangelogMode inputChangelogMode;
    // 异步查找配置。如果用户启用了异步模式，该属性包含超时时间、缓冲容量（Capacity）和输出模式（Ordered/Unordered）
    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable LookupJoinUtil.AsyncLookupOptions asyncLookupOptions;
    // 重试配置。定义了查找失败时的重试策略（如最大次数、延迟等）
    @JsonProperty(FIELD_NAME_RETRY_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable LookupJoinUtil.RetryLookupOptions retryOptions;

    protected CommonExecLookupJoin(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            @Nullable RexNode preFilterCondition,
            @Nullable RexNode remainingJoinCondition,
            // TODO: refactor this into TableSourceTable, once legacy TableSource is removed
            TemporalTableSourceSpec temporalTableSourceSpec,
            Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            @Nullable List<RexNode> projectionOnTemporalTable,
            @Nullable RexNode filterOnTemporalTable,
            @Nullable LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
            @Nullable LookupJoinUtil.RetryLookupOptions retryOptions,
            ChangelogMode inputChangelogMode,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = checkNotNull(joinType);
        this.preFilterCondition = preFilterCondition;
        this.remainingJoinCondition = remainingJoinCondition;
        this.lookupKeys = Collections.unmodifiableMap(checkNotNull(lookupKeys));
        this.temporalTableSourceSpec = checkNotNull(temporalTableSourceSpec);
        this.projectionOnTemporalTable = projectionOnTemporalTable;
        this.filterOnTemporalTable = filterOnTemporalTable;
        this.inputChangelogMode = inputChangelogMode;
        this.asyncLookupOptions = asyncLookupOptions;
        this.retryOptions = retryOptions;
    }

    public TemporalTableSourceSpec getTemporalTableSourceSpec() {
        return temporalTableSourceSpec;
    }
    // 将逻辑上的 Lookup Join 转换为 Flink 算子树（Transformation）
    protected Transformation<RowData> createJoinTransformation(
            PlannerBase planner,
            ExecNodeConfig config,
            boolean upsertMaterialize,
            boolean lookupKeyContainsPrimaryKey) {
        // 从规范说明（Spec）中获取维表的逻辑表对象 RelOptTable，并调用 validate 确保该表支持 Lookup 操作。
        RelOptTable temporalTable =
                temporalTableSourceSpec.getTemporalTable(
                        planner.getFlinkContext(), unwrapTypeFactory(planner));
        // validate whether the node is valid and supported.
        validate(temporalTable);
        // 获取输入流的行类型、维表的行类型以及 Join 后的结果行类型，
        // 为后续的类型匹配和算子生成做准备
        final ExecEdge inputEdge = getInputEdges().get(0);
        // 获取输入流的行类型
        RowType inputRowType = (RowType) inputEdge.getOutputType();
        // 维表的行类型
        RowType tableSourceRowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
        //  Join 后的结果行类型
        RowType resultRowType = (RowType) getOutputType();
        // 校验关联键（Join Keys）在左表（输入流）和右表（维表）之间的类型是否匹配。
        validateLookupKeyType(lookupKeys, inputRowType, tableSourceRowType);
        boolean isAsyncEnabled = null != asyncLookupOptions;
        ResultRetryStrategy retryStrategy =
                retryOptions != null ? retryOptions.toRetryStrategy() : null;

        UserDefinedFunction lookupFunction =
                LookupJoinUtil.getLookupFunction(
                        temporalTable,
                        lookupKeys.keySet(),
                        planner.getFlinkContext().getClassLoader(),
                        isAsyncEnabled,
                        retryStrategy);
        UserDefinedFunctionHelper.prepareInstance(config, lookupFunction);

        boolean isLeftOuterJoin = joinType == FlinkJoinType.LEFT;
        if (isAsyncEnabled) {
            assert lookupFunction instanceof AsyncTableFunction;
        }

        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        if (upsertMaterialize) {
            // upsertMaterialize only works on sync lookup mode, async lookup is unsupported.
            assert !isAsyncEnabled && !inputChangelogMode.containsOnly(RowKind.INSERT);
            return createSyncLookupJoinWithState(
                    inputTransformation,
                    temporalTable,
                    config,
                    planner.getFlinkContext().getClassLoader(),
                    lookupKeys,
                    (TableFunction<Object>) lookupFunction,
                    planner.createRelBuilder(),
                    inputRowType,
                    tableSourceRowType,
                    resultRowType,
                    isLeftOuterJoin,
                    planner.getExecEnv().getConfig().isObjectReuseEnabled(),
                    lookupKeyContainsPrimaryKey);
        } else {
            StreamOperatorFactory<RowData> operatorFactory;
            if (isAsyncEnabled) {
                operatorFactory =
                        createAsyncLookupJoin(
                                temporalTable,
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                lookupKeys,
                                (AsyncTableFunction<Object>) lookupFunction,
                                planner.createRelBuilder(),
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                isLeftOuterJoin,
                                asyncLookupOptions);
            } else {
                operatorFactory =
                        createSyncLookupJoin(
                                temporalTable,
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                lookupKeys,
                                (TableFunction<Object>) lookupFunction,
                                planner.createRelBuilder(),
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                isLeftOuterJoin,
                                planner.getExecEnv().getConfig().isObjectReuseEnabled());
            }

            return ExecNodeUtil.createOneInputTransformation(
                    inputTransformation,
                    createTransformationMeta(LOOKUP_JOIN_TRANSFORMATION, config),
                    operatorFactory,
                    InternalTypeInfo.of(resultRowType),
                    inputTransformation.getParallelism(),
                    false);
        }
    }

    protected abstract Transformation<RowData> createSyncLookupJoinWithState(
            Transformation<RowData> inputTransformation,
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled,
            boolean lookupKeyContainsPrimaryKey);

    protected void validateLookupKeyType(
            final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            final RowType inputRowType,
            final RowType tableSourceRowType) {
        final List<String> imCompatibleConditions = new LinkedList<>();
        lookupKeys.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof LookupJoinUtil.FieldRefLookupKey)
                .forEach(
                        entry -> {
                            int rightKey = entry.getKey();
                            int leftKey =
                                    ((LookupJoinUtil.FieldRefLookupKey) entry.getValue()).index;
                            LogicalType leftType = inputRowType.getTypeAt(leftKey);
                            LogicalType rightType = tableSourceRowType.getTypeAt(rightKey);
                            boolean isCompatible =
                                    PlannerTypeUtils.isInteroperable(leftType, rightType);
                            if (!isCompatible) {
                                String leftName = inputRowType.getFieldNames().get(leftKey);
                                String rightName = tableSourceRowType.getFieldNames().get(rightKey);
                                imCompatibleConditions.add(
                                        String.format(
                                                "%s[%s]=%s[%s]",
                                                leftName, leftType, rightName, rightType));
                            }
                        });

        if (!imCompatibleConditions.isEmpty()) {
            throw new TableException(
                    "Temporal table join requires equivalent condition "
                            + "of the same type, but the condition is "
                            + StringUtils.join(imCompatibleConditions, ","));
        }
    }

    @SuppressWarnings("unchecked")
    private StreamOperatorFactory<RowData> createAsyncLookupJoin(
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            AsyncTableFunction<Object> asyncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            LookupJoinUtil.AsyncLookupOptions asyncLookupOptions) {

        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        LookupJoinCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFuncWithType =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                dataTypeFactory,
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                allLookupKeys,
                                LookupJoinUtil.getOrderedLookupKeys(allLookupKeys.keySet()),
                                asyncLookupFunction,
                                StringUtils.join(temporalTable.getQualifiedName(), "."));

        RelDataType projectionOutputRelDataType = getProjectionOutputRelDataType(relBuilder);
        RowType rightRowType =
                getRightOutputRowType(projectionOutputRelDataType, tableSourceRowType);
        // a projection or filter after table source scan
        GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture =
                LookupJoinCodeGenerator.generateTableAsyncCollector(
                        config,
                        classLoader,
                        "TableFunctionResultFuture",
                        inputRowType,
                        rightRowType,
                        JavaScalaConversionUtil.toScala(
                                Optional.ofNullable(remainingJoinCondition)));
        GeneratedFilterCondition generatedPreFilterCondition =
                LookupJoinCodeGenerator.generatePreFilterCondition(
                        config, classLoader, preFilterCondition, inputRowType);

        DataStructureConverter<?, ?> fetcherConverter =
                DataStructureConverters.getConverter(generatedFuncWithType.dataType());
        AsyncFunction<RowData, RowData> asyncFunc;
        if (projectionOnTemporalTable != null) {
            // a projection or filter after table source scan
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                            filterOnTemporalTable,
                            projectionOutputRelDataType,
                            tableSourceRowType);
            asyncFunc =
                    new AsyncLookupJoinWithCalcRunner(
                            generatedFuncWithType.tableFunc(),
                            (DataStructureConverter<RowData, Object>) fetcherConverter,
                            generatedCalc,
                            generatedResultFuture,
                            generatedPreFilterCondition,
                            InternalSerializers.create(rightRowType),
                            isLeftOuterJoin,
                            asyncLookupOptions.asyncBufferCapacity);
        } else {
            // right type is the same as table source row type, because no calc after temporal table
            asyncFunc =
                    new AsyncLookupJoinRunner(
                            generatedFuncWithType.tableFunc(),
                            (DataStructureConverter<RowData, Object>) fetcherConverter,
                            generatedResultFuture,
                            generatedPreFilterCondition,
                            InternalSerializers.create(rightRowType),
                            isLeftOuterJoin,
                            asyncLookupOptions.asyncBufferCapacity);
        }

        // Why not directly enable retry on 'AsyncWaitOperator'? because of two reasons:
        // 1. AsyncLookupJoinRunner has a 'stateful' resultFutureBuffer bind to each input record
        // (it's non-reenter-able) 2. can not lookup new value if cache empty values enabled when
        // chained with the new AsyncCachingLookupFunction. So similar to sync lookup join with
        // retry, use a 'RetryableAsyncLookupFunctionDelegator' to support retry.
        return new AsyncWaitOperatorFactory<>(
                asyncFunc,
                asyncLookupOptions.asyncTimeout,
                asyncLookupOptions.asyncBufferCapacity,
                asyncLookupOptions.asyncOutputMode);
    }

    private StreamOperatorFactory<RowData> createSyncLookupJoin(
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled) {
        return SimpleOperatorFactory.of(
                new ProcessOperator<>(
                        createSyncLookupJoinFunction(
                                temporalTable,
                                config,
                                classLoader,
                                allLookupKeys,
                                syncLookupFunction,
                                relBuilder,
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                isLeftOuterJoin,
                                isObjectReuseEnabled)));
    }

    protected RelDataType getProjectionOutputRelDataType(RelBuilder relBuilder) {
        return projectionOnTemporalTable != null
                ? RexUtil.createStructType(unwrapTypeFactory(relBuilder), projectionOnTemporalTable)
                : null;
    }

    protected RowType getRightOutputRowType(
            RelDataType projectionOutputRelDataType, RowType tableSourceRowType) {
        return projectionOutputRelDataType != null
                ? (RowType) toLogicalType(projectionOutputRelDataType)
                : tableSourceRowType;
    }

    protected ProcessFunction<RowData, RowData> createSyncLookupJoinFunction(
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled) {

        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        int[] orderedLookupKeys = LookupJoinUtil.getOrderedLookupKeys(allLookupKeys.keySet());

        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                LookupJoinCodeGenerator.generateSyncLookupFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        tableSourceRowType,
                        resultRowType,
                        allLookupKeys,
                        orderedLookupKeys,
                        syncLookupFunction,
                        StringUtils.join(temporalTable.getQualifiedName(), "."),
                        isObjectReuseEnabled);

        RelDataType projectionOutputRelDataType = getProjectionOutputRelDataType(relBuilder);
        RowType rightRowType =
                getRightOutputRowType(projectionOutputRelDataType, tableSourceRowType);
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                LookupJoinCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, classLoader),
                        inputRowType,
                        rightRowType,
                        resultRowType,
                        JavaScalaConversionUtil.toScala(
                                Optional.ofNullable(remainingJoinCondition)),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        true);

        GeneratedFilterCondition generatedPreFilterCondition =
                LookupJoinCodeGenerator.generatePreFilterCondition(
                        config, classLoader, preFilterCondition, inputRowType);
        ProcessFunction<RowData, RowData> processFunc;
        if (projectionOnTemporalTable != null) {
            // a projection or filter after table source scan
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                            filterOnTemporalTable,
                            projectionOutputRelDataType,
                            tableSourceRowType);

            processFunc =
                    new LookupJoinWithCalcRunner(
                            generatedFetcher,
                            generatedCalc,
                            generatedCollector,
                            generatedPreFilterCondition,
                            isLeftOuterJoin,
                            rightRowType.getFieldCount());
        } else {
            // right type is the same as table source row type, because no calc after temporal table
            processFunc =
                    new LookupJoinRunner(
                            generatedFetcher,
                            generatedCollector,
                            generatedPreFilterCondition,
                            isLeftOuterJoin,
                            rightRowType.getFieldCount());
        }
        return processFunc;
    }

    // ----------------------------------------------------------------------------------------
    //                                       Validation
    // ----------------------------------------------------------------------------------------

    private void validate(RelOptTable temporalTable) {

        // validate table source and function implementation first
        validateTableSource(temporalTable);

        // check join on all fields of PRIMARY KEY or (UNIQUE) INDEX
        if (lookupKeys.isEmpty()) {
            throw new TableException(
                    String.format(
                            "Temporal table join requires an equality condition on fields of %s.",
                            getTableSourceDescription(temporalTable)));
        }

        // check type
        if (joinType != FlinkJoinType.LEFT && joinType != FlinkJoinType.INNER) {
            throw new TableException(
                    String.format(
                            "Temporal table join currently only support INNER JOIN and LEFT JOIN, but was %s JOIN.",
                            joinType.toString()));
        }
        // success
    }

    private String getTableSourceDescription(RelOptTable temporalTable) {
        if (temporalTable instanceof TableSourceTable) {
            return String.format(
                    "table [%s]",
                    ((TableSourceTable) temporalTable)
                            .contextResolvedTable()
                            .getIdentifier()
                            .asSummaryString());
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            return String.format(
                    "table [%s]",
                    ((LegacyTableSourceTable<?>) temporalTable)
                            .tableIdentifier()
                            .asSummaryString());
        }
        // should never reach here.
        return "";
    }

    private void validateTableSource(RelOptTable temporalTable) {
        if (temporalTable instanceof TableSourceTable) {
            if (!(((TableSourceTable) temporalTable).tableSource() instanceof LookupTableSource)) {
                throw new TableException(
                        String.format(
                                "%s must implement LookupTableSource interface if it is used in temporal table join.",
                                getTableSourceDescription(temporalTable)));
            }

        } else if (temporalTable instanceof LegacyTableSourceTable) {

            TableSource<?> tableSource = ((LegacyTableSourceTable<?>) temporalTable).tableSource();
            if (!(tableSource instanceof LookupableTableSource)) {

                throw new TableException(
                        String.format(
                                "%s must implement LookupableTableSource interface if it is used in temporal table join.",
                                getTableSourceDescription(temporalTable)));
            }
            TypeInformation<?> tableSourceProducedType =
                    TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(
                            tableSource.getProducedDataType());
            if (!(tableSourceProducedType instanceof InternalTypeInfo
                            && tableSourceProducedType
                                    .getTypeClass()
                                    .isAssignableFrom(RowData.class))
                    && !(tableSourceProducedType instanceof RowTypeInfo)) {
                throw new TableException(
                        String.format(
                                "Temporal table join only support Row or RowData type as return type of temporal table. But was %s.",
                                tableSourceProducedType));
            }
        } else {
            throw new TableException(
                    String.format(
                            "table [%s] is neither TableSourceTable not LegacyTableSourceTable.",
                            StringUtils.join(temporalTable.getQualifiedName(), ".")));
        }
    }
}
