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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.IdentifierSnapshotNamespace;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup.includeExpandedColumn;
import static org.apache.flink.util.Preconditions.checkNotNull;
//专门用于处理 Flink 的 SQL 验证。它的主要作用是对 Flink SQL 查询进行验证，确保查询符合 Flink 特定的规则和语义约束
/** Extends Calcite's {@link SqlValidator} by Flink-specific behavior. */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

    // Enables CallContext#getOutputDataType() when validating SQL expressions.
    private SqlNode sqlNodeForExpectedOutputType; //希望输出的SqlNode
    private RelDataType expectedOutputType; //希望输出的数据类型
    //它为查询优化过程提供了一个上下文环境。你可以把它想象成一个“优化器的工作台”，在这个工作台上，优化器可以访问各种工具和资源来完成优化任务
    private final RelOptCluster relOptCluster;
    //SqlNode转为RelNode的上下文
    private final RelOptTable.ToRelContext toRelContext;
    //配置 SQL 框架，它是底层 Calcite 查询规划和优化框架的一部分
    private final FrameworkConfig frameworkConfig;
    //列扩展策略列表
    private final List<ColumnExpansionStrategy> columnExpansionStrategies;

    public FlinkCalciteSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlValidator.Config config,
            RelOptTable.ToRelContext toRelcontext,
            RelOptCluster relOptCluster,
            FrameworkConfig frameworkConfig) {
        super(opTab, catalogReader, typeFactory, config);
        this.relOptCluster = relOptCluster;
        this.toRelContext = toRelcontext;
        this.frameworkConfig = frameworkConfig;
        this.columnExpansionStrategies =
                ShortcutUtils.unwrapTableConfig(relOptCluster)
                        .get(TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY);
    }
    //设置希望输出的SqlNode和数据类型
    public void setExpectedOutputType(SqlNode sqlNode, RelDataType expectedOutputType) {
        this.sqlNodeForExpectedOutputType = sqlNode;
        this.expectedOutputType = expectedOutputType;
    }

    public Optional<RelDataType> getExpectedOutputType(SqlNode sqlNode) {
        if (sqlNode == sqlNodeForExpectedOutputType) {
            return Optional.of(expectedOutputType);
        }
        return Optional.empty();
    }
    //重写了 validateLiteral 方法，检查 DECIMAL 类型字面量的精度是否超过 Flink 配置的最大精度（DecimalType.MAX_PRECISION）
    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() == DECIMAL) {
            final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
            if (decimal.precision() > DecimalType.MAX_PRECISION) {
                throw newValidationError(
                        literal, Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
            }
        }
        super.validateLiteral(literal);
    }

    @Override  //处理了与表值函数（TVFs）结合使用的左外连接的特殊情况，避免了某些谓词在连接条件中不合法的情况，解决了 Calcite 中存在的一个已知问题（FLINK-7865）
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        // Due to the improper translation of lateral table left outer join in Calcite, we need to
        // temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
        if (join.getJoinType() == JoinType.LEFT
                && SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
            SqlNode right = SqlUtil.stripAs(join.getRight());
            if (right instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) right;
                SqlNode operand0 = call.operand(0);
                if (operand0 instanceof SqlBasicCall
                        && ((SqlBasicCall) operand0).getOperator()
                                instanceof org.apache.calcite.sql.SqlWindowTableFunction) {
                    return;
                }
            }
            final SqlNode condition = join.getCondition();
            if (condition != null
                    && (!SqlUtil.isLiteral(condition)
                            || ((SqlLiteral) condition).getValueAs(Boolean.class)
                                    != Boolean.TRUE)) {
                throw new ValidationException(
                        String.format(
                                "Left outer joins with a table function do not accept a predicate such as %s. "
                                        + "Only literal TRUE is accepted.",
                                condition));
            }
        }
        super.validateJoin(join, scope);
    }

    @Override //用于处理函数中的列列表参数。Flink 不支持列列表，因此它将这些参数转化为未知类型，从而在验证过程中忽略它们
    public void validateColumnListParams(
            SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
        // we don't support column lists and translate them into the unknown type in the type
        // factory,
        // this makes it possible to ignore them in the validator and fall back to regular row types
        // see also SqlFunction#deriveType
    }

    @Override //此方法特别处理了时间旅行的场景，确保时间旅行表达式是有效的时间戳，并使用正确的 schema 版本
    protected void registerNamespace(
            @Nullable SqlValidatorScope usingScope,
            @Nullable String alias,
            SqlValidatorNamespace ns,
            boolean forceNullable) {

        // Generate a new validator namespace for time travel scenario.
        // Since time travel only supports constant expressions, we need to ensure that the period
        // of
        // snapshot is not an identifier.
        Optional<SqlSnapshot> snapshot = getSnapShotNode(ns);
        if (usingScope != null
                && snapshot.isPresent()
                && !(hasInputReference(snapshot.get().getPeriod()))) {
            SqlSnapshot sqlSnapshot = snapshot.get();
            SqlNode periodNode = sqlSnapshot.getPeriod();
            SqlToRelConverter sqlToRelConverter = this.createSqlToRelConverter();
            RexNode rexNode = sqlToRelConverter.convertExpression(periodNode);
            RexNode simplifiedRexNode =
                    FlinkRexUtil.simplify(
                            sqlToRelConverter.getRexBuilder(),
                            rexNode,
                            relOptCluster.getPlanner().getExecutor());
            List<RexNode> reducedNodes = new ArrayList<>();
            relOptCluster
                    .getPlanner()
                    .getExecutor()
                    .reduce(
                            relOptCluster.getRexBuilder(),
                            Collections.singletonList(simplifiedRexNode),
                            reducedNodes);
            // check whether period is the unsupported expression
            final RexNode reducedNode = reducedNodes.get(0);
            if (!(reducedNode instanceof RexLiteral)) {
                throw new ValidationException(
                        String.format(
                                "Unsupported time travel expression: %s for the expression can not be reduced to a constant by Flink.",
                                periodNode));
            }

            RexLiteral rexLiteral = (RexLiteral) reducedNode;
            final RelDataType sqlType = rexLiteral.getType();
            if (!SqlTypeUtil.isTimestamp(sqlType)) {
                throw newValidationError(
                        periodNode,
                        Static.RESOURCE.illegalExpressionForTemporal(
                                sqlType.getSqlTypeName().getName()));
            }

            TimestampString timestampString = rexLiteral.getValueAs(TimestampString.class);
            checkNotNull(
                    timestampString,
                    "The time travel expression %s can not reduce to a valid timestamp string. This is a bug. Please file an issue.",
                    periodNode);

            TableConfig tableConfig = ShortcutUtils.unwrapContext(relOptCluster).getTableConfig();
            ZoneId zoneId = tableConfig.getLocalTimeZone();
            long timeTravelTimestamp =
                    TimestampData.fromEpochMillis(timestampString.getMillisSinceEpoch())
                            .toLocalDateTime()
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli();

            SchemaVersion schemaVersion = TimestampSchemaVersion.of(timeTravelTimestamp);
            IdentifierNamespace identifierNamespace = (IdentifierNamespace) ns;
            ns =
                    new IdentifierSnapshotNamespace(
                            identifierNamespace,
                            schemaVersion,
                            ((DelegatingScope) usingScope).getParent());

            sqlSnapshot.setOperand(
                    1,
                    SqlLiteral.createTimestamp(
                            timestampString,
                            rexLiteral.getType().getPrecision(),
                            sqlSnapshot.getPeriod().getParserPosition()));
        }

        super.registerNamespace(usingScope, alias, ns, forceNullable);
    }

    private static boolean hasInputReference(SqlNode node) {
        return node.accept(new SqlToRelConverter.SqlIdentifierFinder());
    }

    /**
     * Get the {@link SqlSnapshot} node in a {@link SqlValidatorNamespace}.
     *
     * <p>In general, if there is a snapshot expression, the enclosing node of IdentifierNamespace
     * is usually SqlSnapshot. However, if we encounter a situation with an "as" operator, we need
     * to identify whether the enclosingNode is an "as" call and if its first operand is
     * SqlSnapshot.
     * 辅助方法，用于从给定的命名空间中获取 SqlSnapshot 节点。时间旅行查询需要这种快照表达式，以便在特定时间点查询历史数据
     * @param ns The namespace used to find SqlSnapshot
     * @return SqlSnapshot found in {@param ns}, empty if not found
     */
    private Optional<SqlSnapshot> getSnapShotNode(SqlValidatorNamespace ns) {
        if (ns instanceof IdentifierNamespace) {
            SqlNode enclosingNode = ns.getEnclosingNode();
            // FOR SYSTEM_TIME AS OF [expression]
            if (enclosingNode instanceof SqlSnapshot) {
                return Optional.of((SqlSnapshot) enclosingNode);
                // FOR SYSTEM_TIME AS OF [expression] as [identifier]
            } else if (enclosingNode instanceof SqlBasicCall
                    && ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlAsOperator
                    && ((SqlBasicCall) enclosingNode).getOperandList().get(0)
                            instanceof SqlSnapshot) {
                return Optional.of(
                        (SqlSnapshot) ((SqlBasicCall) enclosingNode).getOperandList().get(0));
            }
        }
        return Optional.empty();
    }
    //创建一个 SqlToRelConverter，该转换器负责将 SQL 节点转换为关系表达式（如 RelNode），用于后续的查询优化
    private SqlToRelConverter createSqlToRelConverter() {
        return new SqlToRelConverter(
                toRelContext,
                this,
                this.getCatalogReader().unwrap(CalciteCatalogReader.class),
                relOptCluster,
                frameworkConfig.getConvertletTable(),
                frameworkConfig.getSqlToRelConverterConfig());
    }

    @Override  //该方法用于将列添加到 SELECT 列表中。它包括处理列扩展策略的逻辑。如果定义了列扩展策略，并且列是有效的，它将被添加到 SELECT 列表中；否则，列会被忽略
    protected void addToSelectList(
            List<SqlNode> list,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList,
            SqlNode exp,
            SelectScope scope,
            boolean includeSystemVars) {
        // Extract column's origin to apply strategy
        if (!columnExpansionStrategies.isEmpty() && exp instanceof SqlIdentifier) {
            final SqlQualified qualified = scope.fullyQualify((SqlIdentifier) exp);
            if (qualified.namespace != null && qualified.namespace.getTable() != null) {
                final CatalogSchemaTable schemaTable =
                        (CatalogSchemaTable) qualified.namespace.getTable().table();
                final ResolvedSchema resolvedSchema =
                        schemaTable.getContextResolvedTable().getResolvedSchema();
                final String columnName = qualified.suffix().get(0);
                final Column column = resolvedSchema.getColumn(columnName).orElse(null);
                if (qualified.suffix().size() == 1 && column != null) {
                    if (includeExpandedColumn(column, columnExpansionStrategies)
                            || declaredDescriptorColumn(scope, column)) {
                        super.addToSelectList(
                                list, aliases, fieldList, exp, scope, includeSystemVars);
                    }
                    return;
                }
            }
        }

        // Always add to list
        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    @Override //该方法执行 SQL 节点的无条件重写，处理了窗口表值函数（TVFs）的特殊情况。例如，在 TUMBLE 等窗口函数中，它会重写表表达式，确保必要的描述符列得到正确扩展
    protected @PolyNull SqlNode performUnconditionalRewrites(
            @PolyNull SqlNode node, boolean underFrom) {

        // Special case for window TVFs like:
        // TUMBLE(TABLE t, DESCRIPTOR(metadata_virtual), INTERVAL '1' MINUTE))
        //
        // "TABLE t" is translated into an implicit "SELECT * FROM t". This would ignore columns
        // that are not expanded by default. However, the descriptor explicitly states the need
        // for this column. Therefore, explicit table expressions (for window TVFs at most one)
        // are captured before rewriting and replaced with a "marker" SqlSelect that contains the
        // descriptor information. The "marker" SqlSelect is considered during column expansion.
        final List<SqlIdentifier> explicitTableArgs = getExplicitTableOperands(node);

        final SqlNode rewritten = super.performUnconditionalRewrites(node, underFrom);

        if (!(node instanceof SqlBasicCall)) {
            return rewritten;
        }
        final SqlBasicCall call = (SqlBasicCall) node;
        final SqlOperator operator = call.getOperator();

        if (operator instanceof SqlWindowTableFunction) {
            if (explicitTableArgs.stream().allMatch(Objects::isNull)) {
                return rewritten;
            }

            final List<SqlIdentifier> descriptors =
                    call.getOperandList().stream()
                            .flatMap(FlinkCalciteSqlValidator::extractDescriptors)
                            .collect(Collectors.toList());

            for (int i = 0; i < call.operandCount(); i++) {
                final SqlIdentifier tableArg = explicitTableArgs.get(i);
                if (tableArg != null) {
                    final SqlNode opReplacement = new ExplicitTableSqlSelect(tableArg, descriptors);
                    if (call.operand(i).getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                        // for TUMBLE(DATA => TABLE t3, ...)
                        final SqlCall assignment = call.operand(i);
                        assignment.setOperand(0, opReplacement);
                    } else {
                        // for TUMBLE(TABLE t3, ...)
                        call.setOperand(i, opReplacement);
                    }
                }
                // for TUMBLE([DATA =>] SELECT ..., ...)
            }
        }

        return rewritten;
    }

    // --------------------------------------------------------------------------------------------
    // Column expansion
    // --------------------------------------------------------------------------------------------

    /** 这是一个特殊的 SqlSelect 子类，用于捕获 TVF 操作数中的显式表表达式。它帮助处理 TVF 中表和描述符之间的关系
     * A special {@link SqlSelect} to capture the origin of a {@link SqlKind#EXPLICIT_TABLE} within
     * TVF operands.
     */
    private static class ExplicitTableSqlSelect extends SqlSelect {

        private final List<SqlIdentifier> descriptors;

        public ExplicitTableSqlSelect(SqlIdentifier table, List<SqlIdentifier> descriptors) {
            super(
                    SqlParserPos.ZERO,
                    null,
                    SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
                    table,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            this.descriptors = descriptors;
        }
    }

    /** 该方法检查给定的列是否在 TVF 操作数中的描述符部分声明
     * Returns whether the given column has been declared in a {@link SqlKind#DESCRIPTOR} next to a
     * {@link SqlKind#EXPLICIT_TABLE} within TVF operands.
     */
    private static boolean declaredDescriptorColumn(SelectScope scope, Column column) {
        if (!(scope.getNode() instanceof ExplicitTableSqlSelect)) {
            return false;
        }
        final ExplicitTableSqlSelect select = (ExplicitTableSqlSelect) scope.getNode();
        return select.descriptors.stream()
                .map(SqlIdentifier::getSimple)
                .anyMatch(id -> id.equals(column.getName()));
    }

    /** 该方法识别 TVF 操作数中的显式表表达式，这对于正确重写和验证查询非常重要
     * Returns all {@link SqlKind#EXPLICIT_TABLE} operands within TVF operands. A list entry is
     * {@code null} if the operand is not an {@link SqlKind#EXPLICIT_TABLE}.
     */
    private static List<SqlIdentifier> getExplicitTableOperands(SqlNode node) {
        if (!(node instanceof SqlBasicCall)) {
            return null;
        }
        final SqlBasicCall call = (SqlBasicCall) node;

        if (!(call.getOperator() instanceof SqlFunction)) {
            return null;
        }
        final SqlFunction function = (SqlFunction) call.getOperator();

        if (!isTableFunction(function)) {
            return null;
        }

        return call.getOperandList().stream()
                .map(FlinkCalciteSqlValidator::extractExplicitTable)
                .collect(Collectors.toList());
    }
    //该方法从 SQL 节点中提取描述符（如 DESCRIPTOR(metadata_virtual)），特别是在 TVF 操作数中
    private static @Nullable SqlIdentifier extractExplicitTable(SqlNode op) {
        if (op.getKind() == SqlKind.EXPLICIT_TABLE) {
            final SqlBasicCall opCall = (SqlBasicCall) op;
            if (opCall.operandCount() == 1 && opCall.operand(0) instanceof SqlIdentifier) {
                // for TUMBLE(TABLE t3, ...)
                return opCall.operand(0);
            }
        } else if (op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            // for TUMBLE(DATA => TABLE t3, ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return extractExplicitTable(opCall.operand(0));
        }
        return null;
    }

    private static Stream<SqlIdentifier> extractDescriptors(SqlNode op) {
        if (op.getKind() == SqlKind.DESCRIPTOR) {
            // for TUMBLE(..., DESCRIPTOR(col), ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return opCall.getOperandList().stream()
                    .filter(SqlIdentifier.class::isInstance)
                    .map(SqlIdentifier.class::cast);
        } else if (op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            // for TUMBLE(..., TIMECOL => DESCRIPTOR(col), ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return extractDescriptors(opCall.operand(0));
        }
        return Stream.empty();
    }

    private static boolean isTableFunction(SqlFunction function) {
        return function instanceof SqlTableFunction
                || function.getFunctionType() == SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
    }
}
