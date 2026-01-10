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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.LookupJoinHintOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.runtime.functions.table.lookup.CachingAsyncLookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.CachingLookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.CacheLoader;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.LookupFullCache;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.inputformat.InputFormatCacheLoader;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableLookupFunctionDelegator;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexLiteral;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_CAPACITY;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_LOOKUP;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_OUTPUT_MODE;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_TIMEOUT;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.FIXED_DELAY;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.MAX_ATTEMPTS;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.RETRY_PREDICATE;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.RETRY_STRATEGY;
import static org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy.NO_RETRY_STRATEGY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for lookup joins using {@link LookupTableSource}. */
// 专门用于处理 Lookup Join（维表关联） 的优化与运行时构建。
// 它连接了 SQL 解析层（Calcite）与 Flink 运行时层，负责根据用户定义的 SQL、Hint 以及底层 Connector 的能力，决定如何执行维表查询。
// 策略选择：决定使用**同步（Sync）还是异步（Async）**模式查询维表。
// 配置合并：将 Flink 全局配置（TableConfig）与 SQL 语句中的 Join Hint（如重试策略、异步参数）进行合并。
// 运行时转换：将逻辑执行计划中的 Lookup 信息转换为运行时可执行的 LookupFunction 或 AsyncLookupFunction。


@Internal
public final class LookupJoinUtil {

    /** A field used as an equal condition when querying content from a dimension table. */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ConstantLookupKey.class),
        @JsonSubTypes.Type(value = FieldRefLookupKey.class)
    })
    // 代表维表查询的一个等值条件
    public static class LookupKey {
        private LookupKey() {
            // sealed class
        }
    }

    /** A {@link LookupKey} whose value is constant. */
    // 专门用于处理 Lookup Join 条件中的常量等值过滤
    // 在 Flink SQL 的维表关联中，Join 条件不仅可以是字段关联（如 ON s.id = d.id），还可以包含常量过滤（如 ON s.id = d.id AND d.status = 'ACTIVE'）
    // ConstantLookupKey 的作用就是在逻辑计划阶段存储这个硬编码的常量值（如上述例子中的 'ACTIVE'）。
    // 当维表算子在运行时拼接查询请求时，它会从这个类中读取常量值，直接作为查询过滤条件发送给外部数据库。
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("Constant")
    public static class ConstantLookupKey extends LookupKey {
        public static final String FIELD_NAME_SOURCE_TYPE = "sourceType";
        public static final String FIELD_NAME_LITERAL = "literal";
        // 存储该常量的逻辑类型（如 INT, VARCHAR, BOOLEAN 等）
        // 在维表查询时，类型匹配非常重要。例如，外部数据库可能对 123（数值）和 '123'（字符串）的处理逻辑完全不同。
        @JsonProperty(FIELD_NAME_SOURCE_TYPE)
        public final LogicalType sourceType;
        // 存储具体的常量值
        // RexLiteral 是 Calcite 框架中的类，代表一个行表达式（Row Expression）中的字面量。
        // 它不仅包含值，还包含了该值在 SQL 语法层面的元数据
        @JsonProperty(FIELD_NAME_LITERAL)
        public final RexLiteral literal;

        @JsonCreator
        public ConstantLookupKey(
                @JsonProperty(FIELD_NAME_SOURCE_TYPE) LogicalType sourceType,
                @JsonProperty(FIELD_NAME_LITERAL) RexLiteral literal) {
            this.sourceType = sourceType;
            this.literal = literal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConstantLookupKey that = (ConstantLookupKey) o;
            return Objects.equals(sourceType, that.sourceType)
                    && Objects.equals(literal, that.literal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceType, literal);
        }
    }

    /** A {@link LookupKey} whose value comes from the left table field. */
    // 专门用于处理 Lookup Join 条件中最常见的场景：左表（流表）字段与维表字段的等值关联。
    // 在 SQL 中执行 JOIN dim ON s.user_id = dim.id 时，关联的依据是流表中的 user_id 字段。
    // FieldRefLookupKey 的作用就是在逻辑计划阶段记录左表字段的索引位置。在运行时，维表算子会根据这个索引，从流入的每一行数据（RowData）中提取出具体的值，作为 Key 去查询外部维表。
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("FieldRef")
    public static class FieldRefLookupKey extends LookupKey {
        public static final String FIELD_NAME_INDEX = "index";

        // 该类的核心数据。它代表了左表（输入流）中参与关联的字段下标（从 0 开始）。
        // 如果流表的 Schema 是 (order_id, user_id, amount)，而 Join 条件是 ON s.user_id = d.id，那么此处的 index 就是 1。
        @JsonProperty(FIELD_NAME_INDEX)
        public final int index;

        @JsonCreator
        public FieldRefLookupKey(@JsonProperty(FIELD_NAME_INDEX) int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldRefLookupKey that = (FieldRefLookupKey) o;
            return index == that.index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }
    }

    /** AsyncLookupOptions includes async related options. */
    // 专门用于承载和传递 异步维表关联（Async Lookup Join） 的核心配置参数。
    // 当用户在 SQL 中开启异步查询（例如通过 Hint /*+ LOOKUP('table'='dim', 'async'='true') */）时，Flink 需要一套参数来控制底层 AsyncWaitOperator 的行为。
    // AsyncLookupOptions 的作用就是统一封装这些异步执行策略，确保它们能从 SQL 层正确传递到运行时算子。
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("AsyncOptions")
    public static class AsyncLookupOptions {
        public static final String FIELD_NAME_CAPACITY = "capacity ";
        public static final String FIELD_NAME_TIMEOUT = "timeout";
        public static final String FIELD_NAME_OUTPUT_MODE = "output-mode";
        // 异步 I/O 的最大并发请求数
        // 它定义了允许同时处于“在途（In-flight）”状态的请求数量。
        // 当达到此阈值时，算子会产生反压，停止接收新数据，直到前面的请求完成。
        @JsonProperty(FIELD_NAME_CAPACITY)
        public final int asyncBufferCapacity;
        // 异步请求的超时时间（单位：毫秒）
        @JsonProperty(FIELD_NAME_TIMEOUT)
        public final long asyncTimeout;
        // 异步结果的输出模式。
        // ORDERED（有序）：强制下游接收数据的顺序与上游流入顺序一致。这通常涉及内部缓存排序，延迟稍高。
        // UNORDERED（无序）：哪个请求先完成，哪个结果就先发往下游。性能最高，但会改变数据流的顺序（仅在不影响语义的情况下使用）。
        @JsonProperty(FIELD_NAME_OUTPUT_MODE)
        public final AsyncDataStream.OutputMode asyncOutputMode;

        @JsonCreator
        public AsyncLookupOptions(
                @JsonProperty(FIELD_NAME_CAPACITY) int asyncBufferCapacity,
                @JsonProperty(FIELD_NAME_TIMEOUT) long asyncTimeout,
                @JsonProperty(FIELD_NAME_OUTPUT_MODE) AsyncDataStream.OutputMode asyncOutputMode) {
            this.asyncBufferCapacity = asyncBufferCapacity;
            this.asyncTimeout = asyncTimeout;
            this.asyncOutputMode = asyncOutputMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AsyncLookupOptions that = (AsyncLookupOptions) o;
            return asyncBufferCapacity == that.asyncBufferCapacity
                    && asyncTimeout == that.asyncTimeout
                    && asyncOutputMode == that.asyncOutputMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(asyncBufferCapacity, asyncTimeout, asyncOutputMode);
        }

        @Override
        public String toString() {
            return asyncOutputMode + ", " + asyncTimeout + "ms, " + asyncBufferCapacity;
        }
    }

    /** RetryOptions includes retry lookup related options. */
    // 定义并存储当 Lookup 查询未命中或失败时的补救策略。它负责从 SQL Hint 中解析重试参数，并在运行时将其转化为具体的 ResultRetryStrategy（执行策略），交给维表算子使用。
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("RetryLookupOptions")
    public static class RetryLookupOptions {
        public static final String FIELD_NAME_RETRY_PREDICATE = "retry-predicate";
        public static final String FIELD_NAME_RETRY_STRATEGY = "retry-strategy";
        public static final String FIELD_NAME_RETRY_FIXED_DELAY = "fixed-delay";
        public static final String FIELD_NAME_RETRY_MAX_ATTEMPTS = "max-attempts";

        // 重试断言（触发条件）
        // 决定在什么情况下触发重试。目前主要支持 LOOKUP_MISS（即维表查询结果为空时进行重试）。
        @JsonProperty(FIELD_NAME_RETRY_PREDICATE)
        private final String retryPredicate;

        // 重试策略类型。
        // 目前主要实现为 FIXED_DELAY（固定延迟重试）
        @JsonProperty(FIELD_NAME_RETRY_STRATEGY)
        private final LookupJoinHintOptions.RetryStrategy retryStrategy;
        // 重试间隔时间。
        // 两次重试尝试之间的等待时间（毫秒）
        @JsonProperty(FIELD_NAME_RETRY_FIXED_DELAY)
        private final Long retryFixedDelay;
        // 最大重试次数。
        // 如果超过这个次数仍然没有查询到结果，则停止重试，按未命中处理。
        @JsonProperty(FIELD_NAME_RETRY_MAX_ATTEMPTS)
        private final Integer retryMaxAttempts;

        @JsonCreator
        public RetryLookupOptions(
                @JsonProperty(FIELD_NAME_RETRY_PREDICATE) String retryPredicate,
                @JsonProperty(FIELD_NAME_RETRY_STRATEGY)
                        LookupJoinHintOptions.RetryStrategy retryStrategy,
                @JsonProperty(FIELD_NAME_RETRY_FIXED_DELAY) Long retryFixedDelay,
                @JsonProperty(FIELD_NAME_RETRY_MAX_ATTEMPTS) Integer retryMaxAttempts) {
            this.retryPredicate = checkNotNull(retryPredicate);
            this.retryStrategy = checkNotNull(retryStrategy);
            this.retryFixedDelay = checkNotNull(retryFixedDelay);
            this.retryMaxAttempts = checkNotNull(retryMaxAttempts);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RetryLookupOptions that = (RetryLookupOptions) o;
            return Objects.equals(retryPredicate, that.retryPredicate)
                    && retryStrategy == that.retryStrategy
                    && Objects.equals(retryFixedDelay, that.retryFixedDelay)
                    && Objects.equals(retryMaxAttempts, that.retryMaxAttempts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(retryPredicate, retryStrategy, retryFixedDelay, retryMaxAttempts);
        }

        @Override
        public String toString() {
            return retryPredicate
                    + ", "
                    + retryStrategy
                    + ", "
                    + retryFixedDelay
                    + "ms, "
                    + retryMaxAttempts;
        }
        // 将 Calcite 里的 RelHint（SQL 里的 /*+ LOOKUP(...) */）转换为 Java 对象。
        // 它通过 Flink 的 Configuration 工具类读取 Hint 里的键值对，并特别处理了 Duration 到 long（毫秒）的转换。
        @Nullable
        public static RetryLookupOptions fromJoinHint(@Nullable RelHint lookupJoinHint) {
            if (null != lookupJoinHint) {
                Configuration conf = Configuration.fromMap(lookupJoinHint.kvOptions);
                Duration fixedDelay = conf.get(FIXED_DELAY);
                if (fixedDelay != null) {
                    return new RetryLookupOptions(
                            conf.get(RETRY_PREDICATE),
                            conf.get(RETRY_STRATEGY),
                            fixedDelay.toMillis(),
                            conf.get(MAX_ATTEMPTS));
                }
            }
            return null;
        }

        /**
         * Convert this {@link RetryLookupOptions} to {@link ResultRetryStrategy} in the best effort
         * manner. If invalid {@link LookupJoinHintOptions#RETRY_PREDICATE} or {@link
         * LookupJoinHintOptions#RETRY_STRATEGY} is given, then {@link
         * ResultRetryStrategy#NO_RETRY_STRATEGY} will return.
         */
        // 将配置对象转化为运行时真正执行的 ResultRetryStrategy
        @JsonIgnore
        @SuppressWarnings("unchecked")
        public ResultRetryStrategy toRetryStrategy() {
            if (!LookupJoinHintOptions.LOOKUP_MISS_PREDICATE.equalsIgnoreCase(retryPredicate)
                    || retryStrategy != LookupJoinHintOptions.RetryStrategy.FIXED_DELAY) {
                return NO_RETRY_STRATEGY;
            }
            // retry option values have been validated by hint checker
            return ResultRetryStrategy.fixedDelayRetry(
                    this.retryMaxAttempts,
                    this.retryFixedDelay,
                    RetryPredicates.EMPTY_RESULT_PREDICATE);
        }
    }

    private LookupJoinUtil() {
        // no instantiation
    }

    /** Gets lookup keys sorted by index in ascending order. */
    // 在 Flink SQL 的维表关联中，关联条件（Join Keys）可能以无序的方式被识别（例如在 SQL 中写 ON s.b = d.b AND s.a = d.a）。
    // 为了确保执行计划的确定性（Determinism）以及在底层生成代码、序列化和状态存储时能够一致地处理字段，
    // Flink 需要将这些 Key 按照它们在流表 Schema 中的原始索引位置进行排序。
    // 此方法的作用就是将输入的字段索引集合转换为一个升序排列的整数数组。
    public static int[] getOrderedLookupKeys(Collection<Integer> allLookupKeys) {
        List<Integer> lookupKeyIndicesInOrder = new ArrayList<>(allLookupKeys);
        // 升序排序
        lookupKeyIndicesInOrder.sort(Integer::compareTo);
        // 返回一个更轻量、更高性能的原生数组，方便后续逻辑（如数组循环）使用
        return lookupKeyIndicesInOrder.stream().mapToInt(Integer::intValue).toArray();
    }

    // 实现了 SQL Hint、全局配置与数据流特性（ChangelogMode）的三方合并。
    // 确定异步查询维表时，最终生效的并发度、超时时间和输出模式。
    public static AsyncLookupOptions getMergedAsyncOptions(
            RelHint lookupHint, TableConfig config, ChangelogMode inputChangelogMode) {
        Configuration confFromHint;
        // 检查 SQL 语句中是否写了 /*+ LOOKUP(...) */ 这种 Hint。
        if (lookupHint == null) {
            confFromHint = new Configuration();
        } else {
            confFromHint = Configuration.fromMap(lookupHint.kvOptions);
        }
        // 构造并返回合并后的选项
        return new AsyncLookupOptions(
                coalesce(
                        confFromHint.get(ASYNC_CAPACITY),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY)),
                coalesce(
                                confFromHint.get(ASYNC_TIMEOUT),
                                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT))
                        .toMillis(),
                convert(
                        inputChangelogMode,
                        coalesce(
                                confFromHint.get(ASYNC_OUTPUT_MODE),
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE))));
    }

    /**
     * This method determines whether async lookup is enabled according to the given lookup keys
     * with considering lookup {@link RelHint} and required upsertMaterialize. Note: it will not
     * create the function instance to avoid potential heavy cost during optimization phase. if
     * required upsertMaterialize is true, will return synchronous lookup function only, otherwise
     * prefers asynchronous lookup function except there's a hint option 'async' = 'false', will
     * raise an error if both candidates not found.
     *
     * <pre>{@code
     * 1. if upsertMaterialize == true : return false
     *
     * 2. preferAsync = except there is a hint option 'async' = 'false'
     *  if (preferAsync) {
     *    return asyncFound ? true : false
     *  } else {
     *    return syncFound ? false : true
     *  }
     * }</pre>
     */
    // 在不真正实例化算子的情况下，预判当前维表关联是否应该采用“异步模式”执行。
    // 在 Flink 优化阶段，优化器需要知道一个 Join 算子是同步的还是异步的，以便决定下游的物理计划（如是否需要 AsyncWaitOperator）。
    // 但由于创建真正的 LookupFunction 可能涉及连接数据库等重操作，因此该方法通过检查接口类型而非创建实例来完成判定。
    public static boolean isAsyncLookup(
            RelOptTable temporalTable,
            Collection<Integer> lookupKeys,
            RelHint lookupHint,
            boolean upsertMaterialize) {
        // prefer (not require) by default
        // 读取 SQL Hint（如 /*+ LOOKUP('table'='dim', 'async'='true') */）
        boolean preferAsync = preferAsync(lookupHint);
        // 目前的 Flink 实现中，异步查找与这种特定的物化机制不兼容。如果必须物化，则强制返回同步模式。
        // 如果该 Join 算子下游需要进行 upsertMaterialize（物化处理，通常用于处理回撤流以保证数据正确性）。
        if (upsertMaterialize) {
            // upsertMaterialize only works on sync lookup mode, async lookup is unsupported.
            return false;
        }
        boolean syncFound = false;
        boolean asyncFound = false;
        // 针对实现了 DynamicTableSource 的现代 Connector
        if (temporalTable instanceof TableSourceTable) {
            int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
            LookupTableSource.LookupRuntimeProvider provider =
                    createLookupRuntimeProvider(temporalTable, lookupKeyIndicesInOrder);
            // 说明 Connector 具备同步查询能力
            if (provider instanceof LookupFunctionProvider
                    || provider instanceof TableFunctionProvider) {
                syncFound = true;
            }
            // 说明 Connector 具备异步查询能力
            if (provider instanceof AsyncLookupFunctionProvider
                    || provider instanceof AsyncTableFunctionProvider) {
                asyncFound = true;
            }
            // 检查旧版 Connector (LegacyTableSourceTable)
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            LegacyTableSourceTable<?> legacyTableSourceTable =
                    (LegacyTableSourceTable<?>) temporalTable;
            LookupableTableSource<?> tableSource =
                    (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
            if (tableSource.isAsyncEnabled()) {
                asyncFound = true;
            } else {
                syncFound = true;
            }
        }
        if (!syncFound && !asyncFound) {
            throw new TableException(
                    String.format(
                            "table %s is neither TableSourceTable not LegacyTableSourceTable",
                            temporalTable.getQualifiedName()));
        }
        return preferAsync ? asyncFound : !syncFound;
    }

    /**
     * Gets required lookup function (async or sync) from temporal table , will raise an error if
     * specified lookup function instance not found.
     */
    // 主要作用是从维表（Temporal Table）中提取具体的查找函数（同步或异步），并将其交给算子去执行。
    public static UserDefinedFunction getLookupFunction(
            RelOptTable temporalTable,// 代表维表的逻辑表对象
            Collection<Integer> lookupKeys,// SQL Join 条件中关联键在维表中的索引集合
            ClassLoader classLoader,// 用于加载用户定义类的类加载器
            boolean async,// 是否需要异步查找函数（由优化器根据配置决定）
            ResultRetryStrategy retryStrategy) { // 查找失败时的重试策略
        UserDefinedFunction lookupFunction = null;
        // 将无序的关联键集合转换为有序的整型数组。
        int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
        // 现代 Dynamic Table Source (新 API)
        if (temporalTable instanceof TableSourceTable) {
            lookupFunction =
                    findLookupFunctionFromNewSource(
                            (TableSourceTable) temporalTable,
                            lookupKeyIndicesInOrder,
                            retryStrategy,
                            async,
                            classLoader);
        // Legacy Table Source (旧版 API)
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            lookupFunction =
                    findLookupFunctionFromLegacySource(
                            (LegacyTableSourceTable<?>) temporalTable,
                            lookupKeyIndicesInOrder,
                            async);
        }
        if (null == lookupFunction) {
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Required ")
                    .append(async ? "async" : "sync")
                    .append(" lookup function by planner, but table ")
                    .append(temporalTable.getQualifiedName())
                    .append(
                            "does not offer a valid lookup function neither as TableSourceTable nor LegacyTableSourceTable");
            throw new TableException(errorMsg.toString());
        }
        return lookupFunction;
    }

    /**
     * Evaluates if prefer async lookup by given lookup {@link RelHint}. Returns true except async
     * option in hint is false.
     */
    private static boolean preferAsync(@Nullable RelHint lookupHint) {
        // async option has no default value, prefer async except async option is false
        if (null == lookupHint) {
            return true;
        }
        Configuration conf = Configuration.fromMap(lookupHint.kvOptions);
        Boolean async = conf.get(ASYNC_LOOKUP);
        return null == async || async;
    }

    private static <T> T coalesce(T t1, T t2) {
        return t1 != null ? t1 : t2;
    }

    private static AsyncDataStream.OutputMode convert(
            ChangelogMode inputChangelogMode,
            ExecutionConfigOptions.AsyncOutputMode asyncOutputMode) {
        if (inputChangelogMode.containsOnly(RowKind.INSERT)
                && asyncOutputMode == ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED) {
            return AsyncDataStream.OutputMode.UNORDERED;
        }
        return AsyncDataStream.OutputMode.ORDERED;
    }

    /**
     * Wraps LookupFunction into a RetryableLookupFunctionDelegator to support retry. Note: only
     * LookupFunction is supported.
     */
    private static LookupFunction wrapSyncRetryDelegator(
            LookupFunctionProvider provider, ResultRetryStrategy retryStrategy) {
        if (retryStrategy != null && retryStrategy != NO_RETRY_STRATEGY) {
            return new RetryableLookupFunctionDelegator(
                    provider.createLookupFunction(), retryStrategy);
        }
        return provider.createLookupFunction();
    }

    /**
     * Wraps AsyncLookupFunction into a RetryableAsyncLookupFunctionDelegator to support retry.
     * Note: only AsyncLookupFunction is supported.
     */
    private static AsyncLookupFunction wrapASyncRetryDelegator(
            AsyncLookupFunctionProvider provider, ResultRetryStrategy retryStrategy) {
        if (retryStrategy != null && retryStrategy != NO_RETRY_STRATEGY) {
            return new RetryableAsyncLookupFunctionDelegator(
                    provider.createAsyncLookupFunction(), retryStrategy);
        }
        return provider.createAsyncLookupFunction();
    }
    // 将 LookupRuntimeProvider 转化为具体的 用户自定义函数 (UDF) 的核心逻辑。它负责处理同步/异步、缓存（全量/部分）以及重试策略的包装。
    private static UserDefinedFunction findLookupFunctionFromNewSource(
            TableSourceTable temporalTable,
            int[] lookupKeyIndicesInOrder,
            ResultRetryStrategy retryStrategy,
            boolean async,
            ClassLoader classLoader) {
        // provider 包含了 Connector 实现类（如 Hive、JDBC）提供的原始查找逻辑
        LookupTableSource.LookupRuntimeProvider provider =
                createLookupRuntimeProvider(temporalTable, lookupKeyIndicesInOrder);
        // 处理异步模式
        if (async) {
            if (provider instanceof AsyncLookupFunctionProvider) {
                // 如果 Provider 开启了缓存，则创建一个 CachingAsyncLookupFunction，将缓存对象和包装了重试逻辑的异步查找函数传入
                if (provider instanceof PartialCachingAsyncLookupProvider) {
                    PartialCachingAsyncLookupProvider partialCachingLookupProvider =
                            (PartialCachingAsyncLookupProvider) provider;
                    return new CachingAsyncLookupFunction(
                            partialCachingLookupProvider.getCache(),
                            wrapASyncRetryDelegator(partialCachingLookupProvider, retryStrategy));
                } else {
                    return wrapASyncRetryDelegator(
                            (AsyncLookupFunctionProvider) provider, retryStrategy);
                }
            }
            if (provider instanceof AsyncTableFunctionProvider) {
                return ((AsyncTableFunctionProvider<?>) provider).createAsyncTableFunction();
            }
        } else {
            if (provider instanceof LookupFunctionProvider) {
                if (provider instanceof PartialCachingLookupProvider) {
                    PartialCachingLookupProvider partialCachingLookupProvider =
                            (PartialCachingLookupProvider) provider;
                    return new CachingLookupFunction(
                            partialCachingLookupProvider.getCache(),
                            wrapSyncRetryDelegator(partialCachingLookupProvider, retryStrategy));
                } else if (provider instanceof FullCachingLookupProvider) {
                    FullCachingLookupProvider fullCachingLookupProvider =
                            (FullCachingLookupProvider) provider;
                    RowType tableSourceRowType =
                            FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
                    LookupFullCache fullCache =
                            createFullCache(
                                    fullCachingLookupProvider,
                                    lookupKeyIndicesInOrder,
                                    classLoader,
                                    tableSourceRowType);
                    // retry on fullCachingLookupFunction is meaningless
                    return new CachingLookupFunction(
                            fullCache, fullCachingLookupProvider.createLookupFunction());
                } else {
                    return wrapSyncRetryDelegator((LookupFunctionProvider) provider, retryStrategy);
                }
            }
            if (provider instanceof TableFunctionProvider) {
                return ((TableFunctionProvider<?>) provider).createTableFunction();
            }
        }
        return null;
    }

    private static UserDefinedFunction findLookupFunctionFromLegacySource(
            LegacyTableSourceTable temporalTable, int[] lookupKeyIndicesInOrder, boolean async) {
        String[] lookupFieldNamesInOrder =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(temporalTable.getRowType().getFieldNames()::get)
                        .toArray(String[]::new);
        LegacyTableSourceTable<?> legacyTableSourceTable =
                (LegacyTableSourceTable<?>) temporalTable;
        LookupableTableSource<?> tableSource =
                (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
        // respect the definition of LookupableTableSource#isAsyncEnabled
        if (async && tableSource.isAsyncEnabled()) {
            return tableSource.getAsyncLookupFunction(lookupFieldNamesInOrder);
        }
        if (!async && !tableSource.isAsyncEnabled()) {
            return tableSource.getLookupFunction(lookupFieldNamesInOrder);
        }
        return null;
    }
    // Flink SQL 优化器在处理 Lookup Join（维表关联）时的一个核心工具方法
    // 作用是根据 SQL 关联条件，从维表（Temporal Table）中提取出能够实际执行查找操作的运行实例（Provider）。
    private static LookupTableSource.LookupRuntimeProvider createLookupRuntimeProvider(
            RelOptTable temporalTable, int[] lookupKeyIndicesInOrder) {
        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        // 一维整型数组，存储了 SQL 中 ON 条件里对应维表的字段索引（例如 ON a.id = b.id，则存储了 b.id 在维表中的位置）
        int[][] indices =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(i -> new int[] {i})
                        .toArray(int[][]::new);
        // 在 Calcite（Flink 使用的 SQL 优化引擎）中，维表被表示为 RelOptTable。
        // 首先将其强制转换为 TableSourceTable，这是 Flink 对 Calcite 表对象的封装。
        LookupTableSource tableSource =
                (LookupTableSource) ((TableSourceTable) temporalTable).tableSource();
        LookupRuntimeProviderContext providerContext = new LookupRuntimeProviderContext(indices);
        // 调用了具体数据源实现类（如之前提到的 HiveLookupTableSource）的方法。
        return tableSource.getLookupRuntimeProvider(providerContext);
    }

    private static LookupFullCache createFullCache(
            FullCachingLookupProvider provider,
            int[] lookupKeyIndicesInOrder,
            ClassLoader classLoader,
            RowType tableSourceRowType) {

        ScanTableSource.ScanRuntimeProvider scanProvider = provider.getScanRuntimeProvider();
        Preconditions.checkArgument(
                scanProvider.isBounded(),
                "ScanRuntimeProvider that is used for data loading in "
                        + "lookup 'FULL' cache must be bounded.");

        GenericRowDataKeySelector lookupTableKeySelector =
                (GenericRowDataKeySelector)
                        KeySelectorUtil.getRowDataSelector(
                                classLoader,
                                lookupKeyIndicesInOrder,
                                InternalTypeInfo.of(tableSourceRowType),
                                GenericRowData.class);

        if (scanProvider instanceof InputFormatProvider) {
            InputFormat<RowData, ?> inputFormat =
                    ((InputFormatProvider) scanProvider).createInputFormat();
            CacheLoader cacheLoader =
                    new InputFormatCacheLoader(
                            inputFormat,
                            lookupTableKeySelector,
                            InternalSerializers.create(tableSourceRowType));
            return new LookupFullCache(cacheLoader, provider.getCacheReloadTrigger());
        } else if (scanProvider instanceof SourceFunctionProvider) {
            // TODO support SourceFunctions
            throw new UnsupportedOperationException(
                    "Full caching using SourceFunction currently not supported.");
        } else {
            throw new UnsupportedOperationException(
                    "Currently only InputFormatProvider and SourceFunctionProvider are supported as ScanRuntimeProviders for Full caching lookup join.");
        }
    }
}
