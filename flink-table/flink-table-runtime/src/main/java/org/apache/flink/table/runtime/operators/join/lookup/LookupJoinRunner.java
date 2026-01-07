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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

/** The join runner to lookup the dimension table. */
// LookupJoinRunner 是实现 维表关联（Lookup Join） 的核心执行器。
// 它是一个 ProcessFunction，负责处理流式数据并实时查询外部存储（如 MySQL, HBase, Redis 等）。
// 在 SQL 中执行类似 SELECT ... FROM LeftTable JOIN DimensionTable FOR SYSTEM_TIME AS OF ... 时，
// Flink 不会把维表全部加载到内存，而是每来一条左表数据，就通过 LookupJoinRunner 去外部查询一次。
// 核心职责包括：
// 管理动态生成代码：它持有之前提到的 GeneratedFunction 等，在运行时将它们实例化为具体的查询逻辑。
// 执行查询流程：控制“预过滤 -> 外部查询 -> 结果关联 -> 输出”的完整生命周期。
// 处理 Join 类型：支持 Inner Join 和 Left Outer Join（补空值逻辑）。
// 连接前后端：将输入的 RowData 传递给维表查询器，并将查到的结果与原行进行拼接。

public class LookupJoinRunner extends ProcessFunction<RowData, RowData> {
    private static final long serialVersionUID = -4521543015709964733L;
    // 封装了 FlatMapFunction 的代码，负责从外部系统读取数据（Fetch）。
    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher;
    // 封装了 ListenableCollector 的代码，用于接收查询结果并与左表行合并。
    private final GeneratedCollector<ListenableCollector<RowData>> generatedCollector;
    // 封装了 FilterCondition，用于在查询外部系统前先在本地做一次过滤（减少无效查询）。
    private final GeneratedFunction<FilterCondition> generatedPreFilterCondition;
    // 如果是 true，当维表查不到数据时，会输出一个右侧全为 NULL 的行；
    // 如果是 false（Inner Join），则直接丢弃该行。
    protected final boolean isLeftOuterJoin;
    // 维表字段的数量。
    // 用于在 Left Join 查不到数据时构造等长的 NULL 行。
    protected final int tableFieldsCount;
    // 实例化后的对象，真正执行 lookup 操作
    private transient FlatMapFunction<RowData, RowData> fetcher;
    // 实例化后的对象，负责收集查询结果并追踪是否已经产生过输出。
    protected transient ListenableCollector<RowData> collector;
    // 可复用的容器，用于把左表行和维表行拼接在一起输出。
    protected transient JoinedRowData outRow;
    // 实例化后的过滤条件
    protected transient FilterCondition preFilterCondition;
    // 预先创建好的全 NULL 行，用于 Left Join 补位。
    protected transient GenericRowData nullRow;

    public LookupJoinRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedCollector<ListenableCollector<RowData>> generatedCollector,
            GeneratedFunction<FilterCondition> generatedPreFilterCondition,
            boolean isLeftOuterJoin,
            int tableFieldsCount) {
        this.generatedFetcher = generatedFetcher;
        this.generatedCollector = generatedCollector;
        this.generatedPreFilterCondition = generatedPreFilterCondition;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.tableFieldsCount = tableFieldsCount;
    }
    // 初始化方法，在 TaskManager 启动时调用一次
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.collector =
                generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.preFilterCondition =
                generatedPreFilterCondition.newInstance(
                        getRuntimeContext().getUserCodeClassLoader());

        // 为这些生成的函数设置 RuntimeContext 并调用它们的 open 方法（初始化连接、加载资源等）。
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(preFilterCondition, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);
        FunctionUtils.openFunction(collector, openContext);
        FunctionUtils.openFunction(preFilterCondition, openContext);
        // 初始化 nullRow 和 outRow 实例
        this.nullRow = new GenericRowData(tableFieldsCount);
        this.outRow = new JoinedRowData();
    }
    // 每来一条左表数据，都会进入此方法。
    // 这是整个 Join 的主控逻辑
    @Override
    public void processElement(RowData in, Context ctx, Collector<RowData> out) throws Exception {
        // 设置当前输出通道
        prepareCollector(in, out);

        // apply local filter first
        // 判断当前行是否满足查询条件
        if (preFilter(in)) {
            doFetch(in);
        }

        padNullForLeftJoin(in, out);
    }
    // 将下游真实的 out 交给 collector，并记下当前的输入行 in，最后 reset 状态标记位（collected = false）
    public void prepareCollector(RowData in, Collector<RowData> out) {
        collector.setCollector(out);
        collector.setInput(in);
        collector.reset();
    }

    // 执行 preFilterCondition.apply(in)。
    // 如果返回 false，则这一行不会去请求外部数据库。

    public boolean preFilter(RowData in) throws Exception {
        return preFilterCondition.apply(in);
    }
    // 去外部维表查数据
    public void doFetch(RowData in) throws Exception {
        // fetcher has copied the input field when object reuse is enabled
        // fetcher 会发出查询请求，每查到一行，都会调用一次 getFetcherCollector() 的 collect 方法。
        fetcher.flatMap(in, getFetcherCollector());
    }
    // 处理 Left Join 的边界情况
    // 如果配置了 isLeftOuterJoin 且直到这一步 collector 依然反馈“我没收到任何维表数据”（isCollected() == false），
    // 则强制把 in 和 nullRow 拼在一起发给下游。
    public void padNullForLeftJoin(RowData in, Collector<RowData> out) {
        if (isLeftOuterJoin && !collector.isCollected()) {
            outRow.replace(in, nullRow);
            outRow.setRowKind(in.getRowKind());
            out.collect(outRow);
        }
    }

    public Collector<RowData> getFetcherCollector() {
        return collector;
    }

    @Override
    public void close() throws Exception {
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (collector != null) {
            FunctionUtils.closeFunction(collector);
        }
        if (preFilterCondition != null) {
            FunctionUtils.closeFunction(preFilterCondition);
        }
        super.close();
    }
}
