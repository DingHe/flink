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

package org.apache.flink.table.runtime.collector;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Collector;

/** The basic implementation of collector for {@link TableFunction}. */
// TableFunctionCollector 是一个专门为 UDTF（用户自定义表函数，User-Defined Table Functions） 设计的收集器基类。
// 在 Flink SQL 中，使用 TableFunction（例如 LATERAL TABLE(my_split_func(a))）时，
// 通常会发生**连接（Join）**操作：左表的一行数据输入给表函数，表函数可能会“炸裂”产生多行输出。
// TableFunctionCollector 的核心作用是：
// 持有上下文：它不仅负责收集 UDTF 产生的结果，还持有了产生这些结果的“原始左表行”（input row）。
// 连接左表与右表：它是左表数据与 UDTF 展开后的数据进行 Correlate（关联）计算的桥梁。
// 状态追踪：追踪表函数是否针对当前的输入行产生了任何输出（用于处理像 LEFT JOIN LATERAL 这种即使右边没数据也要输出左边行的情况）。

public abstract class TableFunctionCollector<T> extends AbstractRichFunction
        implements Collector<T> {

    private static final long serialVersionUID = 1L;
    // 存储来自“左表”的当前输入行数据。
    private Object input;
    // 算子真正的输出收集器。
    // TableFunctionCollector 接收到数据后，经过一定的逻辑处理（如行拼装），最终通过这个 collector 发送给下一个算子。
    private Collector collector;
    // 记录当前的 input 是否已经触发过至少一次 collect 调用
    private boolean collected;

    /**
     * Sets the input row from left table, which will be used to cross join with the result of table
     * function.
     */
    public void setInput(Object input) {
        this.input = input;
    }

    /**
     * Gets the input value from left table, which will be used to cross join with the result of
     * table function.
     */
    public Object getInput() {
        return input;
    }

    /** Sets the current collector, which used to emit the final row. */
    public void setCollector(Collector<?> collector) {
        this.collector = collector;
    }

    /** Resets the flag to indicate whether [[collect(T)]] has been called. */
    // 在处理每一条新的左表输入行之前被调用。它将 collected 标记重置为 false，开启一轮新的状态追踪。
    public void reset() {
        this.collected = false;
    }

    /** Output final result of this UDTF to downstreams. */
    @SuppressWarnings("unchecked")
    public void outputResult(Object result) {
        this.collected = true;
        this.collector.collect(result);
    }

    /**
     * Whether {@link #collect(Object)} has been called.
     *
     * @return True if {@link #collect(Object)} has been called.
     */
    public boolean isCollected() {
        return collected;
    }

    public void close() {
        this.collector.close();
    }
}
