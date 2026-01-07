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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;

/**
 * An {@link ExecNodeTranslator} is responsible for translating an {@link ExecNode} to {@link
 * Transformation}s.
 *
 * @param <T> The type of the elements that result from this translator.
 */
// 连接 逻辑计划层（Planning） 与 运行运行时层（Runtime） 的关键接口。
// 在 Flink SQL 解析和优化的过程中，SQL 语句会经历多个阶段的转换。ExecNode（执行节点）是优化器输出的最终结果，它描述了“要做什么”。
// 而 Transformation 是 Flink DataStream API 的底层表达，描述了“如何在集群上执行”。
// 核心作用是：
// 翻译转换：将一个抽象的执行节点（ExecNode）翻译成具体的 Flink 算子链（Transformation）。
// 桥接角色：它是 Flink Table Planner（负责 SQL 逻辑）与 Flink Runtime（负责数据流执行）之间的“翻译官”。
// 幂等性保证：确保同一个节点无论被调用多少次，生成的执行计划都是一致的，这对于复杂的 DAG（有向无环图）结构至关重要。

@Internal
public interface ExecNodeTranslator<T> {

    /**
     * Translates this node into a {@link Transformation}.
     *
     * <p>NOTE: This method should return same translate result if called multiple times.
     *
     * @param planner The {@link Planner} of the translated graph.
     */
    Transformation<T> translateToPlan(Planner planner);
}
