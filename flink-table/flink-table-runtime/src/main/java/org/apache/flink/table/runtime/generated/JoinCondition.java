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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.table.data.RowData;

/** Interface for code generated condition function for [[org.apache.calcite.rel.core.Join]]. */
// JoinCondition 接口是 Flink Table/SQL 运行时内部使用的，用于表示和执行连接操作（Join）中的过滤条件
// 在 Flink SQL 或 Table API 执行连接操作（JOIN）时，通常有两个阶段：
// 键匹配（Key Matching）： 根据 ON 子句中的等值条件（例如 t1.id = t2.id）找到候选匹配行。
// 非键条件过滤（Non-Key Condition Filtering）： 对找到的候选匹配行应用 ON 子句中剩余的非等值条件（例如 t1.ts > t2.ts - 10 或 t1.value > t2.value）。
// JoinCondition 接口的作用就是实现这个第二阶段的过滤逻辑。
// 代码生成： 它是为 Flink Table/SQL 优化器代码生成（Code Generation）而设计的接口。运行时会为特定的连接条件动态生成实现这个接口的 Java 代码，从而避免反射调用，提高执行效率。
// 运行时过滤： 它接收来自连接操作的两个输入行（RowData），应用复杂的非等值过滤逻辑，并返回一个布尔值，决定这对行是否应该作为连接结果的一部分。
public interface JoinCondition extends RichFunction {

    /** @return true if the join condition stays true for the joined row (in1, in2) */
    boolean apply(RowData in1, RowData in2);
}
