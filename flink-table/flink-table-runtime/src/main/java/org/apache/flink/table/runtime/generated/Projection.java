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

import org.apache.flink.table.data.RowData;

/** Interface for code generated projection, which will map a RowData to another one. */
// Projection 接口是 Flink SQL 中实现 SELECT 语句、列裁剪或数据类型转换等操作的底层机制。
// 它的核心作用是将一个输入数据行 (IN extends RowData) 映射（投影）为一个新的输出数据行 (OUT extends RowData)。
// 代码生成： 与其他 Flink Generated 接口类似，Projection 是为 Flink Table/SQL 优化器动态代码生成而设计的。运行时会针对特定的投影（例如，SELECT id, name）动态生成实现此接口的 Java 代码。
// 数据映射： 它定义了从输入行的字段到输出行的字段的精确映射关系，可以包括：
// 列选择/重排（例如，只取输入行中的第 3 和第 1 列）。
// 字段值转换（例如，将输入行的某个字段转换为常量）。
public interface Projection<IN extends RowData, OUT extends RowData> {
    // 执行数据行映射（投影）。
    // 这是该接口的核心方法，包含了将输入行转换为输出行的逻辑。
    // row: 传入的输入数据行 (IN)。
    // 返回值： 返回经过投影操作后生成的新的输出数据行 (OUT)。
    OUT apply(IN row);
}
