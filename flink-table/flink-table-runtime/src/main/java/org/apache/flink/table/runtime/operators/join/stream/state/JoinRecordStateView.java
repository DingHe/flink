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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.table.data.RowData;

/**
 * A {@link JoinRecordStateView} is a view to the join state. It encapsulates the join state and
 * provides some APIs facing the input records. The join state is used to store input records. The
 * structure of the join state is vary depending on the {@link JoinInputSideSpec}.
 *
 * <p>For example: when the {@link JoinInputSideSpec} is JoinKeyContainsUniqueKey, we will use
 * {@link org.apache.flink.api.common.state.ValueState} to store records which has better
 * performance.
 */
// JoinRecordStateView 是 Apache Flink Table API/SQL 中用于流式 Join 算子的一个**状态抽象视图（State Abstraction View）**接口。
// 封装底层状态逻辑： 它将流式 Join 算子中存储输入记录的底层 Flink 状态（如 ValueState、ListState 或 MapState）的具体实现细节封装起来。
// 提供统一的记录管理接口： 无论底层使用哪种状态类型，它都为 Join 算子提供了一套统一、简洁的 API（添加、撤回、获取）来管理属于当前 Join Key 的输入记录。
// 支持状态优化： 这个视图能够根据 Join 输入侧的规范（JoinInputSideSpec，例如是否包含唯一键）来选择最优的底层状态结构。
// 示例： 如果 JoinInputSideSpec 表明 Join Key 中包含唯一键（JoinKeyContainsUniqueKey），那么对于给定的 Join Key，最多只会有一条记录。此时，JoinRecordStateView 的实现会选择使用性能更好的 ValueState 来存储，而不是使用 ListState 或 MapState。
// 它是一个设计模式中的门面（Facade），让 Join 算子的主逻辑只需要与抽象的“记录集合”交互，而无需关心状态后端的复杂性。

public interface JoinRecordStateView {

    /** Add a new record to the state view. */
    // 添加记录。
    // 将一条新的输入记录 (RowData) 添加或写入到该 Join Key 对应的底层状态中。
    void addRecord(RowData record) throws Exception;

    /** Retract the record from the state view. */
    // 撤回记录。
    // 从该 Join Key 对应的底层状态中删除一条记录 (RowData)。
    // 这个方法主要用于处理带有更新/删除语义的流（Changelog Stream），例如，当收到一条 DELETE 消息时，需要将状态中的对应记录移除。
    // 如果底层是 ListState，它会移除匹配的元素。
    void retractRecord(RowData record) throws Exception;

    /** Gets all the records under the current context (i.e. join key). */
    // 获取记录。
    // 返回当前 Join Key 对应的底层状态中存储的所有记录。
    // 由于返回的是 Iterable，可以迭代地访问这些记录，例如，用于查找匹配项以执行 Join 操作。
    Iterable<RowData> getRecords() throws Exception;
}
