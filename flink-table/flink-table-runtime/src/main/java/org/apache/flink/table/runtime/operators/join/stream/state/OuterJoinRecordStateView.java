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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

/**
 * A {@link OuterJoinRecordStateView} is an extension to {@link JoinRecordStateView}. The {@link
 * OuterJoinRecordStateView} is used to store records for the outer input side of the Join, e.g. the
 * left side of left join, the both side of full join.
 *
 * <p>The additional information we should store with the record is the number of associations which
 * is the number of records associated this record with other side. This is an important information
 * when to send/retract a null padding row, to avoid recompute the associated numbers every time.
 *
 * @see JoinRecordStateView
 */
// OuterJoinRecordStateView 接口是 Flink Table/SQL 流式 Join 算子中 JoinRecordStateView 的一个扩展，专门用于处理**外连接（Outer Join）**的输入侧状态。
// 外连接（例如 Left Join、Right Join 或 Full Join）的关键在于，即使一侧的记录在另一侧没有匹配项，也需要保留该记录并在输出中用 NULL 填充另一侧（即输出 Null Padding Row）
// 该类的核心作用是：
// 存储关联计数： 除了存储输入记录本身 (RowData)，它还必须存储每条记录的关联计数 (numOfAssociations)。这个计数代表了当前记录与 Join 另一侧匹配的记录数量。
// 避免重复计算： 关联计数是一个关键的优化。在处理更新或撤回时，如果这个计数已知，算子就能快速判断是否需要发送或撤回 Null Padding Row，而无需每次都重新扫描或计算另一侧的状态。
// 只有当记录的关联计数从 0 变为 >0 时（找到第一个匹配项），或者从 >0 变为 0 时（失去最后一个匹配项），Null Padding Row 的状态才会改变。
// 服务外连接输入侧： 它用于存储 Left Join 的左侧输入，或 Full Join 的两侧输入。
public interface OuterJoinRecordStateView extends JoinRecordStateView {

    /**
     * Adds a new record with the number of associations to the state view.
     *
     * @param record the added record
     * @param numOfAssociations the number of records associated with other side
     */
    // 添加记录（指定关联计数）。
    // 这是外连接特有的方法。它在将新记录添加到状态时，同时记录该记录与 Join 另一侧的初始匹配记录数量（numOfAssociations）
    void addRecord(RowData record, int numOfAssociations) throws Exception;

    /**
     * Updates the number of associations belongs to the record.
     *
     * @param record the record to update
     * @param numOfAssociations the new number of records associated with other side
     */
    // 更新关联计数。
    // 当 Join 另一侧的状态发生变化时（例如，添加或撤回了匹配项），需要调用此方法来更新特定记录的关联计数。
    // 这是外连接算子判断是否需要发出或撤回 Null Padding Row 的依据。
    void updateNumOfAssociations(RowData record, int numOfAssociations) throws Exception;

    /**
     * Gets all the records and number of associations under the current context (i.e. join key).
     */
    // 获取记录和关联计数。这是外连接特有的查询方法。
    // 它返回当前 Join Key 下存储的所有记录，以及它们各自对应的关联计数。返回类型是 Tuple2，其中第一个元素是记录 (RowData)，第二个元素是计数 (Integer)。
    Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations() throws Exception;
}
