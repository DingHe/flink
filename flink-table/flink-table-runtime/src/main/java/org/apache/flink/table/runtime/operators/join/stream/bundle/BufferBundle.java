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

package org.apache.flink.table.runtime.operators.join.stream.bundle;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link BufferBundle} is a bundle to buffer the input records in memory and fold data based on
 * specified pattern to reduce state access. The bundle is used in MiniBatchStreamingJoinOperator.
 * The structure of the bundle varies depending on the {@link JoinInputSideSpec}.
 */
// BufferBundle 是 Flink Table API/SQL 中用于流式 Join 算子（MiniBatchStreamingJoinOperator）的内存数据捆绑（Mini-Batch）抽象类
// 核心作用在于实现 "Mini-Batching" 策略，即在内存中缓冲（Buffer）并对输入记录进行折叠/聚合（Fold）处理，从而减少对 Flink 状态后端的访问次数，显著提高流式 Join 的吞吐量和性能。
public abstract class BufferBundle<T> {
    // 核心缓冲 Map。
    // 用于在内存中存储缓冲的记录。
    // Map 的 Key 通常是 Join Key 或 Unique Key (RowData)。
    // Value (T) 是一个泛型类型，表示折叠后的记录集合或计数（具体取决于子类实现）。
    protected final Map<RowData, T> bundle;
    // 当前捆绑中处理的总记录数。
    // 每次调用 addRecord 都会递增，表示进入缓冲的原始记录总量。
    protected int count;
    // 当前捆绑中不重复的（或折叠后的）键的数量。
    // 通常是 bundle Map 中条目的数量，表示实际需要处理的不同记录/键的数量。
    protected int actualSize;

    public BufferBundle() {
        this.bundle = new HashMap<>();
        this.count = 0;
        this.actualSize = 0;
    }

    /** Check if this bufferBundle is empty. */
    public boolean isEmpty() {
        return count == 0;
    }

    /** Return the number of reduced records. */
    public int reducedSize() {
        return count - actualSize;
    }

    /** Clear this bufferBundle. */
    public void clear() {
        bundle.clear();
        count = 0;
        actualSize = 0;
    }

    /**
     * Get the joinKeys in bufferBundle. Whether to override this method is based on the
     * implementing class.
     */
    public Set<RowData> getJoinKeys() {
        return Collections.emptySet();
    }

    /**
     * Adds a record into the bufferBundle when processing element in a stream and this function
     * would return the size of the bufferBundle.
     *
     * @param joinKey the joinKey associated with the record.
     * @param uniqueKey the uniqueKey associated with the record. This could be null.
     * @param record The record to add.
     * @return number of processed by current bundle.
     */
    public abstract int addRecord(RowData joinKey, @Nullable RowData uniqueKey, RowData record);

    /**
     * Get records associated with joinKeys from bufferBundle.
     *
     * @return a map whose key is joinKey and value is list of records.
     */
    public abstract Map<RowData, List<RowData>> getRecords() throws Exception;

    /**
     * Get records associated with joinKeys from bufferBundle. And this function is different from
     * getRecords() above where getRecords() returns a map whose key is joinKey and value is list of
     * records.
     *
     * @param joinKey one of joinKeys stored in this bundle.
     * @return a map whose key is uniqueKey and value is a list of records.
     */
    public abstract Map<RowData, List<RowData>> getRecordsWithJoinKey(RowData joinKey)
            throws Exception;
}
