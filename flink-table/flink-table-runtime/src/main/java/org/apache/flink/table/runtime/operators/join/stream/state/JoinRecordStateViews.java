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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility to create a {@link JoinRecordStateView} depends on {@link JoinInputSideSpec}. */
public final class JoinRecordStateViews {

    /** Creates a {@link JoinRecordStateView} depends on {@link JoinInputSideSpec}. */
    public static JoinRecordStateView create(
            RuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            InternalTypeInfo<RowData> recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new JoinKeyContainsUniqueKey(ctx, stateName, recordType, ttlConfig);
            } else {
                return new InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new InputSideHasNoUniqueKey(ctx, stateName, recordType, ttlConfig);
        }
    }

    // ------------------------------------------------------------------------------------

    private static final class JoinKeyContainsUniqueKey implements JoinRecordStateView {

        private final ValueState<RowData> recordState;
        private final List<RowData> reusedList;

        private JoinKeyContainsUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            ValueStateDescriptor<RowData> recordStateDesc =
                    new ValueStateDescriptor<>(stateName, recordType);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getState(recordStateDesc);
            // the result records always not more than 1
            this.reusedList = new ArrayList<>(1);
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            recordState.update(record);
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            recordState.clear();
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            reusedList.clear();
            RowData record = recordState.value();
            if (record != null) {
                reusedList.add(record);
            }
            return reusedList;
        }
    }
    // Flink Table/SQL 流式 Join 算子中 JoinRecordStateView 接口的一个具体实现，专用于处理 **Join 输入侧拥有唯一键（Unique Key, UK）**的情况
    // 利用唯一键优化状态： 由于输入流保证了唯一性，对于一个给定的 Join Key，状态中最多只需要存储一条特定唯一键对应的记录。这使得状态结构更加高效和清晰。
    // 存储结构： 它使用 MapState<UK, Record> 来存储数据。Map 的 Key 是记录的唯一键，Value 是完整的记录。
    // 支持更新和删除： 凭借唯一键，当有新的记录到达时，如果唯一键相同，可以直接覆盖旧记录（实现更新，Upsert 语义）；当需要撤回（Retract）记录时，可以直接通过唯一键将对应的记录从状态中精确删除。
    // 简而言之，该实现利用了唯一键的语义特性，提供了一种基于唯一键的记录存储和查找机制，从而简化了状态操作，并支持高效的更新/撤回操作。
    private static final class InputSideHasUniqueKey implements JoinRecordStateView {

        // stores record in the mapping <UK, Record>
        // 记录状态。
        // 底层的 Flink 键控状态。
        // Key 是 唯一键（RowData 形式），Value 是完整的输入记录（RowData）。这种结构确保了每个唯一键在状态中只有一条记录。
        private final MapState<RowData, RowData> recordState;
        // 唯一键提取器。
        // 一个函数对象，负责从输入的完整记录 (RowData) 中提取出其唯一键部分 (RowData)，以便于查询和操作 recordState。
        private final KeySelector<RowData, RowData> uniqueKeySelector;

        private InputSideHasUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> uniqueKeyType,
                KeySelector<RowData, RowData> uniqueKeySelector,
                StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    new MapStateDescriptor<>(stateName, uniqueKeyType, recordType);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.uniqueKeySelector = uniqueKeySelector;
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            recordState.put(uniqueKey, record);
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            recordState.remove(uniqueKey);
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return recordState.values();
        }
    }

    // 该类的作用是为 Join 输入侧没有唯一键（No Unique Key）的情况提供高效且正确的状态管理。
    // 在流式 Join 场景中，如果一个输入流没有唯一键，这意味着对于同一个 Join Key，可能会存在多条完全相同的记录。
    // 为了正确处理这些重复记录以及数据的撤回（Retraction）操作，该实现不能简单地存储记录本身，而是必须存储每条记录的出现次数（Count）
    // InputSideHasNoUniqueKey 的核心在于：
    // 使用 MapState<RowData, Integer>： 利用 MapState 来存储输入记录 (RowData) 及其计数 (Integer)。记录作为 Map 的 Key，计数作为 Map 的 Value。
    // 实现计数语义： 在添加记录时，增加计数；在撤回记录时，减少计数；当计数归零时，从状态中移除该记录，从而正确支持数据流的 Upsert/Retract 语义。
    // 遵循 JoinRecordStateView 接口： 向上层 Join 逻辑提供统一的 addRecord、retractRecord 和 getRecords 接口。
    private static final class InputSideHasNoUniqueKey implements JoinRecordStateView {
        // 记录状态。
        // 这是底层使用的 Flink 键控状态。
        // 它的 Key 是 输入记录本身（RowData），Value 是该记录在当前 Join Key 下的出现次数/计数（Integer）
        private final MapState<RowData, Integer> recordState;

        // 私有构造函数创建并配置 MapStateDescriptor，用于定义 recordState 的结构（Key 是 RowData，Value 是 Integer）。
        // 如果启用了 TTL，则应用 TTL 配置。最后，通过 ctx.getMapState(recordStateDesc) 从状态后端获取实际的 MapState 实例并赋值给 recordState。
        private InputSideHasNoUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            MapStateDescriptor<RowData, Integer> recordStateDesc =
                    new MapStateDescriptor<>(stateName, recordType, Types.INT);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
        }
        // 添加记录（增量计数）。
        // 用于向状态中添加一条记录。它会读取当前记录的计数：如果记录已存在，则计数加 1；如果记录是新的，则计数设为 1。
        // 然后将更新后的计数写回 recordState。
        @Override
        public void addRecord(RowData record) throws Exception {
            Integer cnt = recordState.get(record);
            if (cnt != null) {
                cnt += 1;
            } else {
                cnt = 1;
            }
            recordState.put(record, cnt);
        }
        // 撤回记录（减量计数）。用于从状态中移除一条记录（处理 DELETE 或更新中的旧记录）。它会读取当前记录的计数：
        //1. 如果计数大于 1，则计数减 1，并将新值存回状态。
        //2. 如果计数等于 1，则从状态中移除该记录。
        //3. 如果记录不存在（cnt == null），则忽略（可能记录已过期）
        @Override
        public void retractRecord(RowData record) throws Exception {
            Integer cnt = recordState.get(record);
            if (cnt != null) {
                if (cnt > 1) {
                    recordState.put(record, cnt - 1);
                } else {
                    recordState.remove(record);
                }
            }
            // ignore cnt == null, which means state may be expired
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return new IterableIterator<RowData>() {

                private final Iterator<Map.Entry<RowData, Integer>> backingIterable =
                        recordState.entries().iterator();
                private RowData record;
                private int remainingTimes = 0;

                @Override
                public boolean hasNext() {
                    return backingIterable.hasNext() || remainingTimes > 0;
                }

                @Override
                public RowData next() {
                    if (remainingTimes > 0) {
                        checkNotNull(record);
                        remainingTimes--;
                        return record;
                    } else {
                        Map.Entry<RowData, Integer> entry = backingIterable.next();
                        record = entry.getKey();
                        remainingTimes = entry.getValue() - 1;
                        return record;
                    }
                }

                @Override
                public Iterator<RowData> iterator() {
                    return this;
                }
            };
        }
    }
}
