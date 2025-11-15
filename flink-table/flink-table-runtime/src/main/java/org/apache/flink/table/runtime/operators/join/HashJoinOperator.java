/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.hashtable.BinaryHashPartition;
import org.apache.flink.table.runtime.hashtable.BinaryHashTable;
import org.apache.flink.table.runtime.hashtable.ProbeIterator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Hash join base operator.
 *
 * <p>The join operator implements the logic of a join operator at runtime. It uses a
 * hybrid-hash-join internally to match the records with equal key. The build side of the hash is
 * the first input of the match. It support all join type in {@link HashJoinType}.
 *
 * <p>Note: In order to solve the problem of data skew, or too much data in the hash table, the
 * fallback to sort merge join mechanism is introduced here. If some partitions are spilled to disk
 * more than three times in the process of hash join, it will fallback to sort merge join by default
 * to improve stability. In the future, we will support more flexible adaptive hash join strategy,
 * for example, in the process of building a hash table, if the size of data written to disk reaches
 * a certain threshold, fallback to sort merge join in advance.
 */
// HashJoinOperator 是 Flink Table API 和 SQL 运行时中用于执行 哈希连接 (Hash Join) 逻辑的抽象基类。
// 实现 Hybrid Hash Join 算法： 它内部使用 BinaryHashTable 来实现高效的混合哈希连接（Hybrid Hash Join）。该算法首先尝试将较小的输入（Build Side）构建到内存中的哈希表，然后使用较大的输入（Probe Side）来探测匹配。
// 如果数据量超出内存，它会将溢出的分区写入磁盘进行递归处理。
// 自适应降级 (Adaptive Fallback)： 为了处理数据倾斜或哈希表过大导致的频繁磁盘溢出问题，该算子内置了降级到 Sort Merge Join (SMJ) 的机制。如果某些分区溢出到磁盘的次数超过预设阈值，它会在处理完所有输入后，将这些有问题的分区交给 SMJ 逻辑进行处理，以提高稳定性。
// 两输入流处理： 它实现了 TwoInputStreamOperator 接口，能够处理两个独立的输入流（一个 Build Side，一个 Probe Side），并利用 InputSelectable 接口控制输入的选择顺序，确保先完成 Build 阶段，再进入 Probe 阶段


public abstract class HashJoinOperator extends TableStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                BoundedMultiInput,
                InputSelectable {

    private static final Logger LOG = LoggerFactory.getLogger(HashJoinOperator.class);
    // 连接参数。
    // 包含了执行哈希连接所需的所有配置信息，如连接类型、Build/Probe 端的判断、压缩设置、投影代码、统计信息等
    private final HashJoinParameter parameter;
    // 连接函数反转标记。
    // 指示在调用非等值连接条件函数 (condition) 时，是否需要交换 Build 和 Probe 行的顺序。
    private final boolean reverseJoinFunction;
    // 哈希连接类型。
    // 定义了具体的连接逻辑（如 INNER, FULL_OUTER 等）。
    private final HashJoinType type;
    // Build 端标记。
    // 指示逻辑上的左输入是否为哈希表的构建端。用于在降级到 SMJ 时正确调用输入处理方法。
    private final boolean leftIsBuild;
    // SMJ 备用函数。
    // 封装了 Sort Merge Join 的逻辑，用于在 Hash Join 遇到严重数据倾斜时进行降级处理。
    private final SortMergeJoinFunction sortMergeJoinFunction;
    // 核心哈希表。
    // Flink 运行时提供的混合哈希表实现，用于存储 Build Side 数据，并提供高效的 Key 查找。
    private transient BinaryHashTable table;
    // 结果收集器。 用于将连接结果（RowData）发送到下游算子。
    transient Collector<RowData> collector;
    // Build 侧的 Null 行。
    // 一个全为 Null 的行数据对象，用于在外连接（如 ProbeOuterJoin）中，当 Probe 侧行没有匹配的 Build 侧行时，作为填充使用。
    transient RowData buildSideNullRow;
    // Probe 侧的 Null 行。
    // 一个全为 Null 的行数据对象，用于在外连接（如 BuildOuterJoin）中，当 Build 侧行没有匹配的 Probe 侧行时，作为填充使用。
    private transient RowData probeSideNullRow;
    // 连接结果行。 一个可重用的对象，用于将 Build Row 和 Probe Row 组合成一个输出行，以减少对象创建开销。
    private transient JoinedRowData joinedRow;
    // Build 阶段结束标记。
    // true 表示 Build 阶段已完成，当前算子正在处理 Probe 阶段的输入。
    private transient boolean buildEnd;
    // 非等值连接条件。
    // 由 Flink 代码生成器生成的运行时函数，用于评估非等值连接条件（例如 WHERE A.id = B.id AND A.val < B.val 中的 A.val < B.val）。
    private transient JoinCondition condition;

    // Flag indicates whether fallback to sort merge join in probe phase
    // SMJ 降级标记。 true 表示算子已触发降级到 Sort Merge Join 来处理部分或全部溢出分区。
    private transient boolean fallbackSMJ;

    HashJoinOperator(HashJoinParameter parameter) {
        this.parameter = parameter;
        this.type = parameter.type;
        this.leftIsBuild = parameter.leftIsBuild;
        this.reverseJoinFunction = parameter.reverseJoinFunction;
        this.sortMergeJoinFunction = parameter.sortMergeJoinFunction;
    }

    @Override
    public void open() throws Exception {
        super.open();

        ClassLoader cl = getContainingTask().getUserCodeClassLoader();

        final AbstractRowDataSerializer buildSerializer =
                (AbstractRowDataSerializer)
                        getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
        final AbstractRowDataSerializer probeSerializer =
                (AbstractRowDataSerializer)
                        getOperatorConfig().getTypeSerializerIn2(getUserCodeClassloader());

        boolean hashJoinUseBitMaps =
                getContainingTask()
                        .getEnvironment()
                        .getTaskConfiguration()
                        .get(AlgorithmOptions.HASH_JOIN_BLOOM_FILTERS);

        int parallel = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

        this.condition = parameter.condFuncCode.newInstance(cl);
        condition.setRuntimeContext(getRuntimeContext());
        condition.open(DefaultOpenContext.INSTANCE);
        // 实例化核心 BinaryHashTable，传入内存管理器、I/O 管理器、行序列化器、投影函数以及内存大小等参数。
        this.table =
                new BinaryHashTable(
                        getContainingTask(),
                        parameter.compressionEnabled,
                        parameter.compressionBlockSize,
                        buildSerializer,
                        probeSerializer,
                        parameter.buildProjectionCode.newInstance(cl),
                        parameter.probeProjectionCode.newInstance(cl),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        computeMemorySize(),
                        getContainingTask().getEnvironment().getIOManager(),
                        parameter.buildRowSize,
                        parameter.buildRowCount / parallel,
                        hashJoinUseBitMaps,
                        type,
                        condition,
                        reverseJoinFunction,
                        parameter.filterNullKeys,
                        parameter.tryDistinctBuildRow);

        this.collector = new StreamRecordCollector<>(output);

        this.buildSideNullRow = new GenericRowData(buildSerializer.getArity());
        this.probeSideNullRow = new GenericRowData(probeSerializer.getArity());
        this.joinedRow = new JoinedRowData();
        this.buildEnd = false;
        this.fallbackSMJ = false;

        getMetricGroup().gauge("memoryUsedSizeInBytes", table::getUsedMemoryInBytes);
        getMetricGroup().gauge("numSpillFiles", table::getNumSpillFiles);
        getMetricGroup().gauge("spillInBytes", table::getSpillInBytes);

        parameter.condFuncCode = null;
        parameter.buildProjectionCode = null;
        parameter.probeProjectionCode = null;
    }
    // 处理第一个输入（Build Side）。
    // 检查 buildEnd 标记，确保当前处于 Build 阶段。将接收到的行数据 (element.getValue()) 放入哈希表 (table.putBuildRow()) 进行构建。
    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        checkState(!buildEnd, "Should not build ended.");
        this.table.putBuildRow(element.getValue());
    }
    // 处理第二个输入（Probe Side）。
    // 检查 buildEnd 标记，确保当前处于 Probe 阶段。
    // 使用接收到的行数据 (element.getValue()) 尝试探测哈希表 (table.tryProbe())。如果找到匹配的 Key，则调用 joinWithNextKey() 进行结果输出。
    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        checkState(buildEnd, "Should build ended.");
        if (this.table.tryProbe(element.getValue())) {
            joinWithNextKey();
        }
    }
    // 输入选择。
    // 实现了 InputSelectable 接口。
    // 如果 buildEnd 为 false，则选择输入 1 (InputSelection.FIRST) 进行 Build；
    // 否则选择输入 2 (InputSelection.SECOND) 进行 Probe。
    @Override
    public InputSelection nextSelection() {
        return buildEnd ? InputSelection.SECOND : InputSelection.FIRST;
    }

    @Override
    public void endInput(int inputId) throws Exception {
        switch (inputId) {
            case 1:
                checkState(!buildEnd, "Should not build ended.");
                LOG.info("Finish build phase.");
                buildEnd = true;
                this.table.endBuild();
                break;
            case 2:
                checkState(buildEnd, "Should build ended.");
                LOG.info("Finish probe phase.");
                while (this.table.nextMatching()) {
                    joinWithNextKey();
                }
                LOG.info("Finish rebuild phase.");

                // switch to sort merge join process the remaining partition which recursive
                // level > 3
                fallbackSMJProcessPartition();
                break;
        }
    }

    private void joinWithNextKey() throws Exception {
        // we have a next record, get the iterators to the probe and build side values
        join(table.getBuildSideIterator(), table.getCurrentProbeRow());
    }

    public abstract void join(RowIterator<BinaryRowData> buildIter, RowData probeRow)
            throws Exception;

    void innerJoin(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
        collect(buildIter.getRow(), probeRow);
        while (buildIter.advanceNext()) {
            collect(buildIter.getRow(), probeRow);
        }
    }

    void buildOuterJoin(RowIterator<BinaryRowData> buildIter) throws Exception {
        collect(buildIter.getRow(), probeSideNullRow);
        while (buildIter.advanceNext()) {
            collect(buildIter.getRow(), probeSideNullRow);
        }
    }

    void collect(RowData row1, RowData row2) throws Exception {
        if (reverseJoinFunction) {
            collector.collect(joinedRow.replace(row2, row1));
        } else {
            collector.collect(joinedRow.replace(row1, row2));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeHashTable();
        condition.close();

        // If fallback to sort merge join during hash join, also need to close the operator
        if (fallbackSMJ) {
            sortMergeJoinFunction.close();
        }
    }

    private void closeHashTable() {
        if (this.table != null) {
            this.table.close();
            this.table.free();
            this.table = null;
        }
    }

    /**
     * If here also exists partitions which spilled to disk more than three time when hash join end,
     * means that the key in these partitions is very skewed, so fallback to sort merge join
     * algorithm to process it.
     */
    // SMJ 降级处理
    // 检查 table.getPartitionsPendingForSMJ()，如果存在溢出次数过多而需要降级处理的分区，则：
    // 1. 释放哈希表内存。
    // 2. 初始化 sortMergeJoinFunction。
    // 3. 遍历这些分区，将其 Build 和 Probe 侧数据喂给 SMJ 逻辑。
    // 4. 关闭哈希表和 SMJ 逻辑。
    private void fallbackSMJProcessPartition() throws Exception {
        if (!table.getPartitionsPendingForSMJ().isEmpty()) {
            // release memory to MemoryManager first that is used to sort merge join operator
            table.releaseMemoryCacheForSMJ();
            // initialize sort merge join operator
            LOG.info("Fallback to sort merge join to process spilled partitions.");
            initialSortMergeJoinFunction();
            fallbackSMJ = true;

            for (BinaryHashPartition p : table.getPartitionsPendingForSMJ()) {
                // process build side
                RowIterator<BinaryRowData> buildSideIter =
                        table.getSpilledPartitionBuildSideIter(p);
                while (buildSideIter.advanceNext()) {
                    processSortMergeJoinElement1(buildSideIter.getRow());
                }

                // process probe side
                ProbeIterator probeIter = table.getSpilledPartitionProbeSideIter(p);
                BinaryRowData probeNext;
                while ((probeNext = probeIter.next()) != null) {
                    processSortMergeJoinElement2(probeNext);
                }
            }

            // close the HashTable
            closeHashTable();

            // finish build and probe
            sortMergeJoinFunction.endInput(1);
            sortMergeJoinFunction.endInput(2);
            LOG.info("Finish sort merge join for spilled partitions.");
        }
    }
    // 初始化 sortMergeJoinFunction 实例，传入必要的运行时上下文、内存大小和收集器。
    private void initialSortMergeJoinFunction() throws Exception {
        sortMergeJoinFunction.open(
                true,
                this.getContainingTask(),
                this.getOperatorConfig(),
                (StreamRecordCollector) this.collector,
                this.computeMemorySize(),
                this.getRuntimeContext(),
                this.getMetricGroup());
    }
    // SMJ 输入 1 代理。
    // 根据 leftIsBuild 属性，将 Build 侧的数据正确地转发给 sortMergeJoinFunction 的输入 1 或输入 2。
    private void processSortMergeJoinElement1(RowData rowData) throws Exception {
        if (leftIsBuild) {
            sortMergeJoinFunction.processElement1(rowData);
        } else {
            sortMergeJoinFunction.processElement2(rowData);
        }
    }
    // SMJ 输入 2 代理。
    // 根据 leftIsBuild 属性，将 Probe 侧的数据正确地转发给 sortMergeJoinFunction 的输入 1 或输入 2。
    private void processSortMergeJoinElement2(RowData rowData) throws Exception {
        if (leftIsBuild) {
            sortMergeJoinFunction.processElement2(rowData);
        } else {
            sortMergeJoinFunction.processElement1(rowData);
        }
    }

    public static HashJoinOperator newHashJoinOperator(
            HashJoinType type,
            boolean leftIsBuild,
            boolean compressionEnable,
            int compressionBlockSize,
            GeneratedJoinCondition condFuncCode,
            boolean reverseJoinFunction,
            boolean[] filterNullKeys,
            GeneratedProjection buildProjectionCode,
            GeneratedProjection probeProjectionCode,
            boolean tryDistinctBuildRow,
            int buildRowSize,
            long buildRowCount,
            long probeRowCount,
            RowType keyType,
            SortMergeJoinFunction sortMergeJoinFunction) {
        HashJoinParameter parameter =
                new HashJoinParameter(
                        type,
                        leftIsBuild,
                        compressionEnable,
                        compressionBlockSize,
                        condFuncCode,
                        reverseJoinFunction,
                        filterNullKeys,
                        buildProjectionCode,
                        probeProjectionCode,
                        tryDistinctBuildRow,
                        buildRowSize,
                        buildRowCount,
                        probeRowCount,
                        keyType,
                        sortMergeJoinFunction);
        switch (type) {
            case INNER:
                return new InnerHashJoinOperator(parameter);
            case BUILD_OUTER:
                return new BuildOuterHashJoinOperator(parameter);
            case PROBE_OUTER:
                return new ProbeOuterHashJoinOperator(parameter);
            case FULL_OUTER:
                return new FullOuterHashJoinOperator(parameter);
            case SEMI:
                return new SemiHashJoinOperator(parameter);
            case ANTI:
                return new AntiHashJoinOperator(parameter);
            case BUILD_LEFT_SEMI:
            case BUILD_LEFT_ANTI:
                return new BuildLeftSemiOrAntiHashJoinOperator(parameter);
            default:
                throw new IllegalArgumentException("invalid: " + type);
        }
    }
    // HashJoinParameter 类是 Flink 内部用于配置和传递哈希连接 (Hash Join) 算子所需的所有元数据和参数的静态内部类。
    static class HashJoinParameter implements Serializable {
        // 哈希连接类型。 定义要执行的连接操作的类型，如 INNER, LEFT, RIGHT, FULL 等。
        HashJoinType type;
        // 构建端标记。
        // 表示左输入流是否作为 Hash 表的构建（Build） 端。如果为 true，则右输入是探测（Probe）端。
        boolean leftIsBuild;
        // 启用压缩。
        // 如果 Hash Join 溢出到磁盘，此标记决定是否对溢出数据启用压缩。
        boolean compressionEnabled;
        // 压缩块大小。
        // 如果启用压缩，定义溢出数据块的压缩大小。
        int compressionBlockSize;
        // 连接条件函数代码。
        // 封装了连接操作的非等值连接条件（WHERE 子句中除连接键等值外的其他条件）的动态代码。
        GeneratedJoinCondition condFuncCode;
        // 函数反转标记。
        // 当实际连接逻辑调用时，指示是否需要交换左右输入的顺序来调用连接条件函数 (condFuncCode)，以匹配逻辑上的 Build/Probe 关系。
        boolean reverseJoinFunction;
        // 空键过滤标记。
        // 长度为 2 的布尔数组，用于指示是否应过滤掉左右输入中连接键为 NULL 的行。这在 SEMI 或 ANTI Join 中很重要。
        boolean[] filterNullKeys;
        // 构建端投影代码。
        // 封装了将构建端行数据转换为仅包含连接键和可能包含有效载荷数据的投影逻辑。
        GeneratedProjection buildProjectionCode;
        // 探测端投影代码。
        // 封装了将探测端行数据转换为仅包含连接键的投影逻辑。
        GeneratedProjection probeProjectionCode;
        // 如果为 true，Hash Join 算子将尝试在构建 Hash 表时对重复的构建端行进行去重，
        // 这有助于优化内存使用，尤其是在执行 SEMI Join 时。
        boolean tryDistinctBuildRow;
        // 构建端行大小。 构建端数据行的平均字节大小（用于内存/I/O 估算）
        int buildRowSize;
        // 构建端行数。
        // 构建端数据集的预期行数统计（用于内存/I/O 估算）。
        long buildRowCount;
        // 探测端行数。
        // 探测端数据集的预期行数统计（用于内存/I/O 估算）。
        long probeRowCount;
        // 连接键类型。 描述连接键的 RowType 结构，用于实例化内部的 Key 序列化器和比较器。
        RowType keyType;
        // 备用 Sort Merge Join 逻辑。
        // 在 Flink 的自适应连接 (Adaptive Join) 策略中，如果 Hash Join 由于内存不足（溢出）等原因性能不佳，
        // 可能会降级（Fallback）到 Sort Merge Join，此属性封装了降级所需的 SMJ 逻辑。
        SortMergeJoinFunction sortMergeJoinFunction;

        HashJoinParameter(
                HashJoinType type,
                boolean leftIsBuild,
                boolean compressionEnabled,
                int compressionBlockSize,
                GeneratedJoinCondition condFuncCode,
                boolean reverseJoinFunction,
                boolean[] filterNullKeys,
                GeneratedProjection buildProjectionCode,
                GeneratedProjection probeProjectionCode,
                boolean tryDistinctBuildRow,
                int buildRowSize,
                long buildRowCount,
                long probeRowCount,
                RowType keyType,
                SortMergeJoinFunction sortMergeJoinFunction) {
            this.type = type;
            this.leftIsBuild = leftIsBuild;
            this.compressionEnabled = compressionEnabled;
            this.compressionBlockSize = compressionBlockSize;
            this.condFuncCode = condFuncCode;
            this.reverseJoinFunction = reverseJoinFunction;
            this.filterNullKeys = filterNullKeys;
            this.buildProjectionCode = buildProjectionCode;
            this.probeProjectionCode = probeProjectionCode;
            this.tryDistinctBuildRow = tryDistinctBuildRow;
            this.buildRowSize = buildRowSize;
            this.buildRowCount = buildRowCount;
            this.probeRowCount = probeRowCount;
            this.keyType = keyType;
            this.sortMergeJoinFunction = sortMergeJoinFunction;
        }
    }

    /**
     * Inner join. Output assembled {@link JoinedRowData} when build side row matched probe side
     * row.
     */
    private static class InnerHashJoinOperator extends HashJoinOperator {

        InnerHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            if (buildIter.advanceNext()) {
                if (probeRow != null) {
                    innerJoin(buildIter, probeRow);
                }
            }
        }
    }

    /**
     * BuildOuter join. Output assembled {@link JoinedRowData} when build side row matched probe
     * side row. And if there is no match in the probe table, output {@link JoinedRowData} assembled
     * by build side row and nulls.
     */
    private static class BuildOuterHashJoinOperator extends HashJoinOperator {

        BuildOuterHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            if (buildIter.advanceNext()) {
                if (probeRow != null) {
                    innerJoin(buildIter, probeRow);
                } else {
                    buildOuterJoin(buildIter);
                }
            }
        }
    }

    /**
     * ProbeOuter join. Output assembled {@link JoinedRowData} when probe side row matched build
     * side row. And if there is no match in the build table, output {@link JoinedRowData} assembled
     * by nulls and probe side row.
     */
    private static class ProbeOuterHashJoinOperator extends HashJoinOperator {

        ProbeOuterHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            if (buildIter.advanceNext()) {
                if (probeRow != null) {
                    innerJoin(buildIter, probeRow);
                }
            } else if (probeRow != null) {
                collect(buildSideNullRow, probeRow);
            }
        }
    }

    /**
     * BuildOuter join. Output assembled {@link JoinedRowData} when build side row matched probe
     * side row. And if there is no match, output {@link JoinedRowData} assembled by build side row
     * and nulls or nulls and probe side row.
     */
    private static class FullOuterHashJoinOperator extends HashJoinOperator {

        FullOuterHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            if (buildIter.advanceNext()) {
                if (probeRow != null) {
                    innerJoin(buildIter, probeRow);
                } else {
                    buildOuterJoin(buildIter);
                }
            } else if (probeRow != null) {
                collect(buildSideNullRow, probeRow);
            }
        }
    }

    /** Semi join. Output probe side row when probe side row matched build side row. */
    private static class SemiHashJoinOperator extends HashJoinOperator {

        SemiHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            checkNotNull(probeRow);
            if (buildIter.advanceNext()) {
                collector.collect(probeRow);
            }
        }
    }

    /** Anti join. Output probe side row when probe side row not matched build side row. */
    private static class AntiHashJoinOperator extends HashJoinOperator {

        AntiHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            checkNotNull(probeRow);
            if (!buildIter.advanceNext()) {
                collector.collect(probeRow);
            }
        }
    }

    /**
     * BuildLeftSemiOrAnti join. BuildLeftSemiJoin: Output build side row when build side row
     * matched probe side row. BuildLeftAntiJoin: Output build side row when build side row not
     * matched probe side row.
     */
    private static class BuildLeftSemiOrAntiHashJoinOperator extends HashJoinOperator {

        BuildLeftSemiOrAntiHashJoinOperator(HashJoinParameter parameter) {
            super(parameter);
        }

        @Override
        public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
            if (buildIter.advanceNext()) {
                if (probeRow != null) { // Probe phase
                    // we must iterator to set probedSet.
                    //noinspection StatementWithEmptyBody
                    while (buildIter.advanceNext()) {}
                } else { // End Probe phase, iterator build side elements.
                    collector.collect(buildIter.getRow());
                    while (buildIter.advanceNext()) {
                        collector.collect(buildIter.getRow());
                    }
                }
            }
        }
    }
}
