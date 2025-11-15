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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/** Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN. */
// StreamingJoinOperator 是 Flink Table/SQL 流式无界连接 的核心实现类，
// 它继承自 AbstractStreamingJoinOperator，专门处理 INNER JOIN、LEFT JOIN、RIGHT JOIN 和 FULL JOIN。
// 实现增量流式连接逻辑： 针对流数据中不断到达的 变更消息（INSERT / DELETE / UPDATE_BEFORE / UPDATE_AFTER，即 RowKind），实时地计算出 Join 结果的增量变更，并将这些增量消息（+I, -D, +U, -U）发送到下游。
// 管理 Keyed State： 实例化和管理左右两侧输入数据的 Flink 状态 (JoinRecordStateView)，用于存储到达的记录，以便将当前记录与历史记录进行匹配。
// 精确处理外连接（Outer Join）语义： 通过 OuterJoinRecordStateView 和记录的关联计数 (numOfAssociations)，确保在外连接中，当记录被匹配时，发送正确的撤回消息（DELETE）来撤回之前的 Null 填充行，并在记录被撤回或取消匹配时，发送正确的 Null 填充行（INSERT）。
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    // 左侧为外侧标记。
    // 如果是 LEFT OUTER JOIN 或 FULL OUTER JOIN，则为 true。用于在 open() 阶段决定实例化何种状态视图（带或不带关联计数）。
    protected final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    // 右侧为外侧标记。
    // 如果是 RIGHT OUTER JOIN 或 FULL OUTER JOIN，则为 true。
    protected final boolean rightIsOuter;
    // 输出行对象。
    // 可重用的对象，用于将匹配的两行（或一行和 Null 填充行）合并成一个输出行，并设置 RowKind，减少对象分配开销。
    private transient JoinedRowData outRow;
    // 左侧 Null 填充行。
    // 一个全为 Null 的行数据，用于在 Right/Full Outer Join 中，当右侧记录没有左侧匹配时，作为左侧部分的填充。
    private transient RowData leftNullRow;
    // 右侧 Null 填充行。
    // 一个全为 Null 的行数据，用于在 Left/Full Outer Join 中，当左侧记录没有右侧匹配时，作为右侧部分的填充。
    private transient RowData rightNullRow;

    // left join state
    // 左侧记录状态视图。
    // 封装了 Flink State 的操作，用于存储和访问左输入侧的记录。如果 leftIsOuter 为 true，则实际类型为 OuterJoinRecordStateView。
    protected transient JoinRecordStateView leftRecordStateView;
    // right join state
    // 右侧记录状态视图。
    // 封装了 Flink State 的操作，用于存储和访问右输入侧的记录。如果 rightIsOuter 为 true，则实际类型为 OuterJoinRecordStateView。
    protected transient JoinRecordStateView rightRecordStateView;

    public StreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTime);
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
    }
    // 算子初始化
    @Override
    public void open() throws Exception {
        super.open();

        this.outRow = new JoinedRowData();
        this.leftNullRow = new GenericRowData(leftType.toRowSize());
        this.rightNullRow = new GenericRowData(rightType.toRowSize());

        // initialize states
        if (leftIsOuter) {
            this.leftRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        } else {
            this.leftRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        }

        if (rightIsOuter) {
            this.rightRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        } else {
            this.rightRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        }
    }
    // 处理左输入流元素。
    // 调用通用的 processElement 方法，传入左侧输入数据、左右侧状态视图、标记 inputIsLeft=true 和 isSuppress=false。
    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true, false);
    }
    // 处理右输入流元素。
    // 调用通用的 processElement 方法，传入右侧输入数据、右左侧状态视图、标记 inputIsLeft=false 和 isSuppress=false。
    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false, false);
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
     * method is too complex, so we provide the pseudo code to help understand the logic. We should
     * keep sync the following pseudo code with the real logic of the method.
     *
     * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise, we
     * always send insert and delete for simplification. We can optimize this to send -U & +U
     * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
     * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now for
     * simplification. See {@code
     * FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
     *
     * <pre>
     * if input record is accumulate
     * |  if input side is outer
     * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
     * |  |  if there are matched rows on the other side
     * |  |  | if other side is outer
     * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  | |  if the matched num in the matched rows > 0, skip
     * |  |  | |  otherState.update(other, old + 1)
     * |  |  | endif
     * |  |  | send +I[record+other]s, state.add(record, other.size)
     * |  |  endif
     * |  endif
     * |  if input side not outer
     * |  |  state.add(record)
     * |  |  if there is no matched rows on the other side, skip
     * |  |  if there are matched rows on the other side
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  |  |  if the matched num in the matched rows > 0, skip
     * |  |  |  |  otherState.update(other, old + 1)
     * |  |  |  |  send +I[record+other]s
     * |  |  |  else
     * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
     * |  |  |  endif
     * |  |  endif
     * |  endif
     * endif
     *
     * if input record is retract
     * |  state.retract(record)
     * |  if there is no matched rows on the other side
     * |  | if input side is outer, send -D[record+null]
     * |  endif
     * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
     * |  |  |  if the matched num in the matched rows > 1, skip
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft whether input side is left side
     * @param isSuppress whether suppress the output of redundant messages when the other side is
     *     outer join. This only applies to the case of mini-batch.
     */
    // 核心流式连接逻辑。
    // 这是本算子的核心方法，负责处理每一条增量输入记录，并计算其对 Join 结果的影响。
    protected void processElement(
            RowData input,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft,
            boolean isSuppress)
            throws Exception {
        // 根据当前输入是左输入还是右输入（inputIsLeft），判断该输入侧是否为 outer join（left outer / right outer）。
        // 用于决定当对方没有匹配时是否要输出 padding（例如 record + null）。
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        // 对应对端（other side）是否为 outer。用于在更新对端 state（维护匹配计数）或生成对端 padding 输出时的判断
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        // 判断当前 input 是否是“累加消息”（例如 INSERT / UPDATE_AFTER — 表示增加/插入类型），而非撤回消息（DELETE / UPDATE_BEFORE）
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        // 保存原始的 RowKind
        RowKind inputRowKind = input.getRowKind();
        // 把 input 的 RowKind 改为 INSERT
        // 这是为了后续把 input 写入状态（addRecord）时，状态中保存的记录都以 INSERT 形式保存（避免在状态中存不同 kind 导致复杂性）
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating
        // 表示当前输入 record 在“对端”state 下匹配到的所有记录
        AssociatedRecords associatedRecords =
                AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
        // 处理“增量（accumulate）”消息的分支（INSERT / UPDATE_AFTER）
        if (isAccumulateMsg) { // record is accumulate
            if (inputIsOuter) { // input side is outer
                OuterJoinRecordStateView inputSideOuterStateView =
                        (OuterJoinRecordStateView) inputSideStateView;
                // 若对端没有匹配记录：
                if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
                    // send +I[record+null]
                    // 设置输出为 INSERT (+I)
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0)
                    // 把当前 input 写入输入侧状态，并且记录“当前 record 在对端匹配到 0 条”。
                    // 这个 0 会在未来当对端出现匹配时用于判断是否需要发出对端的退挡（-D）或其他动作。
                    inputSideOuterStateView.addRecord(input, 0);
                } else { // there are matched rows on the other side
                    //有对端匹配
                    if (otherIsOuter) { // other side is outer
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        // 遍历对端匹配的记录
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            RowData other = outerRecord.record;
                            // if the matched num in the matched rows == 0
                            if (outerRecord.numOfAssociations == 0 && !isSuppress) {
                                // 说明之前该对端记录在其本侧是“没有匹配到任何输入侧记录”，
                                // 也就是说那条对端记录之前可能已经输出过 null+other 的 padding（因为它是 outer 并且无匹配），
                                // 现在遇到 input 的这条 new record 后，需要“收回”之前那个 padding
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(other, !inputIsLeft);
                            } // ignore matched number > 0
                            // otherState.update(other, old + 1)
                            otherSideOuterStateView.updateNumOfAssociations(
                                    other, outerRecord.numOfAssociations + 1);
                        }
                    }
                    // send +I[record+other]s
                    outRow.setRowKind(RowKind.INSERT);
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                    // state.add(record, other.size)
                    inputSideOuterStateView.addRecord(input, associatedRecords.size());
                }
            } else { // input side not outer
                // 当 input 不是 outer
                // state.add(record)
                inputSideStateView.addRecord(input);
                if (!associatedRecords.isEmpty()) { // if there are matched rows on the other side
                    if (otherIsOuter) { // if other side is outer
                        //若对端是 outer：
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            // 如果 outerRecord.numOfAssociations == 0 && !isSuppress：说明之前对端记录曾单独输出过 null+other，
                            // 现在因为 input 的到来它得到了第一个匹配，所以需要撤回对端之前输出的 null+other，因此发送 DELETE（-D）
                            if (outerRecord.numOfAssociations == 0
                                    && !isSuppress) { // if the matched num in the matched rows == 0
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(outerRecord.record, !inputIsLeft);
                            }
                            // otherState.update(other, old + 1)
                            otherSideOuterStateView.updateNumOfAssociations(
                                    outerRecord.record, outerRecord.numOfAssociations + 1);
                        }
                        // send +I[record+other]s
                        outRow.setRowKind(RowKind.INSERT);
                    } else {
                        // send +I/+U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                    }
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                }
                // skip when there is no matched rows on the other side
            }
        } else { // input record is retract
            // 处理“撤回（retract）”消息的分支（DELETE / UPDATE_BEFORE）
            // state.retract(record)
            if (!isSuppress) {
                inputSideStateView.retractRecord(input);
            }
            // 若对端没有匹配
            if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send -D[record+null]
                    outRow.setRowKind(RowKind.DELETE);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer
            } else { // there are matched rows on the other side
                // 如果 inputIsOuter：对 input + other 的组合全部以 DELETE（-D）发送（删掉这些对应的 join 结果
                if (inputIsOuter) {
                    // send -D[record+other]s
                    outRow.setRowKind(RowKind.DELETE);
                } else {
                    // 如果 input 不是 outer：使用 inputRowKind（保存的原始 RowKind），
                    // 可能是 DELETE 或 UPDATE_BEFORE/UPDATE_AFTER 等，用于 inner join 的语义保持（可以发送 -D/-U 等）
                    // send -D/-U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                }
                for (RowData other : associatedRecords.getRecords()) {
                    output(input, other, inputIsLeft);
                }
                // if other side is outer
                if (otherIsOuter) {
                    OuterJoinRecordStateView otherSideOuterStateView =
                            (OuterJoinRecordStateView) otherSideStateView;
                    for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                        if (outerRecord.numOfAssociations == 1 && !isSuppress) {
                            // send +I[null+other]
                            outRow.setRowKind(RowKind.INSERT);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        } // nothing else to do when number of associations > 1
                        // otherState.update(other, old - 1)
                        otherSideOuterStateView.updateNumOfAssociations(
                                outerRecord.record, outerRecord.numOfAssociations - 1);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------------------
    // 输出匹配的行。
    // 根据 inputIsLeft 标记，将 inputRow 和 otherRow 正确地组合到 outRow 中，并使用 collector 发送。
    // outRow 的 RowKind 必须在调用前设置好。
    private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
        if (inputIsLeft) {
            outRow.replace(inputRow, otherRow);
        } else {
            outRow.replace(otherRow, inputRow);
        }
        collector.collect(outRow);
    }
    // 输出 Null 填充的行。
    // 根据 isLeft 标记，将 row 与对应的 Null 行（rightNullRow 或 leftNullRow）组合到 outRow 中，并使用 collector 发送。
    // outRow 的 RowKind 必须在调用前设置好（通常是 INSERT 或 DELETE
    private void outputNullPadding(RowData row, boolean isLeft) {
        if (isLeft) {
            outRow.replace(row, rightNullRow);
        } else {
            outRow.replace(leftNullRow, row);
        }
        collector.collect(outRow);
    }
}
