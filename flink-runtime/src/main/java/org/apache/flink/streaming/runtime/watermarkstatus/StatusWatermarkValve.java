/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.streaming.api.watermark.InternalWatermark;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue.HeapPriorityQueueElement;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link
 * WatermarkStatus} are propagated to downstream outputs, given a set of one or multiple
 * subpartitions that continuously receive them. Usages of this class need to define the number of
 * subpartitions that the valve needs to handle, as well as provide a implementation of {@link
 * DataOutput}, which is called by the valve only when it determines a new watermark or watermark
 * status can be propagated.
 */
// 多输入流场景下协调 Watermark 和 WatermarkStatus 推进的核心组件，也被称为状态水位线阀门
// StatusWatermarkValve 的主要职责是接收来自所有输入分区的 Watermark 和 WatermarkStatus 事件，并根据 Flink 的 Watermark 推进规则，精确地决定何时以及以何值向操作符下游输出 Watermark 或 WatermarkStatus。
// 多路协调 (Min-Watermark): 在一个 Task 拥有多个输入分区（Subpartition）时，Watermark 的推进必须遵循木桶原理，即输出的 Watermark 必须等于所有活跃（Active）分区的 Watermark 中的最小值。
// 处理空闲状态 (Idle Handling): 利用 WatermarkStatus（ACTIVE 或 IDLE），它能够将任何处于 IDLE 状态的输入分区从 Watermark 最小值的计算中逻辑排除，从而防止空闲流的 Watermark 停滞整个 Task 的 Watermark 推进。
// ** Watermark 对齐管理:** 它维护了每个分区的 Watermark 是否已追上当前的最小 Watermark。只有 Watermark **对齐（Aligned）且状态为活跃（Active）**的分区，其 Watermark 才能参与最小值的计算。
// 最终 Watermark 刷新 (Flush): 当所有输入分区都变为 IDLE 时，它会输出一个最大 Watermark（所有分区中的最大值），以确保在 Task 停止处理前，所有基于时间的操作都能得到触发。
// StatusWatermarkValve 就像一个精密的协调器，管理着所有输入流的 Watermark 和活跃状态，以确保 Task 的 Watermark 在复杂的多流和空闲场景下能正确、及时地推进。
@Internal
public class StatusWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * The current status of all subpartitions. Changes as watermarks & watermark statuses are fed
     * into the valve.
     */
    // 所有输入分区的状态列表。
    // 存储每个输入通道及其所消费的子分区（Subpartition）的当前状态（包括 Watermark 值、WatermarkStatus 和对齐状态）
    // 列表的索引是channel index
    private final List<Map<Integer, SubpartitionStatus>> subpartitionStatuses;

    /**
     * The index of the subpartition consumed by an input channel, if the channel consumes only one
     * subpartition.
     */
    // 单子分区索引映射。 如果一个输入通道只消费一个子分区（常见情况），这个数组存储了该子分区的 ID。用于快速访问状态。
    private final int[] subpartitionIndexes;

    /** The last watermark emitted from the valve. */
    // 上次输出的 Watermark 时间戳。
    // 记录阀门上次成功向下游输出的 Watermark 值。用于过滤旧的 Watermark，并判断子分区是否已追平 Watermark
    private long lastOutputWatermark;

    /** The last watermark status emitted from the valve. */
    // 上次输出的 Watermark 状态。
    // 记录阀门上次输出的状态 (ACTIVE 或 IDLE)。用于判断整个阀门是否处于空闲状态。
    private WatermarkStatus lastOutputWatermarkStatus;

    /** A heap-based priority queue to help find the minimum watermark. */
    // 已对齐子分区的最小堆。
    // 这是一个基于堆的优先队列，
    // 仅包含那些 Watermark 已对齐且状态为活跃的子分区。
    // 堆顶元素即为当前所有活跃且对齐的 Watermark 中的最小值。
    private final HeapPriorityQueue<SubpartitionStatus> alignedSubpartitionStatuses;

    /** Whether there are multiple subpartitions transmitted through the same input channel. */
    // 输入通道是否共享标志
    private final boolean isInputChannelShared;

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    @VisibleForTesting
    public StatusWatermarkValve(int numInputChannels) {
        this(getIndexSets(numInputChannels));
    }

    private static ResultSubpartitionIndexSet[] getIndexSets(int numInputChannels) {
        ResultSubpartitionIndexSet[] subpartitionIndexRanges =
                new ResultSubpartitionIndexSet[numInputChannels];
        Arrays.fill(subpartitionIndexRanges, new ResultSubpartitionIndexSet(0));
        return subpartitionIndexRanges;
    }

    public StatusWatermarkValve(CheckpointedInputGate inputGate) {
        this(getIndexSets(inputGate));
    }

    private static ResultSubpartitionIndexSet[] getIndexSets(CheckpointedInputGate inputGate) {
        ResultSubpartitionIndexSet[] subpartitionIndexSets =
                new ResultSubpartitionIndexSet[inputGate.getNumberOfInputChannels()];
        for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
            subpartitionIndexSets[i] = inputGate.getChannel(i).getConsumedSubpartitionIndexSet();
        }
        return subpartitionIndexSets;
    }

    // 根据输入的子分区索引集合数组，初始化所有内部状态
    // 接受一个 ResultSubpartitionIndexSet 数组作为参数。
    // 这个数组中的每个元素代表一个输入通道所消费的一个或多个**结果子分区（Subpartition）**的集合
    public StatusWatermarkValve(ResultSubpartitionIndexSet[] subpartitionIndexSets) {
        // 计算总子分区数量
        int numSubpartitions = 0;
        for (ResultSubpartitionIndexSet subpartitionIndexSet : subpartitionIndexSets) {
            numSubpartitions += subpartitionIndexSet.size();
        }
        // 初始化 Watermark 最小堆 (Initialize Min-Watermark Heap)
        // 将总子分区数量作为最小堆的初始容量，以避免后续频繁扩容。
        this.alignedSubpartitionStatuses =
                new HeapPriorityQueue<>(
                        (left, right) -> Long.compare(left.watermark, right.watermark),
                        numSubpartitions);
        // 初始化一个列表，用于存储每个输入通道的状态映射
        this.subpartitionStatuses = new ArrayList<>(subpartitionIndexSets.length);
        this.subpartitionIndexes = new int[subpartitionIndexSets.length];
        Arrays.fill(subpartitionIndexes, -1);
        // 遍历并初始化每个子分区状态
        for (ResultSubpartitionIndexSet subpartitionIndexSet : subpartitionIndexSets) {
            // 为当前输入通道创建一个新的 Map，用于存储该通道所消费的子分区 ID 到其状态对象 (SubpartitionStatus) 的映射。
            Map<Integer, SubpartitionStatus> map = new HashMap<>();
            for (int subpartitionId : subpartitionIndexSet.values()) {
                SubpartitionStatus subpartitionStatus = new SubpartitionStatus();
                subpartitionStatus.watermark = Long.MIN_VALUE;
                subpartitionStatus.watermarkStatus = WatermarkStatus.ACTIVE;
                markWatermarkAligned(subpartitionStatus);
                map.put(subpartitionId, subpartitionStatus);
            }
            if (subpartitionIndexSet.size() == 1) {
                subpartitionIndexes[subpartitionStatuses.size()] =
                        subpartitionIndexSet.values().iterator().next();
            }
            this.subpartitionStatuses.add(map);
        }
        // 初始化阀门输出状态
        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;

        this.isInputChannelShared =
                Arrays.stream(subpartitionIndexSets).anyMatch(x -> x.size() > 1);
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     */
    // 负责处理传入的 Watermark 事件，并根据所有输入流的状态决定是否可以向下游推进 Watermark。
    // int channelIndex: Watermark 来自的输入通道的索引（从 0 开始）
    public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output)
            throws Exception {
        final SubpartitionStatus subpartitionStatus;
        // InternalWatermark 通常用于表示 Watermark 在多个子分区间共享输入通道的情况（虽然较少见，但 Flink 的网络层支持）
        if (watermark instanceof InternalWatermark) {
            int subpartitionStatusIndex = ((InternalWatermark) watermark).getSubpartitionIndex();
            subpartitionStatus =
                    subpartitionStatuses.get(channelIndex).get(subpartitionStatusIndex);
        } else {
            subpartitionStatus =
                    subpartitionStatuses.get(channelIndex).get(subpartitionIndexes[channelIndex]);
        }

        // ignore the input watermark if its subpartition, or all subpartitions are idle (i.e.
        // overall the valve is idle).
        // 检查当前子分区的状态是否为 ACTIVE。如果该子分区当前是 IDLE，那么它的 Watermark 变化不应该参与全局最小值的计算（直到它重新变为 ACTIVE）
        if (lastOutputWatermarkStatus.isActive() && subpartitionStatus.watermarkStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its
            // subpartition, ignore it also.
            // 检查新的 Watermark 是否大于该子分区之前收到的 Watermark。
            // 如果小于或等于（即 Watermark 倒退或重复），则根据 Watermark 必须单调递增的原则，直接忽略该 Watermark。
            if (watermarkMillis > subpartitionStatus.watermark) {
                subpartitionStatus.watermark = watermarkMillis;

                if (subpartitionStatus.isWatermarkAligned) {
                    // 调用方法调整它在最小堆中的位置。因为 Watermark 增大了，它在堆中的优先级（最小值）可能会改变
                    adjustAlignedSubpartitionStatuses(subpartitionStatus);
                } else if (watermarkMillis >= lastOutputWatermark) {
                    // 如果该子分区先前是未对齐的，但现在它的 Watermark 追上了全局的 lastOutputWatermark
                    // previously unaligned subpartitions are now aligned if its watermark has
                    // caught up
                    // 调用方法将其标记为已对齐
                    markWatermarkAligned(subpartitionStatus);
                }

                // now, attempt to find a new min watermark across all aligned subpartitions
                // 尝试推进全局 Watermark
                findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(output);
            }
        }
    }

    /**
     * Feed a {@link WatermarkStatus} into the valve. This may trigger the valve to output either a
     * new Watermark Status, for which {@link DataOutput#emitWatermarkStatus(WatermarkStatus)} will
     * be called, or a new Watermark, for which {@link DataOutput#emitWatermark(Watermark)} will be
     * called.
     *
     * @param watermarkStatus the watermark status to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark status belongs to (index
     *     starting from 0)
     */
    public void inputWatermarkStatus(
            WatermarkStatus watermarkStatus, int channelIndex, DataOutput<?> output)
            throws Exception {
        // Shared input channel is only enabled in batch jobs, which do not have watermark status
        // events.
        Preconditions.checkState(!isInputChannelShared);
        SubpartitionStatus subpartitionStatus =
                subpartitionStatuses.get(channelIndex).get(subpartitionIndexes[channelIndex]);

        // It is supposed that WatermarkStatus will not appear in jobs where one input channel
        // consumes multiple subpartitions, so we do not need to map channelIndex into
        // subpartitionStatusIndex for now, like what is done on Watermarks.

        // only account for watermark status inputs that will result in a status change for the
        // subpartition
        if (watermarkStatus.isIdle() && subpartitionStatus.watermarkStatus.isActive()) {
            // handle active -> idle toggle for the subpartition
            subpartitionStatus.watermarkStatus = WatermarkStatus.IDLE;

            // the subpartition is now idle, therefore not aligned
            markWatermarkUnaligned(subpartitionStatus);

            // if all subpartitions of the valve are now idle, we need to output an idle stream
            // status from the valve (this also marks the valve as idle)
            if (!SubpartitionStatus.hasActiveSubpartitions(subpartitionStatuses)) {

                // now that all subpartitions are idle and no subpartitions will continue to advance
                // its
                // watermark,
                // we should "flush" all watermarks across all subpartitions; effectively, this
                // means
                // emitting
                // the max watermark across all subpartitions as the new watermark. Also, since we
                // already try to advance
                // the min watermark as subpartitions individually become IDLE, here we only need to
                // perform the flush
                // if the watermark of the last active subpartition that just became idle is the
                // current
                // min watermark.
                if (subpartitionStatus.watermark == lastOutputWatermark) {
                    findAndOutputMaxWatermarkAcrossAllSubpartitions(output);
                }

                lastOutputWatermarkStatus = WatermarkStatus.IDLE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            } else if (subpartitionStatus.watermark == lastOutputWatermark) {
                // if the watermark of the subpartition that just became idle equals the last output
                // watermark (the previous overall min watermark), we may be able to find a new
                // min watermark from the remaining aligned subpartitions
                findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(output);
            }
        } else if (watermarkStatus.isActive() && subpartitionStatus.watermarkStatus.isIdle()) {
            // handle idle -> active toggle for the subpartition
            subpartitionStatus.watermarkStatus = WatermarkStatus.ACTIVE;

            // if the last watermark of the subpartition, before it was marked idle, is still
            // larger than
            // the overall last output watermark of the valve, then we can set the subpartition to
            // be
            // aligned already.
            if (subpartitionStatus.watermark >= lastOutputWatermark) {
                markWatermarkAligned(subpartitionStatus);
            }

            // if the valve was previously marked to be idle, mark it as active and output an active
            // stream
            // status because at least one of the subpartitions is now active
            if (lastOutputWatermarkStatus.isIdle()) {
                lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            }
        }
    }
    // 负责实际推进 Watermark 的核心逻辑
    private void findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(DataOutput<?> output)
            throws Exception {
        // 检查是否存在已对齐的子分区
        boolean hasAlignedSubpartitions = !alignedSubpartitionStatuses.isEmpty();

        // we acknowledge and output the new overall watermark if it really is aggregated
        // from some remaining aligned subpartition, and is also larger than the last output
        // watermark
        // 确保存在至少一个已对齐（Aligned）的子分区来计算最小 Watermark。
        // 获取最小堆的堆顶元素。根据最小堆的定义，这个元素的 Watermark 就是当前所有活跃且已对齐子分区 Watermark 中的最小值（即木桶最短的那个板）
        if (hasAlignedSubpartitions
                && alignedSubpartitionStatuses.peek().watermark > lastOutputWatermark) {
            // 更新全局 Watermark 并发送到下游
            lastOutputWatermark = alignedSubpartitionStatuses.peek().watermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * Mark the {@link SubpartitionStatus} as watermark-aligned and add it to the {@link
     * #alignedSubpartitionStatuses}.
     *
     * @param subpartitionStatus the subpartition status to be marked
     */
    // 把子分区SubpartitionStatus的水位线标志为已经对齐
    private void markWatermarkAligned(SubpartitionStatus subpartitionStatus) {
        if (!subpartitionStatus.isWatermarkAligned) {
            subpartitionStatus.isWatermarkAligned = true;
            subpartitionStatus.addTo(alignedSubpartitionStatuses);
        }
    }

    /**
     * Mark the {@link SubpartitionStatus} as watermark-unaligned and remove it from the {@link
     * #alignedSubpartitionStatuses}.
     *
     * @param subpartitionStatus the subpartition status to be marked
     */
    private void markWatermarkUnaligned(SubpartitionStatus subpartitionStatus) {
        if (subpartitionStatus.isWatermarkAligned) {
            subpartitionStatus.isWatermarkAligned = false;
            subpartitionStatus.removeFrom(alignedSubpartitionStatuses);
        }
    }

    /**
     * Adjust the {@link #alignedSubpartitionStatuses} when an element({@link SubpartitionStatus})
     * in it was modified. The {@link #alignedSubpartitionStatuses} is a priority queue, when an
     * element in it was modified, we need to adjust the element's position to ensure its priority
     * order.
     *
     * @param subpartitionStatus the modified subpartition status
     */
    // 调整最小堆的水位线
    private void adjustAlignedSubpartitionStatuses(SubpartitionStatus subpartitionStatus) {
        alignedSubpartitionStatuses.adjustModifiedElement(subpartitionStatus);
    }

    private void findAndOutputMaxWatermarkAcrossAllSubpartitions(DataOutput<?> output)
            throws Exception {
        long maxWatermark = Long.MIN_VALUE;

        for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
            for (SubpartitionStatus subpartitionStatus : map.values()) {
                maxWatermark = Math.max(subpartitionStatus.watermark, maxWatermark);
            }
        }

        if (maxWatermark > lastOutputWatermark) {
            lastOutputWatermark = maxWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * An {@code SubpartitionStatus} keeps track of a subpartition's last watermark, stream status,
     * and whether or not the subpartition's current watermark is aligned with the overall watermark
     * output from the valve.
     *
     * <p>There are 2 situations where a subpartition's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the subpartition is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the subpartition
     *       hasn't caught up to the last output watermark from the valve yet.
     * </ul>
     *
     * <p>NOTE: This class implements {@link HeapPriorityQueueElement} to be managed by {@link
     * #alignedSubpartitionStatuses} to help find minimum watermark.
     */
    @VisibleForTesting
    protected static class SubpartitionStatus implements HeapPriorityQueueElement {
        protected long watermark;
        protected WatermarkStatus watermarkStatus;
        // 水位线是否对齐
        protected boolean isWatermarkAligned;

        /**
         * This field holds the current physical index of this subpartition status when it is
         * managed by a {@link HeapPriorityQueue}.
         */
        private int heapIndex = HeapPriorityQueueElement.NOT_CONTAINED;

        /**
         * Utility to check if at least one subpartition in a given array of subpartitions is
         * active.
         */
        private static boolean hasActiveSubpartitions(
                List<Map<Integer, SubpartitionStatus>> subpartitionStatuses) {
            for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
                for (SubpartitionStatus status : map.values()) {
                    if (status.watermarkStatus.isActive()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int getInternalIndex() {
            return heapIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.heapIndex = newIndex;
        }

        private void removeFrom(HeapPriorityQueue<SubpartitionStatus> queue) {
            checkState(heapIndex != HeapPriorityQueueElement.NOT_CONTAINED);
            queue.remove(this);
            setInternalIndex(HeapPriorityQueueElement.NOT_CONTAINED);
        }

        private void addTo(HeapPriorityQueue<SubpartitionStatus> queue) {
            checkState(heapIndex == HeapPriorityQueueElement.NOT_CONTAINED);
            queue.add(this);
        }
    }

    @VisibleForTesting
    protected SubpartitionStatus getSubpartitionStatus(int subpartitionIndex) {
        for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
            Preconditions.checkState(
                    map.size() == 1,
                    "Cannot trigger this method when an input channel consumes multiple subpartition.");
        }

        Preconditions.checkArgument(
                subpartitionIndex >= 0 && subpartitionIndex < subpartitionStatuses.size(),
                "Invalid subpartition index. Number of subpartitions: "
                        + subpartitionStatuses.size());

        return subpartitionStatuses.get(subpartitionIndex).get(0);
    }
}
