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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Options for performing the checkpoint. Note that different {@link
 * org.apache.flink.runtime.io.network.api.CheckpointBarrier barriers} may have different options.
 *
 * <p>The {@link CheckpointProperties} are related and cover properties that are only relevant at
 * the {@link CheckpointCoordinator}. These options are relevant at the {@link AbstractInvokable}
 * instances running on task managers.
 */
// CheckpointOptions 类封装了在 Flink 运行时执行一次检查点（Checkpoint）所需的所有指令和配置。
// 它不是给 JobMaster (CheckpointCoordinator) 用的配置（那部分由 CheckpointProperties 负责），
// 而是专门针对运行在 TaskManager 上的各个 Task 实例 (AbstractInvokable) 发出的指令。
// 传达执行意图： 当 CheckpointBarrier 流经数据流时，它会携带一个 CheckpointOptions 实例。这个实例告诉接收屏障的 Task：要做什么类型的快照 (Type)，状态应该存到哪里 (Location)，以及应该以什么方式对齐数据流 (Alignment)。
// 控制对齐行为： 尤其重要的是，它定义了检查点在面对反压时应如何处理数据流的对齐（Alignment），这直接关系到检查点的一致性保证（Exactly-Once 或 At-Least-Once）。

public class CheckpointOptions implements Serializable {

    /** How a checkpoint should be aligned. */
    // 检查点对齐方式。
    // 定义了数据流在处理检查点屏障时的处理方式，影响一致性保证和性能。
    public enum AlignmentType {
        // 至少一次语义。
        // 不需要等待所有输入流都到达屏障，直接快照状态，跳过任何数据缓冲或对齐逻辑。
        AT_LEAST_ONCE,
        // 精确一次语义（经典对齐）。
        // 任务必须等待所有输入流都接收到检查点屏障后才能继续处理数据并进行状态快照。
        // 这是传统的 Flink Exactly-Once 模式。
        ALIGNED,
        // 非对齐检查点。 任务在收到屏障后立即快照状态，同时将通道中（in-flight）的待处理数据作为状态的一部分进行保存。
        // 这消除了反压导致的对齐停顿，但需要保存通道状态 (Channel State)。
        UNALIGNED,
        // 强制对齐。
        // 类似于 ALIGNED，但它不能超时降级到非对齐模式。主要用于确保 Savepoint 总是完全对齐的。
        FORCED_ALIGNED
    }

    public static final long NO_ALIGNED_CHECKPOINT_TIME_OUT = Long.MAX_VALUE;

    private static final long serialVersionUID = 5010126558083292915L;

    /** Type of the checkpoint. */
    // 快照类型。
    // 指定本次快照是常规检查点 (CheckpointType.CHECKPOINT) 还是保存点 (CheckpointType.SAVEPOINT) 等。
    private final SnapshotType checkpointType;

    /** Target location for the checkpoint. */
    // 目标位置引用。
    // 指示状态存储后端应该将本次快照存储到哪个目标位置（通常是远程文件系统路径）。
    // 它是一个引用，不包含实际的存储 I/O 逻辑。
    private final CheckpointStorageLocationReference targetLocation;
    // 对齐方式。
    // 存储本次检查点应采用的具体对齐策略（来自 AlignmentType 枚举）。
    private final AlignmentType alignmentType;
    // 对齐超时时间。
    // 仅在 ALIGNED 模式下有意义。
    // 如果在指定的毫秒时间内未能完成对齐，任务可以选择放弃对齐，转而采用 AT_LEAST_ONCE 语义（或 UNALIGNED，取决于配置）。
    private final long alignedCheckpointTimeout;

    public static CheckpointOptions notExactlyOnce(
            SnapshotType type, CheckpointStorageLocationReference location) {
        return new CheckpointOptions(
                type, location, AlignmentType.AT_LEAST_ONCE, NO_ALIGNED_CHECKPOINT_TIME_OUT);
    }

    public static CheckpointOptions alignedNoTimeout(
            SnapshotType type, CheckpointStorageLocationReference location) {
        return new CheckpointOptions(
                type, location, AlignmentType.ALIGNED, NO_ALIGNED_CHECKPOINT_TIME_OUT);
    }

    public static CheckpointOptions unaligned(
            SnapshotType type, CheckpointStorageLocationReference location) {
        checkArgument(!type.isSavepoint(), "Savepoints can not be unaligned");
        return new CheckpointOptions(
                type, location, AlignmentType.UNALIGNED, NO_ALIGNED_CHECKPOINT_TIME_OUT);
    }

    public static CheckpointOptions alignedWithTimeout(
            SnapshotType type,
            CheckpointStorageLocationReference location,
            long alignedCheckpointTimeout) {
        checkArgument(!type.isSavepoint(), "Savepoints can not be unaligned");
        return new CheckpointOptions(
                type, location, AlignmentType.ALIGNED, alignedCheckpointTimeout);
    }

    private static CheckpointOptions forceAligned(
            SnapshotType type,
            CheckpointStorageLocationReference location,
            long alignedCheckpointTimeout) {
        checkArgument(!type.isSavepoint(), "Savepoints can not be unaligned");
        return new CheckpointOptions(
                type, location, AlignmentType.FORCED_ALIGNED, alignedCheckpointTimeout);
    }

    public static CheckpointOptions forConfig(
            SnapshotType checkpointType,
            CheckpointStorageLocationReference locationReference,
            boolean isExactlyOnceMode,
            boolean isUnalignedEnabled,
            long alignedCheckpointTimeout) {
        if (!isExactlyOnceMode) {
            return notExactlyOnce(checkpointType, locationReference);
        } else if (checkpointType.isSavepoint()) {
            return alignedNoTimeout(checkpointType, locationReference);
        } else if (!isUnalignedEnabled) {
            return alignedNoTimeout(checkpointType, locationReference);
        } else if (alignedCheckpointTimeout == 0
                || alignedCheckpointTimeout == NO_ALIGNED_CHECKPOINT_TIME_OUT) {
            return unaligned(checkpointType, locationReference);
        } else {
            return alignedWithTimeout(checkpointType, locationReference, alignedCheckpointTimeout);
        }
    }

    @VisibleForTesting
    public CheckpointOptions(
            SnapshotType checkpointType, CheckpointStorageLocationReference targetLocation) {
        this(checkpointType, targetLocation, AlignmentType.ALIGNED, NO_ALIGNED_CHECKPOINT_TIME_OUT);
    }

    public CheckpointOptions(
            SnapshotType checkpointType,
            CheckpointStorageLocationReference targetLocation,
            AlignmentType alignmentType,
            long alignedCheckpointTimeout) {

        checkArgument(
                alignmentType != AlignmentType.UNALIGNED || !checkpointType.isSavepoint(),
                "Savepoint can't be unaligned");
        checkArgument(
                alignedCheckpointTimeout == NO_ALIGNED_CHECKPOINT_TIME_OUT
                        || alignmentType != AlignmentType.UNALIGNED,
                "Unaligned checkpoint can't have timeout (%s)",
                alignedCheckpointTimeout);
        this.checkpointType = checkNotNull(checkpointType);
        this.targetLocation = checkNotNull(targetLocation);
        this.alignmentType = checkNotNull(alignmentType);
        this.alignedCheckpointTimeout = alignedCheckpointTimeout;
    }

    public boolean needsAlignment() {
        return isExactlyOnceMode()
                && (getCheckpointType().isSavepoint() || !isUnalignedCheckpoint());
    }

    public long getAlignedCheckpointTimeout() {
        return alignedCheckpointTimeout;
    }

    public AlignmentType getAlignment() {
        return alignmentType;
    }

    public boolean isTimeoutable() {
        if (alignmentType == AlignmentType.FORCED_ALIGNED) {
            return false;
        }
        return alignmentType == AlignmentType.ALIGNED
                && (alignedCheckpointTimeout > 0
                        && alignedCheckpointTimeout != NO_ALIGNED_CHECKPOINT_TIME_OUT);
    }

    // ------------------------------------------------------------------------

    /** Returns the type of checkpoint to perform. */
    public SnapshotType getCheckpointType() {
        return checkpointType;
    }

    /** Returns the target location for the checkpoint. */
    public CheckpointStorageLocationReference getTargetLocation() {
        return targetLocation;
    }

    public boolean isExactlyOnceMode() {
        return alignmentType != AlignmentType.AT_LEAST_ONCE;
    }

    public boolean isUnalignedCheckpoint() {
        return alignmentType == AlignmentType.UNALIGNED;
    }

    public boolean needsChannelState() {
        return isUnalignedCheckpoint() || isTimeoutable();
    }

    public CheckpointOptions withUnalignedSupported() {
        if (alignmentType == AlignmentType.FORCED_ALIGNED) {
            return alignedCheckpointTimeout != NO_ALIGNED_CHECKPOINT_TIME_OUT
                    ? alignedWithTimeout(checkpointType, targetLocation, alignedCheckpointTimeout)
                    : unaligned(checkpointType, targetLocation);
        }
        return this;
    }

    public CheckpointOptions withUnalignedUnsupported() {
        if (needsChannelState()) {
            return forceAligned(checkpointType, targetLocation, alignedCheckpointTimeout);
        }
        return this;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(
                targetLocation, checkpointType, alignmentType, alignedCheckpointTimeout);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj != null && obj.getClass() == CheckpointOptions.class) {
            final CheckpointOptions that = (CheckpointOptions) obj;
            return this.checkpointType.equals(that.checkpointType)
                    && this.targetLocation.equals(that.targetLocation)
                    && this.alignmentType == that.alignmentType
                    && this.alignedCheckpointTimeout == that.alignedCheckpointTimeout;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "CheckpointOptions{"
                + "checkpointType="
                + checkpointType
                + ", targetLocation="
                + targetLocation
                + ", alignmentType="
                + alignmentType
                + ", alignedCheckpointTimeout="
                + alignedCheckpointTimeout
                + '}';
    }
    // ------------------------------------------------------------------------
    //  Factory methods
    // ------------------------------------------------------------------------

    private static final CheckpointOptions CHECKPOINT_AT_DEFAULT_LOCATION =
            new CheckpointOptions(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());

    @VisibleForTesting
    public static CheckpointOptions forCheckpointWithDefaultLocation() {
        return CHECKPOINT_AT_DEFAULT_LOCATION;
    }

    public CheckpointOptions toUnaligned() {
        checkState(alignmentType == AlignmentType.ALIGNED);
        return unaligned(checkpointType, targetLocation);
    }
}
