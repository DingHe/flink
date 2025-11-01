/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Captures ambiguous mappings of old channels to new channels for a particular gate or partition.
 *
 * @see InflightDataGateOrPartitionRescalingDescriptor
 */
// 封装了在 Flink 并行度发生变化（伸缩）时，如何将任务的“飞行中数据”（In-flight Data，即通道状态）从旧的通道/任务分配给新的通道/任务的完整映射信息。
// 当 Flink 任务的并行度发生变化（例如，从 2 个任务扩展到 3 个任务）并使用非对齐检查点恢复时：
// 旧任务保存的状态（飞行中数据）必须被新的任务正确加载。
// 这个过程涉及到旧任务（Old Subtask）、旧通道（Old Channel）、**新任务（New Subtask）和新通道（New Channel）**之间的复杂多对多映射。
public class InflightDataRescalingDescriptor implements Serializable {
    // 表示当前任务没有发生并行度伸缩
    public static final InflightDataRescalingDescriptor NO_RESCALE = new NoRescalingDescriptor();

    private static final long serialVersionUID = -3396674344669796295L;

    /** Set when several operator instances are merged into one. */
    // 网关或分区描述符数组。
    // 这是核心属性，它包含了任务的每个**输入网关（Gate）或输出结果分区（Partition）**的重分配细节
    private final InflightDataGateOrPartitionRescalingDescriptor[] gateOrPartitionDescriptors;

    public InflightDataRescalingDescriptor(
            InflightDataGateOrPartitionRescalingDescriptor[] gateOrPartitionDescriptors) {
        this.gateOrPartitionDescriptors = checkNotNull(gateOrPartitionDescriptors);
    }
    // 获取旧任务索引列表
    public int[] getOldSubtaskIndexes(int gateOrPartitionIndex) {
        return gateOrPartitionDescriptors[gateOrPartitionIndex].oldSubtaskIndexes;
    }
    // 获取通道映射。
    // 返回一个 RescaleMappings 对象，它描述了旧通道到新通道**之间的映射关系
    public RescaleMappings getChannelMapping(int gateOrPartitionIndex) {
        return gateOrPartitionDescriptors[gateOrPartitionIndex].rescaledChannelsMappings;
    }

    public boolean isAmbiguous(int gateOrPartitionIndex, int oldSubtaskIndex) {
        return gateOrPartitionDescriptors[gateOrPartitionIndex].ambiguousSubtaskIndexes.contains(
                oldSubtaskIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InflightDataRescalingDescriptor that = (InflightDataRescalingDescriptor) o;
        return Arrays.equals(gateOrPartitionDescriptors, that.gateOrPartitionDescriptors);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(gateOrPartitionDescriptors);
    }

    @Override
    public String toString() {
        return "InflightDataRescalingDescriptor{"
                + "gateOrPartitionDescriptors="
                + Arrays.toString(gateOrPartitionDescriptors)
                + '}';
    }

    /**
     * Captures ambiguous mappings of old channels to new channels.
     *
     * <p>For inputs, this mapping implies the following:
     * <li>
     *
     *     <ul>
     *       {@link #oldSubtaskIndexes} is set when there is a rescale on this task potentially
     *       leading to different key groups. Upstream task has a corresponding {@link
     *       #rescaledChannelsMappings} where it sends data over virtual channel while specifying
     *       the channel index in the VirtualChannelSelector. This subtask then demultiplexes over
     *       the virtual subtask index.
     * </ul>
     *
     * <ul>
     *   {@link #rescaledChannelsMappings} is set when there is a downscale of the upstream task.
     *   Upstream task has a corresponding {@link #oldSubtaskIndexes} where it sends data over
     *   virtual channel while specifying the subtask index in the VirtualChannelSelector. This
     *   subtask then demultiplexes over channel indexes.
     * </ul>
     *
     * <p>For outputs, it's vice-versa. The information must be kept in sync but they are used in
     * opposite ways for multiplexing/demultiplexing.
     *
     * <p>Note that in the common rescaling case both information is set and need to be
     * simultaneously used. If the input subtask subsumes the state of 3 old subtasks and a channel
     * corresponds to 2 old channels, then there are 6 virtual channels to be demultiplexed.
     */
    // 封装了单个输入网关或输出分区的重分配信息
    public static class InflightDataGateOrPartitionRescalingDescriptor implements Serializable {

        private static final long serialVersionUID = 1L;

        /** Set when several operator instances are merged into one. */
        // 旧任务索引。
        // 当前新任务合并的（即承担了状态的）所有旧任务的索引列表。
        private final int[] oldSubtaskIndexes;

        /**
         * Set when channels are merged because the connected operator has been rescaled for each
         * gate/partition.
         */
        // 通道映射对象。 包含了旧通道到新通道的详细映射表
        private final RescaleMappings rescaledChannelsMappings;

        /** All channels where upstream duplicates data (only valid for downstream mappings). */
        // 模糊任务索引集合。
        // 包含了所有可能导致数据重复的旧任务索引集合，仅在某些 Downstream 映射中有效。
        private final Set<Integer> ambiguousSubtaskIndexes;
        // 映射类型。
        // 指示当前映射是 IDENTITY（恒等/无伸缩）还是 RESCALING（伸缩）
        private final MappingType mappingType;

        /** Type of mapping which should be used for this in-flight data. */
        public enum MappingType {
            IDENTITY,
            RESCALING
        }

        public InflightDataGateOrPartitionRescalingDescriptor(
                int[] oldSubtaskIndexes,
                RescaleMappings rescaledChannelsMappings,
                Set<Integer> ambiguousSubtaskIndexes,
                MappingType mappingType) {
            this.oldSubtaskIndexes = oldSubtaskIndexes;
            this.rescaledChannelsMappings = rescaledChannelsMappings;
            this.ambiguousSubtaskIndexes = ambiguousSubtaskIndexes;
            this.mappingType = mappingType;
        }

        public boolean isIdentity() {
            return mappingType == MappingType.IDENTITY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InflightDataGateOrPartitionRescalingDescriptor that =
                    (InflightDataGateOrPartitionRescalingDescriptor) o;
            return Arrays.equals(oldSubtaskIndexes, that.oldSubtaskIndexes)
                    && Objects.equals(rescaledChannelsMappings, that.rescaledChannelsMappings)
                    && Objects.equals(ambiguousSubtaskIndexes, that.ambiguousSubtaskIndexes)
                    && mappingType == that.mappingType;
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(rescaledChannelsMappings, ambiguousSubtaskIndexes, mappingType);
            result = 31 * result + Arrays.hashCode(oldSubtaskIndexes);
            return result;
        }

        @Override
        public String toString() {
            return "InflightDataGateOrPartitionRescalingDescriptor{"
                    + "oldSubtaskIndexes="
                    + Arrays.toString(oldSubtaskIndexes)
                    + ", rescaledChannelsMappings="
                    + rescaledChannelsMappings
                    + ", ambiguousSubtaskIndexes="
                    + ambiguousSubtaskIndexes
                    + ", mappingType="
                    + mappingType
                    + '}';
        }
    }

    private static class NoRescalingDescriptor extends InflightDataRescalingDescriptor {
        private static final long serialVersionUID = 1L;

        public NoRescalingDescriptor() {
            super(new InflightDataGateOrPartitionRescalingDescriptor[0]);
        }

        @Override
        public int[] getOldSubtaskIndexes(int gateOrPartitionIndex) {
            return new int[0];
        }

        @Override
        public RescaleMappings getChannelMapping(int gateOrPartitionIndex) {
            return RescaleMappings.SYMMETRIC_IDENTITY;
        }

        @Override
        public boolean isAmbiguous(int gateOrPartitionIndex, int oldSubtaskIndex) {
            return false;
        }

        private Object readResolve() throws ObjectStreamException {
            return NO_RESCALE;
        }
    }
}
