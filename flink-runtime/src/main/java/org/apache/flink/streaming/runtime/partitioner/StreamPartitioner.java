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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Objects;

/** A special {@link ChannelSelector} for use in streaming programs. */
// StreamPartitioner 是 Flink 流处理中数据分发策略的抽象基类。
// 它在逻辑上对应于用户在 Flink DataStream API 中调用的 .keyBy(), .rebalance(), .broadcast() 等操作。
// 核心作用是定义并实现数据记录 StreamRecord<T> 从一个上游算子任务发送到下游算子任务时的路由规则 .
// 核心意义：
// 实现用户定义的分区策略： 它是 Flink API 中 .keyBy() 等方法在运行时层面的实现，决定了数据在 TaskManager 之间如何流动。
// 网络 I/O 适配器： 它将 Flink 流处理的概念（如 StreamRecord）适配到底层的网络 I/O 接口 (ChannelSelector)。
// 支持弹性伸缩： 它引入了 SubtaskStateMapper 概念，使 Flink 能够在作业运行过程中，在不中断流的情况下，安全地改变任务的并行度（Scaling Up/Down）。


@Internal
public abstract class StreamPartitioner<T>
        implements ChannelSelector<SerializationDelegate<StreamRecord<T>>>, Serializable {
    private static final long serialVersionUID = 1L;
    // 下游通道数量。
    // 表示数据可以发送到的下游并行实例的总数。这个值在 setup() 方法中初始化。
    protected int numberOfChannels;

    @Override
    public void setup(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public boolean isBroadcast() {
        return false;
    }

    public abstract StreamPartitioner<T> copy();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamPartitioner<?> that = (StreamPartitioner<?>) o;
        return numberOfChannels == that.numberOfChannels;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfChannels);
    }

    /**
     * Defines the behavior of this partitioner, when upstream rescaled during recovery of in-flight
     * data.
     */
    // 上游子任务状态映射器
    // 定义当上游任务（Producer）的并行度发生变化时，上游任务的通道状态如何重新分配。
    public SubtaskStateMapper getUpstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ARBITRARY;
    }

    /**
     * Defines the behavior of this partitioner, when downstream rescaled during recovery of
     * in-flight data.
     */
    // 下游子任务状态映射器
    // 定义当下游任务（Consumer）的并行度发生变化时（Rescaling），与本分区器相关联的通道状态如何在新旧并行实例之间进行映射和分配。
    public abstract SubtaskStateMapper getDownstreamSubtaskStateMapper();
    // 是否为点对点分发
    // 指示该分区器是否将数据记录发送到固定且唯一的下游通道。
    public abstract boolean isPointwise();
}
