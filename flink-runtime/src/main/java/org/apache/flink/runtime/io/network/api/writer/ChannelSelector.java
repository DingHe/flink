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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * The {@link ChannelSelector} determines to which logical channels a record should be written to.
 *
 * @param <T> the type of record which is sent through the attached output gate
 */
// ChannelSelector（通道选择器）是 Flink 实现数据重分区（Redistribution）策略的核心抽象。
// 它运行在数据发送端（即上游任务的输出），根据每个数据记录的内容或既定的规则，决定将该记录发送给下游任务的哪一个或哪一组并行实例。
// 数据路由决策： 封装了从一个上游子任务到所有下游子任务的网络通道之间的映射关系。
// 实现分区策略： 所有的 Flink 数据分区器（Partitioner），如 KeyGroupStreamPartitioner (Keyed/Shuffle)、RebalancePartitioner (Rebalance)、BroadcastPartitioner (Broadcast) 等，都在底层通过实现或使用 ChannelSelector 来实现其分发逻辑。
// 支持广播和单通道选择： 区分了单通道选择（如 Keyed 模式）和全通道选择（如 Broadcast 模式）。
public interface ChannelSelector<T extends IOReadableWritable> {

    /**
     * Initializes the channel selector with the number of output channels.
     *
     * @param numberOfChannels the total number of output channels which are attached to respective
     *     output gate.
     */
    // 用于在通道选择器开始工作之前，告知它下游连接的总输出通道数量。
    void setup(int numberOfChannels);

    /**
     * Returns the logical channel index, to which the given record should be written. It is illegal
     * to call this method for broadcast channel selectors and this method can remain not
     * implemented in that case (for example by throwing {@link UnsupportedOperationException}).
     *
     * @param record the record to determine the output channels for.
     * @return an integer number which indicates the index of the output channel through which the
     *     record shall be forwarded.
     */
    // 单通道选择
    // 为给定的单个数据记录 record 确定唯一的目标输出通道索引。
    int selectChannel(T record);

    /**
     * Returns whether the channel selector always selects all the output channels.
     *
     * @return true if the selector is for broadcast mode.
     */
    // 是否为广播模式
    // 检查该通道选择器是否为广播模式。
    // true 表示该选择器会将每条记录发送给所有下游通道（全通道选择），false 表示它只选择单个或部分通道。
    boolean isBroadcast();
}
