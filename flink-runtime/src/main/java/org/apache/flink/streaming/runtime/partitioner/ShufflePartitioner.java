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
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Random;

/**
 * Partitioner that distributes the data equally by selecting one output channel randomly.
 *
 * @param <T> Type of the Tuple
 */
// Flink 内部用于实现随机数据分发策略的核心组件。它决定了上游任务产生的每条数据记录应该被发送到下游任务的哪个并行子任务。
// 随机分配（Shuffle）: 它通过均匀随机地选择一个下游通道来分发数据。
// 负载均衡: 这种随机选择机制旨在实现近似的负载均衡。在大规模数据流中，由于随机性，每个下游子任务最终会接收到大致相同数量的记录。
// 消除偏斜: ShufflePartitioner 是解决数据倾斜（Data Skew）问题的最直接、最通用的方法，因为它不依赖于数据内容（如 Key）来做决定。
@Internal
public class ShufflePartitioner<T> extends StreamPartitioner<T> {
    private static final long serialVersionUID = 1L;

    private Random random = new Random();

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        return random.nextInt(numberOfChannels);
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ROUND_ROBIN;
    }

    @Override
    public StreamPartitioner<T> copy() {
        return new ShufflePartitioner<T>();
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "SHUFFLE";
    }
}
