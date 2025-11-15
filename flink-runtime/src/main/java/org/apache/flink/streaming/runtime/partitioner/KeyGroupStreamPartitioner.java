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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Partitioner selects the target channel based on the key group index.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
// KeyGroupStreamPartitioner 类是 Flink 流处理中最重要且最常用的数据分区器。
// 它实现了 Flink DataStream API 中的 .keyBy(...) 操作，
// 确保具有相同键（Key）的数据记录总会被路由到相同的下游任务实例，这对于状态管理和数据正确性至关重要。
// 核心作用是实现 Flink 的 **Keyed State（键控状态）**和 Key Group 机制 .
// 提取 Key： 使用用户提供的 KeySelector 从数据记录中提取 Key。
// 分配 Key Group： 根据 Key 的哈希值和作业的 maxParallelism（最大并行度），将该 Key 唯一地映射到一个 Key Group。
// 分配通道： 再根据当前下游的实际并行度 (numberOfChannels)，将该 Key Group 映射到下游的某一个子任务通道。
@Internal
public class KeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T>
        implements ConfigurableStreamPartitioner {
    private static final long serialVersionUID = 1L;
    // 键提取器
    private final KeySelector<T, K> keySelector;
    // 最大并行度。也称为 Key Group 总数。
    // 这是作业启动时配置的最大并行度（默认为 $2^{7}$ 或 $2^{15}$）。Key Groups 的数量是固定的，它定义了 Key $\to$ Key Group 的映射范围。
    private int maxParallelism;

    public KeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
        Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
        this.keySelector = Preconditions.checkNotNull(keySelector);
        this.maxParallelism = maxParallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        K key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                key, maxParallelism, numberOfChannels);
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.RANGE;
    }

    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "HASH";
    }

    @Override
    public void configure(int maxParallelism) {
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        this.maxParallelism = maxParallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final KeyGroupStreamPartitioner<?, ?> that = (KeyGroupStreamPartitioner<?, ?>) o;
        return maxParallelism == that.maxParallelism && keySelector.equals(that.keySelector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keySelector, maxParallelism);
    }
}
