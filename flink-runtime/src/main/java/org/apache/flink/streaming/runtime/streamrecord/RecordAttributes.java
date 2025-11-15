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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Experimental;

import java.util.Collections;
import java.util.Objects;

/**
 * A RecordAttributes describes the attributes of records from the current RecordAttributes until
 * the next one is received. It provides stream task with information that can be used to optimize
 * the stream task's performance.
 */
// 主要作用是在 Flink 的流数据中传递记录级别的运行时属性信息，以帮助下游的 StreamTask 或 StreamOperator 做出性能优化决策
// 标记流元素： RecordAttributes 继承自 StreamElement，表明它是一个特殊的流元素（类似于 Watermark、Checkpoint Barrier），它在数据记录之间流动。
// 属性生效范围： 一个 RecordAttributes 元素发出去后，它的属性对它之后直到下一个 RecordAttributes 元素出现之间的所有数据记录都有效。
// 优化指导： 它向算子提供运行时信息，算子可以利用这些信息来调整其处理策略，例如选择性地牺牲延迟以提高吞吐量。
@Experimental
public class RecordAttributes extends StreamElement {

    // 空记录属性常量。
    // 主要用作占位符或默认值。
    public static final RecordAttributes EMPTY_RECORD_ATTRIBUTES =
            new RecordAttributesBuilder(Collections.emptyList()).build();
    // 是否为积压数据标记
    // 指示紧随其后的数据记录（直到下一个 RecordAttributes）是否属于积压数据（Backlog），
    // 即这些数据是否是源头在短时间内大量发送的、可能导致延迟升高的历史数据或追赶数据。
    private final boolean isBacklog;

    public RecordAttributes(boolean isBacklog) {
        this.isBacklog = isBacklog;
    }

    /**
     * If it returns true, then the records received after this element are stale and an operator
     * can optionally buffer records until isBacklog=false. This allows an operator to optimize
     * throughput at the cost of processing latency.
     */
    public boolean isBacklog() {
        return isBacklog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordAttributes that = (RecordAttributes) o;
        return isBacklog == that.isBacklog;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isBacklog);
    }

    @Override
    public String toString() {
        return "RecordAttributes{" + "backlog=" + isBacklog + '}';
    }
}
