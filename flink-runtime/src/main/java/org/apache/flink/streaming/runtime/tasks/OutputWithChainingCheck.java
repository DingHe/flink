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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * This is a wrapper for outputs to check whether the collected record has been emitted to a
 * downstream subtask or to a chained operator.
 */
// 在 Flink 中，为了减少线程切换和数据序列化/反序列化的开销，多个连续的、没有重分区（shuffle）的操作符会被链在一起，作为同一个 Task 在同一个线程内顺序执行。
// OutputWithChainingCheck 的核心作用是：在数据从一个操作符发送到下一个操作符时，判断下一个接收者是在当前 Task 的操作符链内部，还是位于下游的另一个 Subtask（即需要通过网络传输）
@Internal
public interface OutputWithChainingCheck<OUT> extends WatermarkGaugeExposingOutput<OUT> {
    /**
     * @return true if the collected record has been emitted to a downstream subtask. Otherwise,
     *     false.
     */
    // 发送主输出流记录并检查是否需要通过网络发送。
    // 这是一个组合操作，取代了基础的 collect(OUT record)
    // true: 意味着该记录已被发射到下游的另一个 Subtask（即需要通过网络 I/O）
    boolean collectAndCheckIfChained(OUT record);

    /**
     * @return true if the collected record has been emitted to a downstream subtask. Otherwise,
     *     false.
     */
    // 发送侧输出流记录并检查是否需要通过网络发送。 这是一个用于侧输出的组合操作。
    // true: 侧输出记录被发射到下游的另一个 Subtask。
    <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record);
}
