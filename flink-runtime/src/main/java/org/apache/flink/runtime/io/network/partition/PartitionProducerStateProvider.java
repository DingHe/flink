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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.types.Either;

import java.util.function.Consumer;
// Flink 的网络栈（Network Stack）和任务执行/调度层之间扮演着至关重要的角色
// 核心功能是：
// 请求生产者状态： 允许数据的消费者（下游任务）查询其所需数据分区（ResultPartition）的**生产者（上游任务）**当前的执行状态 (ExecutionState)。
// 处理数据依赖： 在 Flink 的数据流模型中，下游任务（消费者）只有在上游任务（生产者）处于特定状态（例如 RUNNING 或 FINISHED）时才能开始消费数据。这个接口是实现这种数据依赖同步的机制。
/** Request execution state of partition producer, the response accepts state check callbacks. */
public interface PartitionProducerStateProvider {
    /**
     * Trigger the producer execution state request.
     *
     * @param intermediateDataSetId ID of the parent intermediate data set.
     * @param resultPartitionId ID of the result partition to check. This identifies the producing
     *     execution and partition.
     * @param responseConsumer consumer for the response handle.
     */
    // 触发生产者执行状态请求
    // 用于向 Flink 运行时请求指定数据分区的生产者的当前状态。
    void requestPartitionProducerState(
            IntermediateDataSetID intermediateDataSetId, // 标识这个分区所属的逻辑数据集，用于在 JobGraph 中定位依赖关系
            ResultPartitionID resultPartitionId, // 唯一标识正在被查询状态的具体数据分区，该 ID 包含了生产者的 ExecutionAttemptID 和分区索引
            Consumer<? super ResponseHandle> responseConsumer); // 回调函数（Consumer），当状态查询得到结果时，将返回一个 ResponseHandle 实例，由这个 Consumer 来处理

    /** Result of state query, accepts state check callbacks. */
    // 代表了对生产者状态查询的响应
    interface ResponseHandle {
        // 返回请求发起者（即当前正在尝试消费数据的任务）的执行状态。
        // 这主要用于在状态检查过程中提供上下文信息。
        ExecutionState getConsumerExecutionState();
        // 返回查询到的数据分区生产者的执行状态。
        Either<ExecutionState, Throwable> getProducerExecutionState();

        /** Cancel the partition consumptions as a result of state check. */
        // 作为状态检查的结果，
        // 如果发现生产者状态不适合继续消费（例如，生产者已经完成了它的输出，或者被取消了），消费者可以调用此方法来安全地终止其对该分区的消费尝试。
        void cancelConsumption();

        /**
         * Fail the partition consumptions as a result of state check.
         *
         * @param cause failure cause
         */
        // 作为状态检查的结果，如果发现生产者状态导致当前消费必须以失败告终（例如，生产者已经失败，并且消费者无法从其获取数据或状态），
        // 消费者可以调用此方法并提供原因 cause
        void failConsumption(Throwable cause);
    }
}
