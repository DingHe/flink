/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.List;

/**
 * Interface for strategies that decide when to release {@link ConsumedPartitionGroup
 * ConsumedPartitionGroups}.
 */
// 这个接口定义了决定何时可以释放（即不再需要并可以清理资源）一组被消费分区（ConsumedPartitionGroup）的策略。
// 在 Flink 中，当一个上游任务产生数据后，下游任务开始消费。
// 一旦所有消费该分区组的下游任务都完成执行（或进入“可重新执行”之外的状态），
// 那么上游产生的中间结果数据就可以被释放，从而节约 TaskManager 上的网络缓冲区或磁盘空间。
public interface PartitionGroupReleaseStrategy {

    /**
     * Calling this method informs the strategy that a vertex finished.
     *
     * @param finishedVertex Id of the vertex that finished the execution
     * @return A list of {@link ConsumedPartitionGroup ConsumedPartitionGroups} that can be released
     */
    // 顶点完成通知。
    // 当一个下游执行顶点完成执行（无论是成功还是失败）时，调用此方法通知策略。
    // 策略会检查该顶点的所有输入分区组，
    // 看是否因为该顶点是最后一个消费者而使这些分区组可以被释放。
    // 返回值是所有可以被安全释放的分区组列表。
    List<ConsumedPartitionGroup> vertexFinished(ExecutionVertexID finishedVertex);

    /**
     * Calling this method informs the strategy that a vertex is no longer in finished state, e.g.,
     * when a vertex is re-executed.
     *
     * @param executionVertexID Id of the vertex that is no longer in finished state.
     */
    // 顶点取消完成状态通知。
    // 当一个执行顶点不再处于完成状态时（例如，因为故障恢复需要重新执行），调用此方法。
    // 策略需要将该顶点重新标记为“未完成”，
    // 这可能会导致之前本应被释放的分区组不能被释放（因为现在它又需要消费数据了）。
    void vertexUnfinished(ExecutionVertexID executionVertexID);

    /** Factory for {@link PartitionGroupReleaseStrategy}. */
    // 工厂接口。 定义了创建 PartitionGroupReleaseStrategy 实例的标准方式。
    interface Factory {
        PartitionGroupReleaseStrategy createInstance(SchedulingTopology schedulingStrategy);
    }
}
