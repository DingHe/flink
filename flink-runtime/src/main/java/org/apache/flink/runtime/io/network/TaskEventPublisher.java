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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;

/**
 * The task event publisher is used for publishing the event to the registered {@link EventListener}
 * instances.
 */
// TaskEventPublisher（任务事件发布者）是 Flink 运行时网络 I/O 模块中的一个接口，它负责在 Flink 任务执行期间分发和通知特定的任务事件 (TaskEvent)
// 应用场景： 这在 Flink 的网络数据交换中尤为重要。例如，当一个任务的数据生成端（上游）需要通知其数据消费端（下游）某些状态变化或控制信号时，就会通过这个机制进行。这些事件通常与数据传输的生命周期或控制流相关。
public interface TaskEventPublisher {

    /**
     * Publishes the event to the registered {@link EventListener} instances.
     *
     * @param partitionId the partition ID to get registered handlers
     * @param event the task event to be published to the handlers
     * @return whether the event was published to a registered event handler or not
     */
    boolean publish(ResultPartitionID partitionId, TaskEvent event);
}
