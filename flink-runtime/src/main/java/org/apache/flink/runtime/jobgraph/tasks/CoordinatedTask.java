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
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

/**
 * An task that is coordinated, i.e. contains operators coordinated by {@link OperatorCoordinator}.
 */
// CoordinatedTask（协调任务）接口定义了任务（Task）如何与驻留在 JobManager 上的算子协调器 (OperatorCoordinator) 进行通信
// 事件分发通道： 它提供了一个统一的机制，允许 JobManager 上的 OperatorCoordinator 向其管理的、运行在 TaskManager 上的具体算子实例发送事件（OperatorEvent）
// 异步通信： 算子协调器是 Flink 1.11+ 引入的一种新的机制，旨在将复杂的状态管理、资源分配、数据源切分等逻辑从 JobManager 的主线程中解耦出来。
// CoordinatedTask 就是这个解耦通信模型的接收端。
// 它是一个任务级别上的事件收件箱，用于接收来自 JobManager 协调器的指令。
@Internal
public interface CoordinatedTask {
    // 分发算子事件
    void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException;
}
