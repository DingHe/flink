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

import java.io.IOException;
import java.util.Optional;

/** Interface for creating result partitions. */
// ResultPartitionProvider 的作用是作为下游任务（消费者）获取上游任务（生产者）输出数据的中央服务接口。
// 在 Flink 运行时环境中，这个接口的具体实现（通常在 TaskExecutor 或 NetworkEnvironment 中）负责以下职责：
// 定位与创建视图： 根据给定的 ResultPartitionID 找到对应的 Result Partition。
// 建立连接： 为下游任务创建 ResultSubpartitionView（即数据读取句柄），从而建立起数据传输的通道。
// 异步处理： 能够处理上游分区可能尚未注册或尚未完成的情况，通过注册监听器来等待分区可用。
// 简而言之，它解决了 “我（下游任务）需要从哪里（哪个 Result Partition）获取数据？” 这个问题，并负责建立实际的数据拉取通道。
public interface ResultPartitionProvider {

    /** Returns the requested intermediate result partition input view. */
    // 创建子分区视图（同步）。
    // 功能： 这是一个同步方法，用于立即获取请求的中间结果子分区视图。
    // 它假设所请求的分区是可用的
    ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet indexSet,
            BufferAvailabilityListener availabilityListener)
            throws IOException;

    /**
     * If the upstream task's partition has been registered, returns the result subpartition input
     * view immediately, otherwise register the listener and return empty.
     *
     * @param partitionId the result partition id
     * @param indexSet the index set
     * @param availabilityListener the buffer availability listener
     * @param partitionRequestListener the partition request listener
     * @return the result subpartition view
     * @throws IOException the thrown exception
     */
    // 创建子分区视图或注册监听器（异步/弹性）。
    // 功能： 这是一个弹性方法，用于处理上游分区尚未完全准备好的情况（例如，上游任务尚未启动或正在恢复）。
    // 核心逻辑： 1. 如果请求的分区立即可用，则立即创建并返回包含 ResultSubpartitionView 的 Optional。
    // 2. 如果请求的分区不可用，则注册 partitionRequestListener，等待分区注册，并返回空的 Optional
    Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet indexSet,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener partitionRequestListener)
            throws IOException;

    /**
     * Release the given listener in this result partition provider.
     *
     * @param listener the given listener
     */
    // 释放分区请求监听器。
    // 功能： 当下游任务决定不再等待某个分区（例如，任务本身失败、取消或超时）时，调用此方法来取消之前注册的 PartitionRequestListener
    void releasePartitionRequestListener(PartitionRequestListener listener);
}
