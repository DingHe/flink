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

/**
 * Listener interface implemented by consumers of {@link ResultSubpartitionView} that want to be
 * notified of availability of further buffers.
 */
// Flink 网络栈中一个非常重要的回调接口，它连接了数据的生产者（ResultSubpartition）和消费者（网络线程或 Task 线程），实现了数据驱动的异步通知机制。
// 核心作用是实现 “生产者-消费者”模型中的异步通知机制，具体来说：
// 唤醒消费者： 当 ResultSubpartition（数据源头）接收到新的数据或事件时，它会调用该接口的方法，通知正在等待的消费者（通常是 Netty 的 I/O 线程或下游 Task 线程）“有新数据可以拉取了”。
// 避免忙等： 通过这种异步通知，消费者不需要持续轮询子分区来检查数据是否到达，从而避免了资源浪费和 CPU 忙等。

public interface BufferAvailabilityListener {

    /**
     * Called whenever there might be new data available.
     *
     * @param view the {@link ResultSubpartitionView} containing available data.
     */
    // 数据可用通知（核心方法）。
    // 调用时机： 当子分区（ResultSubpartition）收到新的数据 Buffer 或事件，并且该数据可以被下游消费时调用。
    // 目的： 唤醒正在等待的消费者，告知他们应该调用 view.pollBuffer() 来拉取新数据。
    void notifyDataAvailable(ResultSubpartitionView view);

    /**
     * Called when the first priority event is added to the head of the buffer queue.
     *
     * @param prioritySequenceNumber the sequence number that identifies the priority buffer.
     */
    // 优先级事件通知（默认方法）。
    // 调用时机： 当一个优先级事件（Priority Event，例如 Checkpoint 屏障或某些重要的控制事件）被添加到子分区队列的头部时调用。
    // 目的： 允许消费者对高优先级事件进行特殊处理或快速响应。
    default void notifyPriorityEvent(int prioritySequenceNumber) {}
}
