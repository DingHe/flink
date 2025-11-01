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

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface defining couple of essential methods for asynchronous and non blocking data polling.
 *
 * <p>For the most efficient usage, user of this class is supposed to call {@link #pollNext()} until
 * it returns that no more elements are available. If that happens, he should check if input {@link
 * #isFinished()}. If not, he should wait for {@link #getAvailableFuture()} {@link
 * CompletableFuture} to be completed. For example:
 *
 * <pre>{@code
 * AsyncDataInput<T> input = ...;
 * while (!input.isFinished()) {
 * 	Optional<T> next;
 *
 * 	while (true) {
 * 		next = input.pollNext();
 * 		if (!next.isPresent()) {
 * 			break;
 * 		}
 * 		// do something with next
 * 	}
 *
 * 	input.getAvailableFuture().get();
 * }
 * }</pre>
 */
// 为 Flink 内部的异步数据源（例如网络数据接收或某些异步 I/O 组件）提供一个标准的、基于拉取（Pull-based）、非阻塞的数据读取模型
// 计目标是实现最高效率的数据消费模式：
// 非阻塞拉取（pollNext()）： 消费者（如 Flink Task 线程）可以不断调用 pollNext() 来快速获取所有立即可用的数据，而不会被阻塞。
// 异步等待（getAvailableFuture()）： 当 pollNext() 返回空（表示当前没有数据）且输入未结束时，消费者应该通过等待 getAvailableFuture() 的完成来暂停，直到有新数据可用或状态发生变化。
// 它定义了 Flink 任务从上游组件（通常是网络输入网关或数据流）获取数据的高效、循环模式，是 Flink 响应式数据处理的关键抽象之一。
@Internal
public interface PullingAsyncDataInput<T> extends AvailabilityProvider {
    /**
     * Poll the next element. This method should be non blocking.
     *
     * @return {@code Optional.empty()} will be returned if there is no data to return or if {@link
     *     #isFinished()} returns true. Otherwise {@code Optional.of(element)}.
     */
    // 拉取下一个元素（非阻塞）。
    // 这是数据消费的核心方法，必须是非阻塞的。
    // 如果当前有数据立即可用，返回 Optional.of(element)；如果当前没有数据，返回 Optional.empty()
    Optional<T> pollNext() throws Exception;

    /** @return true if is finished and for example end of input was reached, false otherwise. */
    // 检查是否结束。 返回 true 表示该数据源已完成，并且不会再有新的数据元素传入
    boolean isFinished();

    /**
     * Tells if we consumed all available data.
     *
     * <p>Moreover it tells us the reason why there is no more data incoming. If any of the upstream
     * subtasks finished because of the stop-with-savepoint --no-drain, we should not drain the
     * input. See also {@code StopMode}.
     */
    // 获取数据结束状态
    EndOfDataStatus hasReceivedEndOfData();

    /** Status for describing if we have reached the end of data. */
    enum EndOfDataStatus {
        NOT_END_OF_DATA, // 尚未结束。 表示数据输入仍在进行中。
        DRAINED, // 正常耗尽/完成。 表示输入已正常处理完毕所有数据，且在结束前数据流已完全排空（Drain）
        STOPPED // 强制停止。 表示输入因为停止命令（例如 stop-with-savepoint --no-drain）而终止
    }
}
