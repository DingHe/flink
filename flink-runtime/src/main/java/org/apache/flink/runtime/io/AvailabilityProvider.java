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

import java.util.concurrent.CompletableFuture;

/**
 * Interface defining couple of essential methods for listening on data availability using {@link
 * CompletableFuture}. For usage check out for example {@link PullingAsyncDataInput}.
 */
// 提供一种高性能、基于 CompletableFuture 的机制来跟踪和通知某个组件（如数据源、网络输入通道或缓冲区）是否“可用”（Available），即是否准备好进行下一步处理或是否有数据可供拉取。
// 在 Flink 异步拉取数据（Pull-based）的组件中（例如 PullingAsyncDataInput）：
// 统一可用性模型： 无论组件是否立即可用，它都返回一个 CompletableFuture。
// 如果立即可用： 返回一个已完成的 Future（常量 AVAILABLE）
// 如果不立即可用： 返回一个未完成的 Future，当组件变为可用时，该 Future 会被完成（Complete）


@Internal
public interface AvailabilityProvider {
    /**
     * Constant that allows to avoid volatile checks {@link CompletableFuture#isDone()}. Check
     * {@link #isAvailable()} and {@link #isApproximatelyAvailable()} for more explanation.
     */
    // 常量：立即可用。 这是一个已经完成（completed）的 CompletableFuture 实例。它用于表示提供者当前立即可用的状态
    CompletableFuture<?> AVAILABLE = CompletableFuture.completedFuture(null);

    /** @return a future that is completed if the respective provider is available.*/
    // 如果提供者当前可用，则返回 AVAILABLE；如果不可用，则返回一个未完成的 Future，当提供者变为可用时，该 Future 将被完成
    CompletableFuture<?> getAvailableFuture();

    /**
     * In order to best-effort avoid volatile access in {@link CompletableFuture#isDone()}, we check
     * the condition of <code>future == AVAILABLE</code> firstly for getting probable performance
     * benefits while hot looping.
     *
     * <p>It is always safe to use this method in performance nonsensitive scenarios to get the
     * precise state.
     *
     * @return true if this instance is available for further processing.
     */
    // 精确判断是否可用
    // 首先检查 getAvailableFuture() 是否是 AVAILABLE 常量；如果不是，则调用 future.isDone() 来获取精确的完成状态
    default boolean isAvailable() {
        CompletableFuture<?> future = getAvailableFuture();
        return future == AVAILABLE || future.isDone();
    }

    /**
     * Checks whether this instance is available only via constant {@link #AVAILABLE} to avoid
     * performance concern caused by volatile access in {@link CompletableFuture#isDone()}. So it is
     * mainly used in the performance sensitive scenarios which do not always need the precise
     * state.
     *
     * <p>This method is still safe to get the precise state if {@link #getAvailableFuture()} was
     * touched via (.get(), .wait(), .isDone(), ...) before, which also has a "happen-before"
     * relationship with this call.
     * @return true if this instance is available for further processing.
     */
    // 近似判断是否可用（性能优化）
    // 只检查 getAvailableFuture() 是否等于 AVAILABLE 常量。它避免了调用 isDone() 产生的 volatile 访问开销，适用于性能敏感的热循环场景。
    default boolean isApproximatelyAvailable() {
        return getAvailableFuture() == AVAILABLE;
    }

    // 逻辑 AND 组合。
    // 接收两个 CompletableFuture，返回一个新的 Future，该 Future 仅在两者都完成时才会完成
    static CompletableFuture<?> and(CompletableFuture<?> first, CompletableFuture<?> second) {
        if (first == AVAILABLE && second == AVAILABLE) {
            return AVAILABLE;
        } else if (first == AVAILABLE) {
            return second;
        } else if (second == AVAILABLE) {
            return first;
        } else {
            return CompletableFuture.allOf(first, second);
        }
    }

   // 逻辑 OR 组合。 静态方法
   // 接收两个 CompletableFuture，返回一个新的 Future，该 Future 在两者中任一完成时就会完成
    static CompletableFuture<?> or(CompletableFuture<?> first, CompletableFuture<?> second) {
        if (first == AVAILABLE || second == AVAILABLE) {
            return AVAILABLE;
        }
        return CompletableFuture.anyOf(first, second);
    }

    /**
     * A availability implementation for providing the helpful functions of resetting the
     * available/unavailable states.
     */
    // 内部管理着一个 availableFuture 实例，并提供了一系列方法来动态控制组件的可用状态，方便实现类使用
    final class AvailabilityHelper implements AvailabilityProvider {

        private CompletableFuture<?> availableFuture = new CompletableFuture<>();

        public CompletableFuture<?> and(CompletableFuture<?> other) {
            return AvailabilityProvider.and(availableFuture, other);
        }

        public CompletableFuture<?> and(AvailabilityProvider other) {
            return and(other.getAvailableFuture());
        }

        public CompletableFuture<?> or(CompletableFuture<?> other) {
            return AvailabilityProvider.or(availableFuture, other);
        }

        public CompletableFuture<?> or(AvailabilityProvider other) {
            return or(other.getAvailableFuture());
        }

        /** Judges to reset the current available state as unavailable. */
        public void resetUnavailable() {
            if (isAvailable()) {
                availableFuture = new CompletableFuture<>();
            }
        }

        /** Resets the constant completed {@link #AVAILABLE} as the current state. */
        public void resetAvailable() {
            availableFuture = AVAILABLE;
        }

        /**
         * Returns the previously not completed future and resets the constant completed {@link
         * #AVAILABLE} as the current state.
         */
        public CompletableFuture<?> getUnavailableToResetAvailable() {
            CompletableFuture<?> toNotify = availableFuture;
            availableFuture = AVAILABLE;
            return toNotify;
        }

        /**
         * Creates a new uncompleted future as the current state and returns the previous
         * uncompleted one.
         */
        public CompletableFuture<?> getUnavailableToResetUnavailable() {
            CompletableFuture<?> toNotify = availableFuture;
            availableFuture = new CompletableFuture<>();
            return toNotify;
        }

        /** @return a future that is completed if the respective provider is available. */
        @Override
        public CompletableFuture<?> getAvailableFuture() {
            return availableFuture;
        }

        @Override
        public String toString() {
            if (availableFuture == AVAILABLE) {
                return "AVAILABLE";
            }
            return availableFuture.toString();
        }
    }
}
