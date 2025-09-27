/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
//主要用于 Flink 任务的处理时间定时任务调度和协调。它不是面向开发者使用的，而是 Flink 运行时内部的核心组件
@Internal
class ProcessingTimeServiceImpl implements ProcessingTimeService {
    //底层的定时器服务，负责具体的任务调度和执行
    private final TimerService timerService;
    //用于在定时任务的回调函数（ProcessingTimeCallback）执行前后添加额外的逻辑，比如记录定时器的数量
    private final Function<ProcessingTimeCallback, ProcessingTimeCallback>
            processingTimeCallbackWrapper;
    //用于跟踪当前正在运行的定时任务数量
    private final AtomicInteger numRunningTimers; //调度中的任务
    //用于表示所有定时任务已经完成并进入静止状态。当 quiesce() 方法被调用且 numRunningTimers 降为 0 时，它会被完成
    private final CompletableFuture<Void> quiesceCompletedFuture;
    //表示服务是否已进入静止状态
    private volatile boolean quiesced;

    ProcessingTimeServiceImpl(
            TimerService timerService,
            Function<ProcessingTimeCallback, ProcessingTimeCallback>
                    processingTimeCallbackWrapper) {
        this.timerService = timerService;
        this.processingTimeCallbackWrapper = processingTimeCallbackWrapper;

        this.numRunningTimers = new AtomicInteger(0);
        this.quiesceCompletedFuture = new CompletableFuture<>();
        this.quiesced = false;
    }

    @Override
    public Clock getClock() {
        return timerService.getClock();
    }

    @Override
    public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
        if (isQuiesced()) {
            return new NeverCompleteFuture(
                    ProcessingTimeServiceUtil.getProcessingTimeDelay(
                            timestamp, getCurrentProcessingTime()));
        }

        return timerService.registerTimer(
                timestamp,
                addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(target)));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            ProcessingTimeCallback callback, long initialDelay, long period) {
        if (isQuiesced()) {
            return new NeverCompleteFuture(initialDelay);
        }

        return timerService.scheduleAtFixedRate(
                addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(callback)),
                initialDelay,
                period);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            ProcessingTimeCallback callback, long initialDelay, long period) {
        if (isQuiesced()) {
            return new NeverCompleteFuture(initialDelay);
        }

        return timerService.scheduleWithFixedDelay(
                addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(callback)),
                initialDelay,
                period);
    }

    @Override
    public CompletableFuture<Void> quiesce() {
        if (!quiesced) {
            quiesced = true;

            if (numRunningTimers.get() == 0) {
                quiesceCompletedFuture.complete(null);
            }
        }

        return quiesceCompletedFuture;
    }

    private boolean isQuiesced() {
        return quiesced;
    }

    private ProcessingTimeCallback addQuiesceProcessingToCallback(ProcessingTimeCallback callback) {

        return timestamp -> {
            if (isQuiesced()) {
                return;
            }

            numRunningTimers.incrementAndGet();
            try {
                // double check to deal with the race condition:
                // before executing the previous line to increase the number of running timers,
                // the quiesce-completed future is already completed as the number of running
                // timers is 0 and "quiesced" is true
                if (!isQuiesced()) {
                    callback.onProcessingTime(timestamp);
                }
            } finally {
                if (numRunningTimers.decrementAndGet() == 0 && isQuiesced()) {
                    quiesceCompletedFuture.complete(null);
                }
            }
        };
    }
}
