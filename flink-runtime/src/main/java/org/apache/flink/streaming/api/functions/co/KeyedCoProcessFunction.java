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

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A function that processes elements of two keyed streams and produces a single output one.
 *
 * <p>The function will be called for every element in the input streams and can produce zero or
 * more output elements. Contrary to the {@link CoFlatMapFunction}, this function can also query the
 * time (both event and processing) and set timers, through the provided {@link Context}. When
 * reacting to the firing of set timers the function can emit yet more elements.
 *
 * <p>An example use-case for connected streams would be the application of a set of rules that
 * change over time ({@code stream A}) to the elements contained in another stream (stream {@code
 * B}). The rules contained in {@code stream A} can be stored in the state and wait for new elements
 * to arrive on {@code stream B}. Upon reception of a new element on {@code stream B}, the function
 * can now apply the previously stored rules to the element and directly emit a result, and/or
 * register a timer that will trigger an action in the future.
 *
 * @param <K> Type of the key.
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
// KeyedCoProcessFunction<K, IN1, IN2, OUT> 是 Flink 中用于处理两条已根据键（Key）进行分区的连接数据流的最强大的函数接口
// KeyedCoProcessFunction 是 CoProcessFunction 的键控版本。它将双流处理、状态管理、时间服务和定时器功能**绑定到特定的键（Key）**上。
// 基于键的状态隔离： 允许用户在处理两条流中的数据时，访问和修改与当前数据元素关联的键控状态（Keyed State）。这意味着状态是按键隔离和维护的。
// 控定时器： 用户可以为每个独立的键设置基于事件时间或处理时间的定时器。定时器触发时，onTimer() 方法也会在对应键的上下文中执行。
// 连接流高级控制： 提供了处理两条异构流中数据，并根据状态和时间触发复杂逻辑的能力。例如，它能轻松实现复杂的会话关联、模式匹配或双流数据的同步/异步 JOIN 逻辑。
@PublicEvolving
public abstract class KeyedCoProcessFunction<K, IN1, IN2, OUT> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * This method is called for each element in the first of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *     timers and querying the time. The context is only valid during the invocation of this
     *     method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *     and go into recovery.
     */
    // 处理第一条输入键控流的元素。
    // 当第一条流 (IN1) 中有元素到达时调用。在调用时，Flink 运行时已自动将当前处理键设置为该元素所属的键，确保了状态访问和定时器操作都是键隔离的。
    public abstract void processElement1(IN1 value, Context ctx, Collector<OUT> out)
            throws Exception;

    /**
     * This method is called for each element in the second of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *     timers and querying the time. The context is only valid during the invocation of this
     *     method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *     and go into recovery.
     */
    // 处理第二条输入键控流的元素。
    // 当第二条流 (IN2) 中有元素到达时调用。它与 processElement1 具有相同的键控上下文行为。
    public abstract void processElement2(IN2 value, Context ctx, Collector<OUT> out)
            throws Exception;

    /**
     * Called when a timer set using {@link TimerService} fires.
     *
     * @param timestamp The timestamp of the firing timer.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
     *     querying the {@link TimeDomain} of the firing timer and getting a {@link TimerService}
     *     for registering timers and querying the time. The context is only valid during the
     *     invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    // 定时器触发时的回调。 当用户之前设置的特定键的定时器触发时调用。默认实现为空
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {}

    /**
     * Information available in an invocation of {@link #processElement1(Object, Context,
     * Collector)}/ {@link #processElement2(Object, Context, Collector)} or {@link #onTimer(long,
     * OnTimerContext, Collector)}.
     */
    public abstract class Context {

        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        public abstract Long timestamp();

        /** A {@link TimerService} for querying time and registering timers. */
        public abstract TimerService timerService();

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        public abstract <X> void output(OutputTag<X> outputTag, X value);

        /** Get key of the element being processed. */
        public abstract K getCurrentKey();
    }

    /**
     * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class OnTimerContext extends Context {
        /** The {@link TimeDomain} of the firing timer. */
        public abstract TimeDomain timeDomain();

        /** Get key of the firing timer. */
        @Override
        public abstract K getCurrentKey();
    }
}
