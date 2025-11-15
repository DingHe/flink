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

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A function that processes elements of two streams and produces a single output one.
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
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
// CoProcessFunction<IN1, IN2, OUT> 是 Flink 中用于处理两条连接（Connected）数据流的高级函数接口。
// 它结合了 CoFlatMapFunction 的多输入处理能力和 ProcessFunction 的时间控制能力。
// 连接流的统一处理： 允许用户在同一个函数实例中处理来自两条类型可能不同的输入流 (IN1 和 IN2) 的数据，并产生一个统一的输出流 (OUT)
// 丰富的运行时上下文： 通过提供的 Context，用户函数可以访问键控状态（Keyed State）、查询当前时间和 Watermark、以及注册基于事件时间或处理时间的定时器（Timer）
// 时间驱动的逻辑： 用户可以定义 onTimer() 方法，在定时器触发时执行复杂的逻辑（例如，超时检测、聚合窗口关闭），从而实现比标准窗口操作更灵活的、基于时间驱动的业务逻辑。
// 侧输出（Side Output）： 允许将处理结果发送到主输出流之外的一个或多个侧输出流。
// 一个典型应用场景是控制流和数据流的组合：例如，将一条包含动态规则的流 (IN1) 与一条包含实时交易数据的流 (IN2) 连接起来，并使用状态和定时器实现复杂的模式匹配和超时逻辑
@PublicEvolving
public abstract class CoProcessFunction<IN1, IN2, OUT> extends AbstractRichFunction {

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
    // 处理第一条输入流的元素。
    // 当第一条连接流 (IN1) 中有新的元素到达时调用。
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
    // 处理第二条输入流的元素。
    // 当第二条连接流 (IN2) 中有新的元素到达时调用。
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
    // 定时器触发时的回调。
    // 当用户之前设置的定时器（事件时间或处理时间）到达触发时间时调用。默认实现为空。
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {}

    /**
     * Information available in an invocation of {@link #processElement1(Object, Context,
     * Collector)}/ {@link #processElement2(Object, Context, Collector)} or {@link #onTimer(long,
     * OnTimerContext, Collector)}.
     */
    // Context 提供了在处理输入元素时可用的运行时信息和控制功能。
    public abstract class Context {

        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        // 获取当前正在处理元素的事件时间戳。
        // 如果程序的时间特性（Time Characteristic）设置为 处理时间，则返回 null。
        public abstract Long timestamp();

        /** A {@link TimerService} for querying time and registering timers. */
        // 获取时间服务实例。
        // 允许用户查询当前的处理时间和事件时间 Watermark，以及注册新的事件时间或处理时间定时器。
        public abstract TimerService timerService();

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        // 发射侧输出（Side Output）。
        // 将一个记录 (value) 发射到由 outputTag 标识的侧输出流。
        public abstract <X> void output(OutputTag<X> outputTag, X value);
    }

    /**
     * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class OnTimerContext extends Context {
        /** The {@link TimeDomain} of the firing timer. */
        // 获取触发定时器的时间域。
        // 返回 TimeDomain.EVENT_TIME（事件时间）或 TimeDomain.PROCESSING_TIME（处理时间），用于区分是哪种定时器被触发。
        public abstract TimeDomain timeDomain();
    }
}
