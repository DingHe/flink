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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed {@link
 * KeyedCoProcessFunction KeyedCoProcessFunction}.
 */
// KeyedCoProcessOperator 是 KeyedCoProcessFunction 与 Flink 运行时环境之间的关键桥梁，它提供了实现复杂双流状态处理和时间控制所需的全部运行时功能。
// 键控上下文管理： 确保在处理来自两条输入流的元素时，以及在定时器触发时，算子都能正确地设置当前正在处理的键（Key）。这是访问键控状态和注册/删除键控定时器的前提。

@Internal
public class KeyedCoProcessOperator<K, IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, KeyedCoProcessFunction<K, IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;
    // 带时间戳的输出收集器。
    // 一个可重用的收集器，用于将用户函数发射的 OUT 类型元素封装成 StreamRecord，并自动带上当前元素或定时器的时间戳。
    private transient TimestampedCollector<OUT> collector;
    // 元素处理上下文。
    // 用于在执行 processElement1/2 时，提供给用户函数访问时间、侧输出和当前键等信息。
    private transient ContextImpl<K, IN1, IN2, OUT> context;
    // 定时器上下文。
    // 用于在执行 onTimer 回调时，提供给用户函数访问触发时间、时间域和触发键等信息。
    private transient OnTimerContextImpl<K, IN1, IN2, OUT> onTimerContext;

    public KeyedCoProcessOperator(KeyedCoProcessFunction<K, IN1, IN2, OUT> keyedCoProcessFunction) {
        super(keyedCoProcessFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl<>(userFunction, timerService);
        onTimerContext = new OnTimerContextImpl<>(userFunction, timerService);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        userFunction.processElement1(element.getValue(), context, collector);
        context.element = null;
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        userFunction.processElement2(element.getValue(), context, collector);
        context.element = null;
    }

    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    protected TimestampedCollector<OUT> getCollector() {
        return collector;
    }

    private class ContextImpl<K, IN1, IN2, OUT>
            extends KeyedCoProcessFunction<K, IN1, IN2, OUT>.Context {

        private final TimerService timerService;

        private StreamRecord<?> element;

        ContextImpl(KeyedCoProcessFunction<K, IN1, IN2, OUT> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        public K getCurrentKey() {
            return (K) KeyedCoProcessOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl<K, IN1, IN2, OUT>
            extends KeyedCoProcessFunction<K, IN1, IN2, OUT>.OnTimerContext {

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<K, VoidNamespace> timer;

        OnTimerContextImpl(
                KeyedCoProcessFunction<K, IN1, IN2, OUT> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(timer != null);
            return timer.getTimestamp();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain != null);
            return timeDomain;
        }

        @Override
        public K getCurrentKey() {
            return timer.getKey();
        }
    }
}
