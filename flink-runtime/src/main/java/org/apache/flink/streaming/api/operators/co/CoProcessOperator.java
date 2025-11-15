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
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * CoProcessFunction CoProcessFunctions}.
 */
// CoProcessOperator 的核心作用是作为 CoProcessFunction（用户定义的双流处理逻辑）与 Flink 运行时环境之间的桥梁。
// 双流输入适配： 实现 TwoInputStreamOperator 接口，能够接收来自两条连接流 (IN1 和 IN2) 的数据，并分别将它们路由到 CoProcessFunction 对应的 processElement1 和 processElement2 方法。
// 上下文和时间戳注入： 在调用用户函数之前，它会设置当前输入元素的时间戳，并创建和管理一个**上下文（Context）**对象，将运行时信息（如时间服务、时间戳、侧输出能力）暴露给用户函数
// Watermark 跟踪： 接收并跟踪 Watermark，以便用户函数可以通过 Context 查询当前的事件时间进度。
// 真正的、支持状态和定时器的 CoProcessFunction 实现通常位于 KeyedCoProcessOperator 中，
// 而这个 CoProcessOperator 主要是用于 非键控 的双流操作，例如仅作数据整合和侧输出。
@Internal
public class CoProcessOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, CoProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;
    // 带时间戳的输出收集器。
    // 一个可重用的收集器，用于将用户函数发射的 OUT 类型元素封装成 StreamRecord，并自动带上当前正在处理的元素的时间戳，然后发送给下游。
    private transient TimestampedCollector<OUT> collector;
    // 运行时上下文实现。 CoProcessFunction.Context 的运行时实现类。
    // 它在每次调用用户函数前被设置，用于向用户函数提供 Watermark、处理时间、当前时间戳和侧输出等功能。
    private transient ContextImpl context;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    // 当前 Watermark。
    // 存储当前算子接收到的最新 Watermark 的时间戳
    private long currentWatermark = Long.MIN_VALUE;

    public CoProcessOperator(CoProcessFunction<IN1, IN2, OUT> flatMapper) {
        super(flatMapper);
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);

        context = new ContextImpl(userFunction, getProcessingTimeService());
    }
    // 处理来自第一条流的数据元素。
    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        userFunction.processElement1(element.getValue(), context, collector);
        context.element = null;
    }
    // 处理来自第二条流的数据元素
    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        userFunction.processElement2(element.getValue(), context, collector);
        context.element = null;
    }
    // 处理 Watermark
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    private class ContextImpl extends CoProcessFunction<IN1, IN2, OUT>.Context
            implements TimerService {

        private final ProcessingTimeService timerService;

        private StreamRecord<?> element;

        ContextImpl(CoProcessFunction<IN1, IN2, OUT> function, ProcessingTimeService timerService) {
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
        public long currentProcessingTime() {
            return timerService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
        }

        @Override
        public TimerService timerService() {
            return this;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }
    }
}
