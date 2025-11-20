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
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperator} for processing {@link
 * CoMapFunction CoMapFunctions}.
 */
// CoStreamMap 是 Apache Flink Streaming API 中的一个内部操作符（Operator），它用于封装和执行用户定义的 CoMapFunction。
// 这个操作符是专门设计来处理两个独立的输入流，并对它们分别应用不同的转换逻辑，然后将结果合并到一个输出流中。
@Internal
public class CoStreamMap<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, CoMapFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;

    public CoStreamMap(CoMapFunction<IN1, IN2, OUT> mapper) {
        super(mapper);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        output.collect(element.replace(userFunction.map1(element.getValue())));
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        output.collect(element.replace(userFunction.map2(element.getValue())));
    }
}
