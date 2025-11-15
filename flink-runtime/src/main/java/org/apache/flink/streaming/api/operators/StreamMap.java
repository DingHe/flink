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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
// StreamMap 的核心作用是实现 Flink DataStream API 中的 map() 转换操作。
// 它接收一个输入元素，应用用户自定义的 MapFunction，然后将转换后的结果作为输出元素发送给下游算子。
// 逐条转换： 对输入流中的每个数据记录，应用 MapFunction 中的 map() 方法进行一次精确的一对一转换。
// 链式优化： 默认设置 ChainingStrategy.ALWAYS，意味着它通常会与上游算子**链式化（Chain）**在一起，在同一个线程中执行，从而减少序列化/反序列化和网络传输的开销，提高性能。
// 时间戳和键的保持： 作为一个基本的转换算子，它会保留输入元素的**时间戳（Timestamp）和当前键（Key）**信息，并将这些信息传递给输出记录。

/** A {@link StreamOperator} for executing {@link MapFunction MapFunctions}. */
@Internal
public class StreamMap<IN, OUT> extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    public StreamMap(MapFunction<IN, OUT> mapper) {
        super(mapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        output.collect(element.replace(userFunction.map(element.getValue())));
    }
}
