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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** A {@link StreamOperator} for executing {@link FilterFunction FilterFunctions}. */
// StreamFilter 的核心作用是实现 Flink DataStream API 中的 filter() 转换操作。
// 它根据用户提供的 FilterFunction 对输入流中的数据记录进行选择性保留。
// 条件过滤： 对输入的每个数据记录，应用用户自定义的 FilterFunction 中的 filter() 方法。
// 选择性输出： 只有当 filter() 方法返回 true 时，该记录才会被发送到下游；返回 false 的记录将被丢弃。
// 链式优化： 默认设置 ChainingStrategy.ALWAYS，意味着它通常会与上游算子**链式化（Chain）**在一起执行，以提高效率。
// 数据保持： 对于通过过滤的元素，它会原封不动地将输入记录（包括值、时间戳和键信息）发送给下游。
@Internal
public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 1L;

    public StreamFilter(FilterFunction<IN> filterFunction) {
        super(filterFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
        }
    }
}
