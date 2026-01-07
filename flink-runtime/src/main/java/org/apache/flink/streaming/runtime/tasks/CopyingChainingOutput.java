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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
// 在 Flink 算子链中，数据是通过方法调用直接传递的。如果多个下游算子连接到同一个上游算子：
// 风险：如果下游算子 A 在处理数据时修改了对象内部的值，那么下游算子 B 接收到的数据就会被污染（因为它们共享同一个内存对象）。
// 解决方案：当 Flink 配置中**禁用对象重用（Object Reuse disabled，这是默认行为）**时，系统会使用 CopyingChainingOutput。
// 核心逻辑：在将数据交给下游算子之前，它会先利用序列化器（Serializer）对数据进行一次深拷贝（Deep Copy）。这样每个下游算子拿到的都是独立的对象副本，互不干扰，保证了数据的准确性。

final class CopyingChainingOutput<T> extends ChainingOutput<T> {
    // 类型序列化器
    // 这是该类与父类 ChainingOutput 的主要区别。
    // 它持有数据的序列化器，用于在 pushToOperator 方法中执行 serializer.copy()。
    private final TypeSerializer<T> serializer;

    public CopyingChainingOutput(
            Input<T> input,
            TypeSerializer<T> serializer,
            @Nullable Counter prevRecordsOutCounter,
            OperatorMetricGroup curOperatorMetricGroup,
            @Nullable OutputTag<T> outputTag) {
        super(input, prevRecordsOutCounter, curOperatorMetricGroup, outputTag);
        this.serializer = serializer;
    }

    @Override
    public void collect(StreamRecord<T> record) {
        if (this.outputTag != null) {
            // we are not responsible for emitting to the main output.
            return;
        }

        pushToOperator(record);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
            // we are not responsible for emitting to the side-output specified by this
            // OutputTag.
            return;
        }

        pushToOperator(record);
    }

    // 实现“拷贝并在链中向下传递”的逻辑
    @Override
    protected <X> void pushToOperator(StreamRecord<X> record) {
        try {
            // we know that the given outputTag matches our OutputTag so the record
            // must be of the type that our operator (and Serializer) expects.
            @SuppressWarnings("unchecked")
            StreamRecord<T> castRecord = (StreamRecord<T>) record;

            numRecordsOut.inc();
            numRecordsIn.inc();
            StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
            recordProcessor.accept(copy);
        } catch (ClassCastException e) {
            if (outputTag != null) {
                // Enrich error message
                ClassCastException replace =
                        new ClassCastException(
                                String.format(
                                        "%s. Failed to push OutputTag with id '%s' to operator. "
                                                + "This can occur when multiple OutputTags with different types "
                                                + "but identical names are being used.",
                                        e.getMessage(), outputTag.getId()));

                throw new ExceptionInChainedOperatorException(replace);
            } else {
                throw new ExceptionInChainedOperatorException(e);
            }
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }
}
