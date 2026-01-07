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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.KeyContextHandler;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessing;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingConsumer;

/** Utility class for creating record processor for {@link Input} {@link StreamOperator}. */
public class RecordProcessorUtils {

    private static final String METHOD_SET_KEY_CONTEXT_ELEMENT = "setKeyContextElement";
    private static final String METHOD_SET_KEY_CONTEXT_ELEMENT1 = "setKeyContextElement1";
    private static final String METHOD_SET_KEY_CONTEXT_ELEMENT2 = "setKeyContextElement2";

    /**
     * Get record processor for {@link Input}, which will omit call of {@link
     * Input#setKeyContextElement} if it doesn't have key context.
     *
     * @param input the {@link Input}
     * @return the record processor
     */
    // 核心目的是：根据下游算子的特性，返回最高效的数据处理函数，尽量避免不必要的 Key 上下文（Key Context）切换。
    // 在 Flink 的 Keyed Stream 中，每处理一条数据前通常需要 setKeyContextElement 来切换当前算子的状态后端（State Backend）对应的 Key。
    // 如果能判定某个算子不需要切换 Key，就能节省大量 CPU 开销。
    // 接收一个下游输入端 input，返回一个函数式接口 ThrowingConsumer（接收 StreamRecord，可能抛出异常）
    public static <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(
            Input<T> input) {
        boolean canOmitSetKeyContext;
        // 检查输入端是否是 Flink 最常见的抽象算子基类。
        if (input instanceof AbstractStreamOperator) {
            // 检查该算子是否定义了 Key 属性。如果是一个非 Keyed 算子（比如普通的 map），则返回 true
            canOmitSetKeyContext = canOmitSetKeyContext((AbstractStreamOperator<?>) input, 0);
        } else {
            canOmitSetKeyContext =
                    input instanceof KeyContextHandler
                            && !((KeyContextHandler) input).hasKeyContext();
        }
        // 最快路径（无 Key 切换）
        if (canOmitSetKeyContext) {
            // 如果判定可以省略 Key 切换
            // 直接返回 input 的 processElement 方法引用。这是最高效的路径，数据直接进入处理逻辑，没有任何额外的方法包装或状态切换。
            return input::processElement;
        // 异步状态处理路径
        } else if (input instanceof AsyncStateProcessing
                && ((AsyncStateProcessing) input).isAsyncStateProcessingEnabled()) {
            return ((AsyncStateProcessing) input).getRecordProcessor(1);
        } else {
            // 标准路径（有 Key 切换）
            return record -> {
                input.setKeyContextElement(record);
                input.processElement(record);
            };
        }
    }

    /**
     * Get record processor for the first input of {@link TwoInputStreamOperator}, which will omit
     * call of {@link StreamOperator#setKeyContextElement1} if it doesn't have key context.
     *
     * @param operator the {@link TwoInputStreamOperator}
     * @return the record processor
     */
    public static <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor1(
            TwoInputStreamOperator<T, ?, ?> operator) {
        boolean canOmitSetKeyContext;
        if (operator instanceof AbstractStreamOperator) {
            canOmitSetKeyContext = canOmitSetKeyContext((AbstractStreamOperator<?>) operator, 0);
        } else {
            canOmitSetKeyContext =
                    operator instanceof KeyContextHandler
                            && !((KeyContextHandler) operator).hasKeyContext1();
        }

        if (canOmitSetKeyContext) {
            return operator::processElement1;
        } else if (operator instanceof AsyncStateProcessing
                && ((AsyncStateProcessing) operator).isAsyncStateProcessingEnabled()) {
            return ((AsyncStateProcessing) operator).getRecordProcessor(1);
        } else {
            return record -> {
                operator.setKeyContextElement1(record);
                operator.processElement1(record);
            };
        }
    }

    /**
     * Get record processor for the second input of {@link TwoInputStreamOperator}, which will omit
     * call of {@link StreamOperator#setKeyContextElement2} if it doesn't have key context.
     *
     * @param operator the {@link TwoInputStreamOperator}
     * @return the record processor
     */
    public static <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor2(
            TwoInputStreamOperator<?, T, ?> operator) {
        boolean canOmitSetKeyContext;
        if (operator instanceof AbstractStreamOperator) {
            canOmitSetKeyContext = canOmitSetKeyContext((AbstractStreamOperator<?>) operator, 1);
        } else {
            canOmitSetKeyContext =
                    operator instanceof KeyContextHandler
                            && !((KeyContextHandler) operator).hasKeyContext2();
        }

        if (canOmitSetKeyContext) {
            return operator::processElement2;
        } else if (operator instanceof AsyncStateProcessing
                && ((AsyncStateProcessing) operator).isAsyncStateProcessingEnabled()) {
            return ((AsyncStateProcessing) operator).getRecordProcessor(2);
        } else {
            return record -> {
                operator.setKeyContextElement2(record);
                operator.processElement2(record);
            };
        }
    }
    // 检查该算子是否定义了 Key 属性。如果是一个非 Keyed 算子（比如普通的 map），则返回 true
    private static boolean canOmitSetKeyContext(
            AbstractStreamOperator<?> streamOperator, int input) {
        // Since AbstractStreamOperator is @PublicEvolving, we need to check whether the
        // "SetKeyContextElement" is overridden by the (user-implemented) subclass. If it is
        // overridden, we cannot omit it due to the subclass may maintain different key selectors on
        // its own.
        return !hasKeyContext(streamOperator, input)
                && !methodSetKeyContextIsOverridden(streamOperator, input);
    }
    // 检查该算子是否定义了 Key 属性。如果是一个非 Keyed 算子（比如普通的 map），则返回 true
    private static boolean hasKeyContext(AbstractStreamOperator<?> operator, int input) {
        if (input == 0) {
            return operator.hasKeyContext1();
        } else {
            return operator.hasKeyContext2();
        }
    }

    private static boolean methodSetKeyContextIsOverridden(
            AbstractStreamOperator<?> operator, int input) {
        if (input == 0) {
            if (operator instanceof OneInputStreamOperator) {
                return methodIsOverridden(
                                operator,
                                OneInputStreamOperator.class,
                                METHOD_SET_KEY_CONTEXT_ELEMENT,
                                StreamRecord.class)
                        || methodIsOverridden(
                                operator,
                                AbstractStreamOperator.class,
                                METHOD_SET_KEY_CONTEXT_ELEMENT1,
                                StreamRecord.class);
            } else {
                return methodIsOverridden(
                        operator,
                        AbstractStreamOperator.class,
                        METHOD_SET_KEY_CONTEXT_ELEMENT1,
                        StreamRecord.class);
            }
        } else {
            return methodIsOverridden(
                    operator,
                    AbstractStreamOperator.class,
                    METHOD_SET_KEY_CONTEXT_ELEMENT2,
                    StreamRecord.class);
        }
    }

    private static boolean methodIsOverridden(
            AbstractStreamOperator<?> operator,
            Class<?> expectedDeclaringClass,
            String methodName,
            Class<?>... parameterTypes) {
        try {
            Class<?> methodDeclaringClass =
                    operator.getClass().getMethod(methodName, parameterTypes).getDeclaringClass();
            return methodDeclaringClass != expectedDeclaringClass;
        } catch (NoSuchMethodException exception) {
            throw new FlinkRuntimeException(
                    String.format(
                            "BUG: Can't find '%s' method in '%s'",
                            methodName, operator.getClass()));
        }
    }

    /** Private constructor to prevent instantiation. */
    private RecordProcessorUtils() {}
}
