/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.function.FunctionWithException;

/**
 * Envelope that carries whether the wrapped value is deterministic or not.
 *
 * @param <T> type of the wrapped value
 */
// 将一个业务对象与一个描述其“确定性（Determinism）”的布尔标记捆绑在一起。
// 在分布式计算中，“确定性”意味着给定的输入在多次执行中是否总是产生相同的结果。
// 元数据携带：不仅仅传递数据 value 本身，还告诉下游处理逻辑这个数据是“确定的”还是“非确定的”。
// 状态跟踪：在进行逻辑转换（Map）时，能够自动继承数据的确定性属性。
// 应用场景举例： 在 Flink 启动或解析配置时，某些 ID 或路径是通过固定规则生成的（确定的），而有些则是通过随机数或当前时间生成的（非确定的）。Flink 使用这个类来确保在处理某些关键决策（如 JobID 的分配）时，系统知道该值是否可重现。
public class DeterminismEnvelope<T> {
    // 被包装的实际数据对象（例如一个 JobID、配置字符串或物理地址）
    private final T value;
    // true：表示该 value 是确定的，多次运行结果一致
    private final boolean isDeterministic;

    private DeterminismEnvelope(T value, boolean isDeterministic) {
        this.value = value;
        this.isDeterministic = isDeterministic;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public T unwrap() {
        return value;
    }

    public <V, E extends Exception> DeterminismEnvelope<V> map(
            FunctionWithException<? super T, ? extends V, E> mapper) throws E {
        final V newValue = mapper.apply(value);

        if (isDeterministic) {
            return deterministicValue(newValue);
        } else {
            return nondeterministicValue(newValue);
        }
    }

    public static <T> DeterminismEnvelope<T> deterministicValue(T value) {
        return new DeterminismEnvelope<>(value, true);
    }

    public static <T> DeterminismEnvelope<T> nondeterministicValue(T value) {
        return new DeterminismEnvelope<>(value, false);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
