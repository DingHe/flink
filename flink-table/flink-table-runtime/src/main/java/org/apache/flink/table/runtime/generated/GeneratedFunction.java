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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

/**
 * Describes a generated {@link Function}.
 *
 * @param <F> type of Function
 */
// 在 Flink SQL 或 Table API 中，为了追求极致的执行效率，Flink 并不会对每一条数据都通过通用的解释器（Interpreter）来执行（因为解释器会有大量的条件判断和虚函数调用，性能较差）。
// 相反，Flink 会根据你写的 SQL 逻辑，在运行时动态生成 Java 代码。例如，如果你写了 SELECT a + b FROM T，Flink 会生成一段专门处理这两个字段相加的 Java 类代码。
// 容器作用：它像一个“包裹”，封装了动态生成的 Java 代码字符串、类名以及该类运行所需的外部引用对象。
// 桥梁作用：它连接了“代码生成阶段”和“集群执行阶段”。代码在 JobManager 端生成，封装进 GeneratedFunction 后，通过序列化发送到 TaskManager，在 TaskManager 端再进行编译和实例化。
// 类型安全：通过泛型 <F extends Function> 确保生成的代码最终会实现 Flink 的 Function 接口（如 MapFunction、FlatMapFunction 等）。


public class GeneratedFunction<F extends Function> extends GeneratedClass<F> {

    private static final long serialVersionUID = 2L;

    @VisibleForTesting
    public GeneratedFunction(String className, String code, Object[] references) {
        super(className, code, references, new Configuration());
    }

    /**
     * Creates a GeneratedFunction.
     *
     * @param className class name of the generated Function.
     * @param code code of the generated Function.
     * @param references referenced objects of the generated Function.
     * @param conf configuration when generating Function.
     */
    public GeneratedFunction(
            String className, String code, Object[] references, ReadableConfig conf) {
        super(className, code, references, conf);
    }
}
