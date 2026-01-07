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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Collector;

/**
 * Describes a generated {@link Collector}.
 *
 * @param <C> type of collector
 */
// 在 Flink 的 DataStream API 中，Collector（收集器）负责将计算结果（如 collect(T record)）发送到下游算子。
// 在 Flink SQL/Table API 中，由于处理的数据结构（如 RowData）和处理逻辑（如聚合后的结果拼装、Join 后的结果输出）是动态变化的，Flink 不会使用一个通用的 Collector，而是会：
// 动态生成定制化的 Collector 代码：针对特定的 Schema 生成最优化的输出逻辑。
// 封装与传输：GeneratedCollector 类充当这些动态生成的代码的“容器”。它将 Java 源码字符串、类名和关联引用封装起来，以便从 JobManager 发送到 TaskManager。
// 运行时实例化：在 TaskManager 的算子初始化阶段，通过它持有的源码动态编译并创建出真正的 Collector 对象。


public class GeneratedCollector<C extends Collector<?>> extends GeneratedClass<C> {

    private static final long serialVersionUID = 2L;

    @VisibleForTesting
    public GeneratedCollector(String className, String code, Object[] references) {
        super(className, code, references, new Configuration());
    }

    /**
     * Creates a GeneratedCollector.
     *
     * @param className class name of the generated Collector.
     * @param code code of the generated Collector.
     * @param references referenced objects of the generated Collector.
     * @param conf configuration when generating Collector.
     */
    public GeneratedCollector(
            String className, String code, Object[] references, ReadableConfig conf) {
        super(className, code, references, conf);
    }
}
