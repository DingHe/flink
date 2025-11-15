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

/** Describes a generated {@link Projection}. */
// GeneratedProjection 继承自 GeneratedClass<Projection>，它的主要职责是作为一个容器，
// 将 Flink SQL 优化器为数据行转换（即 SELECT、列裁剪等）动态生成的 Java 源代码，
// 封装成一个可序列化、可动态编译、且能在运行时创建实例的对象。
public class GeneratedProjection extends GeneratedClass<Projection> {

    private static final long serialVersionUID = 2L;

    @VisibleForTesting
    public GeneratedProjection(String className, String code, Object[] references) {
        super(className, code, references, new Configuration());
    }

    /**
     * Creates a GeneratedProjection.
     *
     * @param className class name of the generated Function.
     * @param code code of the generated Function.
     * @param references referenced objects of the generated Function.
     * @param conf configuration when generating Function.
     */
    public GeneratedProjection(
            String className, String code, Object[] references, ReadableConfig conf) {
        super(className, code, references, conf);
    }
}
