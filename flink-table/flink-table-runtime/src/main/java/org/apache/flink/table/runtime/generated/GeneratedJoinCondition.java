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

/** Describes a generated {@link JoinCondition}. */
// GeneratedJoinCondition 继承自 GeneratedClass<JoinCondition>，因此它继承了管理、编译和实例化动态生成代码的所有功能。
// 特定类型封装： 专门用于封装 Flink SQL 优化器针对 JOIN 操作中非等值过滤条件 (JoinCondition 接口) 动态生成的 Java 源代码。
public class GeneratedJoinCondition extends GeneratedClass<JoinCondition> {

    private static final long serialVersionUID = 2L;

    @VisibleForTesting
    public GeneratedJoinCondition(String className, String code, Object[] references) {
        super(className, code, references, new Configuration());
    }

    /**
     * Creates a GeneratedJoinCondition.
     *
     * @param className class name of the generated JoinCondition.
     * @param code code of the generated JoinCondition.
     * @param references referenced objects of the generated JoinCondition.
     * @param conf configuration when generating JoinCondition.
     */
    public GeneratedJoinCondition(
            String className, String code, Object[] references, ReadableConfig conf) {
        super(className, code, references, conf);
    }
}
