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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface that different types of partitioned state must implement.
 *
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the key of
 * the current element. That way, the system can handle stream and state partitioning consistently
 * together.
 */
// State 接口是 Flink 键控状态（Keyed State）体系的基石。虽然它看起来非常简单，但它在 Flink 的容错和有状态流处理中具有极其重要的地位。
// 定义键控状态基础： 它定义了所有 Flink 算子中可用的、基于当前处理元素的 Key 进行分区的状态所必须具备的最基本行为。
// 保证一致性： 它确保所有具体的状态实现（如 ValueState、ListState、MapState 等）都遵循 Flink 的键控模型。这意味着：
// 自动分区： 状态只能在 KeyedStream 上被访问。
// Key 自动提供： Flink 运行时环境会自动根据当前正在处理的数据元素，提供相应的 Key 来访问状态。用户代码永远只看到与当前 Key 关联的值。
@PublicEvolving
public interface State {

    /** Removes the value mapped under the current key. */
    // 清除当前 Key 对应的状态值。
    void clear();
}
