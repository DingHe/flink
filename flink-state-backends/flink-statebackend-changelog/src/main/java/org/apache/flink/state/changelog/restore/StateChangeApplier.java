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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.state.changelog.StateChangeOperation;

/** Applies state data change to some state. */
// StateChangeApplier（状态变更应用器）的职责是消费状态变更日志，并将日志中记录的每一次操作（例如 ADD_ELEMENT、SET、CLEAR 等）精确地重放到对应的状态对象上。
// 重放变更： 在 Flink 从 Checkpoint 或 Savepoint 恢复时，如果启用了 Changelog 机制，系统会读取变更日志流。对于流中的每一条记录，都会调用相应状态的 StateChangeApplier 来执行应用操作。
// 状态类型无关性： 尽管不同的状态类型（如 ListState、MapState）有不同的应用器实现，但它们都统一实现了这个接口，使得 Flink 内部能够以一致的方式处理所有 Keyed State 的变更应用。
@Internal
public interface StateChangeApplier {
    // 应用状态变更：这是执行状态重放的核心方法。
    // 实现类会根据传入的 operation 类型，从 DataInputView 中读取相应的序列化数据（例如，ADD_ELEMENT 操作会读取要添加的元素数据），然后将该操作应用到它所关联的底层状态实例上。
    void apply(StateChangeOperation operation, DataInputView in) throws Exception;
}
