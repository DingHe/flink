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

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.changelog.restore.ChangelogApplierFactory;
import org.apache.flink.state.changelog.restore.StateChangeApplier;

/**
 * State used by {@link ChangelogKeyedStateBackend}. Allows replaying recorded changes on recovery
 * using {@link StateChangeApplier}
 */
// ChangelogState 的作用是将底层状态的存储和变更日志的记录/重放逻辑连接起来。
// 当 Flink 启用状态变更日志时，实际用于状态操作的 Keyed State 对象（如 ChangelogValueState）会实现或包装这个接口。它主要用于在两个关键阶段发挥作用：
// 记录阶段： 间接地允许状态后端（通过包装类）记录对状态执行的所有修改。
// 恢复阶段： 允许 Flink 在恢复时，获取一个用于应用这些已记录变更的 StateChangeApplier。
@Internal
public interface ChangelogState {
    // 获取变更应用器。该方法在 Job 恢复时被调用。
    // 它接收一个 ChangelogApplierFactory 实例，并利用它来创建并返回一个具体的 StateChangeApplier。
    // 这个应用器知道如何读取并应用当前 ChangelogState 实例的变更日志。
    StateChangeApplier getChangeApplier(ChangelogApplierFactory factory);
    // 用于将真正的、底层的状态实例（Delegated State，例如 HeapValueState 或 RocksDBValueState）注入到实现 ChangelogState 的包装类中。
    // 这样，变更日志包装器就知道它应该将操作委托给哪个底层状态对象。
    <IS> void setDelegatedState(IS state);

    /** Enable logging meta data before next writes. */
    // 用于通知状态对象：在下一次写入状态数据之前，需要先将状态的元数据（如序列化器、默认值等）写入到变更日志中。
    // 这通常发生在 Checkpoint 启动或恢复后第一次访问状态时，以确保元数据也包含在 Checkpoint 中
    void resetWritingMetaFlag();
}
