/*

* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.

*/

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Internal;

/**
 * The internal checkpoint listener add another {@link #notifyCheckpointSubsumed(long)} interface
 * for internal usage.
 */
// InternalCheckpointListener 接口是对用户级别的 CheckpointListener 接口的扩展，主要作用是提供一个额外的钩子（Hook），
// 用于通知 Flink 内部组件某个 Checkpoint 已经被废弃（Subsumed）
// 处理 Checkpoint 废弃： 它增加了 notifyCheckpointSubsumed() 方法，专门用于通知 Flink 不再需要某个特定的 Checkpoint，因为它已经被更新、更完整的 Checkpoint 所取代。
@Internal
public interface InternalCheckpointListener extends CheckpointListener {

    /**
     * This method is called as a notification once a distributed checkpoint has been subsumed.
     *
     * <p>These notifications are "best effort", meaning they can sometimes be skipped.
     *
     * <p>This method is very rarely necessary to implement. The "best effort" guarantee, together
     * with the fact that this method should not result in discarding any data (per the "Checkpoint
     * Subsuming Contract") means it is mainly useful for earlier cleanups of auxiliary resources.
     *
     * @param checkpointId The ID of the checkpoint that has been subsumed.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task or job.
     */
    void notifyCheckpointSubsumed(long checkpointId) throws Exception;
}
