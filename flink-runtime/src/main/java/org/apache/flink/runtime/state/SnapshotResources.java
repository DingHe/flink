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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

/**
 * A set of resources produced in the synchronous part of a snapshot required to finish the
 * snapshot.
 *
 * @see SnapshotStrategy
 */
// 资源抽象： 它是对 Checkpoint/Savepoint 同步阶段（Sync Phase）所创建的任何临时或辅助性资源的抽象。这些资源可能是锁、文件句柄、内存缓冲区等。
// 生命周期管理： 它确保这些资源能够存活到异步快照阶段（Async Phase） 结束，并在完成后被安全、及时地释放
// 它是一个 Checkpoint 流程中的 “临时资源提货单” ，确保状态后端在完成快照后能够进行必要的清理工作
@Internal
public interface SnapshotResources {
    /** Cleans up the resources after the asynchronous part is done. */
    // 释放封装的资源。
    // 调用时机： Flink 运行时会在整个快照过程完成（包括同步部分和异步部分，无论成功还是失败）之后调用此方法。
    void release();
}
