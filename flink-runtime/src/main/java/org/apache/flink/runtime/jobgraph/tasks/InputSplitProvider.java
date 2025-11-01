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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;

/**
 * An input split provider can be successively queried to provide a series of {@link InputSplit}
 * objects a task is supposed to consume in the course of its execution.
 */
// InputSplitProvider（输入分片提供者）是 Flink 在任务执行阶段管理和分配数据输入分片的核心机制。
// 按需分配分片： 它的主要职责是为正在运行的 Flink 任务（Task）**按需（Successively Queried）**提供一系列的 InputSplit 对象。
// 解耦任务与调度： 它将执行数据读取的任务本身，与输入分片如何获取、何时获取的过程解耦。任务只需要请求“下一个分片”，而无需关心这些分片是如何从 JobManager 端传输过来的。
@Public
public interface InputSplitProvider {

    /**
     * Requests the next input split to be consumed by the calling task.
     *
     * @param userCodeClassLoader used to deserialize input splits
     * @return the next input split to be consumed by the calling task or <code>null</code> if the
     *     task shall not consume any further input splits.
     * @throws InputSplitProviderException if fetching the next input split fails
     */
    // 请求下一个输入分片
    InputSplit getNextInputSplit(ClassLoader userCodeClassLoader)
            throws InputSplitProviderException;
}
