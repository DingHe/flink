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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An decorative interface of {@link SplitEnumerator} which allows to handle {@link SourceEvent}
 * sent from a specific execution attempt.
 *
 * <p>The split enumerator must implement this interface if it needs to deal with custom source
 * events and is used in cases that a subtask can have multiple concurrent execution attempts, e.g.
 * if speculative execution is enabled. Otherwise an error will be thrown when the split enumerator
 * receives a custom source event.
 */
// SupportsHandleExecutionAttemptSourceEvent 接口是 Flink 统一 Source API 的一个装饰性接口（Decorative Interface），
// 用于扩展 SplitEnumerator 的能力。
// 处理特定执行尝试的事件： 它允许 SplitEnumerator 接收和处理来自 SourceReader 的自定义 Source 事件，并且更关键的是，它能够识别发送事件的 读取器是哪一个特定的执行尝试（Execution Attempt）。
// 支持高级执行模式： 主要用于 Flink 启用了高级执行模式（如推测执行 (Speculative Execution)）的场景。在这些模式下，一个逻辑子任务（subtaskId）可能同时有多个竞争性的执行实例（attemptNumber）在运行。
// 防止事件冲突： 如果 Source 依赖自定义事件进行协调，但在推测执行环境下不实现此接口，当 SplitEnumerator 接收到自定义事件时，由于无法分辨是哪个尝试发送的，可能会抛出错误。实现此接口可以确保事件被正确地路由和处理。


@PublicEvolving
public interface SupportsHandleExecutionAttemptSourceEvent {

    /**
     * Handles a custom source event from the source reader. It is similar to {@link
     * SplitEnumerator#handleSourceEvent(int, SourceEvent)} but is aware of the subtask execution
     * attempt who sent this event.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param attemptNumber the attempt number of the source reader who sent the source event.
     * @param sourceEvent the source event from the source reader.
     */
    // 处理包含执行尝试信息的自定义 Source 事件。
    // 这是替代 SplitEnumerator#handleSourceEvent(int, SourceEvent) 的方法，增加了对执行尝试号的识别。
    void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent);
}
