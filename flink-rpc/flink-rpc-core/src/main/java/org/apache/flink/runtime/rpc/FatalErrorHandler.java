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

package org.apache.flink.runtime.rpc;

/** Handler for fatal errors. */
// FatalErrorHandler 的核心作用是处理那些无法恢复的致命错误（Fatal Errors）。
// 在分布式系统中，并非所有的异常都能通过简单的 try-catch 或重试来解决。
// 有些错误（如 OutOfMemoryError、严重的底层 RPC 中断、或者关键线程的意外终止）会导致当前的组件（如 JobManager 或 TaskManager）处于不一致或不可用的状态。
// 当这种“致命”情况发生时，系统不能选择无视或仅仅记录日志，而必须采取决断性措施（通常是关闭进程或触发集群切换），以防止故障蔓延（如产生脏数据或导致集群死锁）。

public interface FatalErrorHandler {

    /**
     * Being called when a fatal error occurs.
     *
     * <p>IMPORTANT: This call should never be blocking since it might be called from within the
     * main thread of an {@link RpcEndpoint}.
     *
     * @param exception cause
     */
    // 触发时机：当某个组件遇到它认为无法自行处理、且会导致进程无法继续正常运行的异常时，会调用此方法。
    void onFatalError(Throwable exception);
}
