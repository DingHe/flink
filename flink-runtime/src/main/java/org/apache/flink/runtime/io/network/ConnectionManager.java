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

package org.apache.flink.runtime.io.network;

import java.io.IOException;

/**
 * The connection manager manages physical connections for the (logical) remote input channels at
 * runtime.
 */
// 网络栈中负责物理连接管理的核心抽象。
// ConnectionManager 的核心角色是 “网络通信的中间层”。
// 在 Flink 的 TaskManager 之间进行数据交换时，逻辑上我们看到的是 InputChannel（输入通道）和 ResultPartition（结果分区）。但在物理层面上，数据必须通过 TCP 连接或本地内存拷贝来传输。
// 物理连接的生命周期管理：负责启动网络服务（通常基于 Netty），并维护不同 TaskManager 之间的 TCP 连接。
// 屏蔽传输细节：为上层提供统一的 PartitionRequestClient，让上层不需要关心底层的 Netty Channel 如何建立、重连或多路复用。
// 资源回收：在作业结束或出错时，负责关闭打开的物理通道，防止句柄泄露。
public interface ConnectionManager {

    /**
     * Starts the internal related components for network connection and communication.
     *
     * @return a port to connect to the task executor for shuffle data exchange, -1 if only local
     *     connection is possible.
     */
    // 初始化并启动网络组件。
    int start() throws IOException;

    /** Creates a {@link PartitionRequestClient} instance for the given {@link ConnectionID}. */
    // 根据给定的连接标识符，创建一个用于请求分区的客户端实例。
    // ConnectionID 包含了目标 TaskManager 的地址（InetSocketAddress）和作业 ID 等识别信息。
    PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException;

    /** Closes opened ChannelConnections in case of a resource release. */
    // 关闭与特定节点关联的所有打开的物理连接。
    void closeOpenChannelConnections(ConnectionID connectionId);
    // 统计当前活跃的物理连接数量。
    int getNumberOfActiveConnections();
    // 彻底关闭连接管理器。
    void shutdown() throws IOException;
}
