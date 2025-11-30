/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/** Handler for common queryable state logic. */
// KvStateHandler（键值状态处理器）是一个封装了 查询服务 (Queryable State) 核心逻辑的辅助类。
// 它的主要作用是充当 JobManager/Scheduler 与底层的 KvStateLocationRegistry 之间的桥梁。
// 查询服务允许外部客户端或 Flink 内部组件通过网络查询 Flink 任务中的键值状态（KvState）。
// KvStateHandler 负责处理与 KvState 注册、反注册和位置查询相关的请求。
public class KvStateHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KvStateHandler.class);
    // 执行图实例。
    // KvStateHandler 依赖于 ExecutionGraph 来获取作业的 ID 和其内置的 KvStateLocationRegistry（键值状态位置注册表），
    // 所有操作都通过这个注册表进行。
    private final ExecutionGraph executionGraph;

    public KvStateHandler(ExecutionGraph executionGraph) {
        this.executionGraph = executionGraph;
    }
    // 处理客户端请求 KvState 位置的查找操作。
    public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {

        // sanity check for the correct JobID
        // Job ID 校验： 首先检查传入的 jobId 是否与当前 executionGraph 的 ID 匹配。
        // 如果不匹配，抛出 FlinkJobNotFoundException。
        if (executionGraph.getJobID().equals(jobId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Lookup key-value state for job {} with registration " + "name {}.",
                        executionGraph.getJobID(),
                        registrationName);
            }
            // 结果返回： 如果找到位置 (location != null)，
            // 返回 KvStateLocation 对象（包含了 KvState 所在的 JobVertex、KeyGroup 范围以及服务地址等信息）。
            final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
            final KvStateLocation location = registry.getKvStateLocation(registrationName);
            if (location != null) {
                return location;
            } else {
                throw new UnknownKvStateLocation(registrationName);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Request of key-value state location for unknown job {} received.", jobId);
            }
            throw new FlinkJobNotFoundException(jobId);
        }
    }
    // 处理来自 TaskManager 的 KvState 注册通知。
    public void notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        // Job ID 校验： 检查传入的 jobId 是否匹配，不匹配则抛出 FlinkJobNotFoundException。
        if (executionGraph.getJobID().equals(jobId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Key value state registered for job {} under name {}.",
                        executionGraph.getJobID(),
                        registrationName);
            }

            try {
                // 调用 registry.notifyKvStateRegistered(...) 方法，
                // 将 KvState 的详细信息（KvState ID、它所在的 TaskManager 地址、所属的 JobVertex ID 和 KeyGroup 范围）转发给注册表进行记录。
                executionGraph
                        .getKvStateLocationRegistry()
                        .notifyKvStateRegistered(
                                jobVertexId,
                                keyGroupRange,
                                registrationName,
                                kvStateId,
                                kvStateServerAddress);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new FlinkJobNotFoundException(jobId);
        }
    }

    public void notifyKvStateUnregistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName)
            throws FlinkJobNotFoundException {

        if (executionGraph.getJobID().equals(jobId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Key value state unregistered for job {} under name {}.",
                        executionGraph.getJobID(),
                        registrationName);
            }

            try {
                executionGraph
                        .getKvStateLocationRegistry()
                        .notifyKvStateUnregistered(jobVertexId, keyGroupRange, registrationName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new FlinkJobNotFoundException(jobId);
        }
    }
}
