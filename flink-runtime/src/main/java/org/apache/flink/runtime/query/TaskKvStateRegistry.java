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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/** A helper for KvState registrations of a single task. */
// Flink **可查询状态（Queryable State）功能中的一个重要组件。
// 它作为任务（Task）级别的一个辅助工具，专门用于管理该任务所拥有的所有键值状态（KvState）**的注册和生命周期。
// 核心目标是为单个运行在 TaskManager 上的任务，提供一个代理和跟踪其可查询状态实例的机制。
// 任务级状态管理： 它记录了一个 Flink Task 所创建的所有可查询 KvState 实例的元数据（例如状态名称、键组范围、状态 ID）。
// 代理注册： 它将 KvState 实例的注册请求转发给全局的 KvStateRegistry (通常位于 TaskManager 级别)，并将注册成功后获得的唯一标识符保存下来。
public class TaskKvStateRegistry {

    /** KvStateRegistry for KvState instance registrations. */
    // 全局 KvState 注册表。这是 TaskManager 级别的服务，负责实际存储 KvState 实例的引用，并将其暴露给外部查询服务。
    private final KvStateRegistry registry;

    /** JobID of the task. */
    // 当前任务所属的 Job ID
    private final JobID jobId;

    /** JobVertexID of the task. */
    // 当前任务所属的 Job Vertex ID。用于在全局注册表中进一步定位状态所属的算子实例。
    private final JobVertexID jobVertexId;

    /** List of all registered KvState instances of this task. */
    // 已注册 KvState 列表。一个内部列表，用于跟踪该任务成功注册的所有 KvState 实例的元数据（包括 KvStateID、名称和 KeyGroupRange）。
    // 这是实现 unregisterAll 的关键
    private final List<KvStateInfo> registeredKvStates = new ArrayList<>();

    TaskKvStateRegistry(KvStateRegistry registry, JobID jobId, JobVertexID jobVertexId) {
        this.registry = Preconditions.checkNotNull(registry, "KvStateRegistry");
        this.jobId = Preconditions.checkNotNull(jobId, "JobID");
        this.jobVertexId = Preconditions.checkNotNull(jobVertexId, "JobVertexID");
    }

    /**
     * Registers the KvState instance at the KvStateRegistry.
     *
     * @param keyGroupRange Key group range the KvState instance belongs to
     * @param registrationName The registration name (not necessarily the same as the KvState name
     *     defined in the state descriptor used to create the KvState instance)
     * @param kvState The
     */
    public void registerKvState(
            KeyGroupRange keyGroupRange,
            String registrationName,
            InternalKvState<?, ?, ?> kvState,
            ClassLoader userClassLoader) {
        KvStateID kvStateId =
                registry.registerKvState(
                        jobId,
                        jobVertexId,
                        keyGroupRange,
                        registrationName,
                        kvState,
                        userClassLoader);
        registeredKvStates.add(new KvStateInfo(keyGroupRange, registrationName, kvStateId));
    }

    /** Unregisters all registered KvState instances from the KvStateRegistry. */
    public void unregisterAll() {
        for (KvStateInfo kvState : registeredKvStates) {
            registry.unregisterKvState(
                    jobId,
                    jobVertexId,
                    kvState.keyGroupRange,
                    kvState.registrationName,
                    kvState.kvStateId);
        }
    }

    /** 3-tuple holding registered KvState meta data. */
    private static class KvStateInfo {

        private final KeyGroupRange keyGroupRange;

        private final String registrationName;

        private final KvStateID kvStateId;

        KvStateInfo(KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId) {
            this.keyGroupRange = keyGroupRange;
            this.registrationName = registrationName;
            this.kvStateId = kvStateId;
        }
    }
}
