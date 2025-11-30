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
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple registry, which maps {@link InternalKvState} registration notifications to {@link
 * KvStateLocation} instances.
 */
// KvStateLocationRegistry（键值状态位置注册表）是 Flink 查询服务 (Queryable State) 的核心组件，
// 它通常由 JobManager/Checkpoint Coordinator 持有。
// 核心职责： 它充当一个中央查找服务，用于记录作业中所有 TaskManager 上注册的 KvState 实例的位置信息。
// 映射关系： 它建立从用户定义的 KvState 注册名称 到 KvState 所在 TaskManager 地址的精确映射。
// 将属于同一个 KvState（由相同的 registrationName 标识）但在不同 Task 实例上（即不同的 Key Group Range）的物理位置信息聚合起来，形成一个完整的 KvStateLocation 视图，供客户端查询。
public class KvStateLocationRegistry {

    /** JobID this coordinator belongs to. */
    private final JobID jobId;

    /** Job vertices for determining parallelism per key. */
    private final Map<JobVertexID, ExecutionJobVertex> jobVertices;

    /**
     * Location info keyed by registration name. The name needs to be unique per JobID, i.e. two
     * operators cannot register KvState with the same name.
     */
    private final Map<String, KvStateLocation> lookupTable = new HashMap<>();

    /**
     * Creates the registry for the job.
     *
     * @param jobId JobID this coordinator belongs to.
     * @param jobVertices Job vertices map of all vertices of this job.
     */
    public KvStateLocationRegistry(JobID jobId, Map<JobVertexID, ExecutionJobVertex> jobVertices) {
        this.jobId = Preconditions.checkNotNull(jobId, "JobID");
        this.jobVertices = Preconditions.checkNotNull(jobVertices, "Job vertices");
    }

    /**
     * Returns the {@link KvStateLocation} for the registered KvState instance or <code>null</code>
     * if no location information is available.
     *
     * @param registrationName Name under which the KvState instance is registered.
     * @return Location information or <code>null</code>.
     */
    public KvStateLocation getKvStateLocation(String registrationName) {
        return lookupTable.get(registrationName);
    }

    /**
     * Notifies the registry about a registered KvState instance.
     *
     * @param jobVertexId JobVertexID the KvState instance belongs to
     * @param keyGroupRange Key group range the KvState instance belongs to
     * @param registrationName Name under which the KvState has been registered
     * @param kvStateId ID of the registered KvState instance
     * @param kvStateServerAddress Server address where to find the KvState instance
     * @throws IllegalArgumentException If JobVertexID does not belong to job
     * @throws IllegalArgumentException If state has been registered with same name by another
     *     operator.
     * @throws IndexOutOfBoundsException If key group index is out of bounds.
     */
    public void notifyKvStateRegistered(
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress) {

        KvStateLocation location = lookupTable.get(registrationName);

        if (location == null) {
            // First registration for this operator, create the location info
            ExecutionJobVertex vertex = jobVertices.get(jobVertexId);

            if (vertex != null) {
                int parallelism = vertex.getMaxParallelism();
                location = new KvStateLocation(jobId, jobVertexId, parallelism, registrationName);
                lookupTable.put(registrationName, location);
            } else {
                throw new IllegalArgumentException("Unknown JobVertexID " + jobVertexId);
            }
        }

        // Duplicated name if vertex IDs don't match
        if (!location.getJobVertexId().equals(jobVertexId)) {
            IllegalStateException duplicate =
                    new IllegalStateException(
                            "Registration name clash. KvState with name '"
                                    + registrationName
                                    + "' has already been registered by another operator ("
                                    + location.getJobVertexId()
                                    + ").");

            ExecutionJobVertex vertex = jobVertices.get(jobVertexId);
            if (vertex != null) {
                vertex.fail(new SuppressRestartsException(duplicate));
            }

            throw duplicate;
        }
        location.registerKvState(keyGroupRange, kvStateId, kvStateServerAddress);
    }

    /**
     * Notifies the registry about an unregistered KvState instance.
     *
     * @param jobVertexId JobVertexID the KvState instance belongs to
     * @param keyGroupRange Key group index the KvState instance belongs to
     * @param registrationName Name under which the KvState has been registered
     * @throws IllegalArgumentException If another operator registered the state instance
     * @throws IllegalArgumentException If the registration name is not known
     */
    public void notifyKvStateUnregistered(
            JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName) {

        KvStateLocation location = lookupTable.get(registrationName);

        if (location != null) {
            // Duplicate name if vertex IDs don't match
            if (!location.getJobVertexId().equals(jobVertexId)) {
                throw new IllegalArgumentException(
                        "Another operator ("
                                + location.getJobVertexId()
                                + ") registered the KvState "
                                + "under '"
                                + registrationName
                                + "'.");
            }

            location.unregisterKvState(keyGroupRange);

            if (location.getNumRegisteredKeyGroups() == 0) {
                lookupTable.remove(registrationName);
            }
        } else {
            throw new IllegalArgumentException(
                    "Unknown registration name '"
                            + registrationName
                            + "'. "
                            + "Probably registration/unregistration race.");
        }
    }
}
