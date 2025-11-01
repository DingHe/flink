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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.shuffle.DefaultShuffleMetrics;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a task
 * manager.
 */
// Flink TaskExecutor 上的一个核心组件，负责管理该节点上所有由上游任务生产和等待被下游任务消费的中间结果分区（Result Partitions）
// 集中注册与管理： 充当 TaskManager 上所有 ResultPartition 实例的中心注册表，使得这些分区可以通过其 ResultPartitionID 被快速查找。
// 提供读取服务： 实现 ResultPartitionProvider 接口，为请求数据的下游任务提供创建 ResultSubpartitionView 的服务。
// 处理异步请求： 管理分区请求监听器（PartitionRequestListener）。当下游请求的分区尚未创建时，它会注册一个监听器，并在上游创建分区时通知所有等待的消费者。
// 简而言之，它是 TaskManager 内部网络栈的核心大脑，协调着数据生产者和消费者之间的连接与通信。
public class ResultPartitionManager implements ResultPartitionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);
    // 已注册分区表（核心）。
    // 存储所有当前在该 TaskManager 上活动的 ResultPartition 实例，以 ResultPartitionID 为键进行查找。
    private final Map<ResultPartitionID, ResultPartition> registeredPartitions =
            CollectionUtil.newHashMapWithExpectedSize(16);
    // 监听器管理器表。
    // 存储所有等待特定 ResultPartition 被创建的 PartitionRequestListenerManager 实例。用于实现异步等待逻辑，键为尚未注册的分区 ID。
    @GuardedBy("registeredPartitions")
    private final Map<ResultPartitionID, PartitionRequestListenerManager> listenerManagers =
            new HashMap<>();
    // 超时检查调度任务。
    // 如果配置了超时，这是一个被调度的任务（ScheduledFuture），用于定期执行 checkRequestPartitionListeners 方法，检查等待的分区请求是否超时。
    @Nullable private ScheduledFuture<?> partitionListenerTimeoutChecker;
    // 监听器超时时间。
    // 定义等待 Result Partition 注册的最长毫秒数。
    private final int partitionListenerTimeout;
    // 关闭标志。
    // 标志 ResultPartitionManager 是否已经关闭。
    // 一旦关闭，禁止新的注册操作。
    private boolean isShutdown;

    @VisibleForTesting
    public ResultPartitionManager() {
        this(0, null);
    }

    public ResultPartitionManager(
            int partitionListenerTimeout, ScheduledExecutor scheduledExecutor) {
        this.partitionListenerTimeout = partitionListenerTimeout;
        if (partitionListenerTimeout > 0 && scheduledExecutor != null) {
            this.partitionListenerTimeoutChecker =
                    scheduledExecutor.scheduleWithFixedDelay(
                            this::checkRequestPartitionListeners,
                            partitionListenerTimeout,
                            partitionListenerTimeout,
                            TimeUnit.MILLISECONDS);
        }
    }

    public void registerResultPartition(ResultPartition partition) throws IOException {
        PartitionRequestListenerManager listenerManager;
        synchronized (registeredPartitions) {
            checkState(!isShutdown, "Result partition manager already shut down.");

            ResultPartition previous =
                    registeredPartitions.put(partition.getPartitionId(), partition);

            if (previous != null) {
                throw new IllegalStateException("Result partition already registered.");
            }

            listenerManager = listenerManagers.remove(partition.getPartitionId());
        }
        if (listenerManager != null) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreated(partition);
            }
        }

        LOG.debug("Registered {}.", partition);
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            BufferAvailabilityListener availabilityListener)
            throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                throw new PartitionNotFoundException(partitionId);
            }

            LOG.debug("Requesting subpartitions {} of {}.", subpartitionIndexSet, partition);

            subpartitionView =
                    partition.createSubpartitionView(subpartitionIndexSet, availabilityListener);
        }

        return subpartitionView;
    }

    @Override
    public Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener partitionRequestListener)
            throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                listenerManagers
                        .computeIfAbsent(partitionId, key -> new PartitionRequestListenerManager())
                        .registerListener(partitionRequestListener);
                subpartitionView = null;
            } else {

                LOG.debug("Requesting subpartitions {} of {}.", subpartitionIndexSet, partition);

                subpartitionView =
                        partition.createSubpartitionView(
                                subpartitionIndexSet, availabilityListener);
            }
        }

        return subpartitionView == null ? Optional.empty() : Optional.of(subpartitionView);
    }

    @Override
    public void releasePartitionRequestListener(PartitionRequestListener listener) {
        synchronized (registeredPartitions) {
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.get(listener.getResultPartitionId());
            if (listenerManager != null) {
                listenerManager.remove(listener.getReceiverId());
                if (listenerManager.isEmpty()) {
                    listenerManagers.remove(listener.getResultPartitionId());
                }
            }
        }
    }

    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
        PartitionRequestListenerManager listenerManager;
        synchronized (registeredPartitions) {
            ResultPartition resultPartition = registeredPartitions.remove(partitionId);
            if (resultPartition != null) {
                resultPartition.release(cause);
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            listenerManager = listenerManagers.remove(partitionId);
        }
        if (listenerManager != null && !listenerManager.isEmpty()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
    }

    public void shutdown() {
        synchronized (registeredPartitions) {
            LOG.debug(
                    "Releasing {} partitions because of shutdown.",
                    registeredPartitions.values().size());

            for (ResultPartition partition : registeredPartitions.values()) {
                partition.release();
            }

            registeredPartitions.clear();

            releaseListenerManagers();

            // stop the timeout checks for the TaskManagers
            if (partitionListenerTimeoutChecker != null) {
                partitionListenerTimeoutChecker.cancel(false);
                partitionListenerTimeoutChecker = null;
            }

            isShutdown = true;

            LOG.debug("Successful shutdown.");
        }
    }

    private void releaseListenerManagers() {
        for (PartitionRequestListenerManager listenerManager : listenerManagers.values()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
        listenerManagers.clear();
    }

    /** Check whether the partition request listener is timeout. */
    private void checkRequestPartitionListeners() {
        List<PartitionRequestListener> timeoutPartitionRequestListeners = new LinkedList<>();
        synchronized (registeredPartitions) {
            if (isShutdown) {
                return;
            }
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<ResultPartitionID, PartitionRequestListenerManager>> iterator =
                    listenerManagers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ResultPartitionID, PartitionRequestListenerManager> entry =
                        iterator.next();
                PartitionRequestListenerManager partitionRequestListeners = entry.getValue();
                partitionRequestListeners.removeExpiration(
                        now, partitionListenerTimeout, timeoutPartitionRequestListeners);
                if (partitionRequestListeners.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        for (PartitionRequestListener partitionRequestListener : timeoutPartitionRequestListeners) {
            partitionRequestListener.notifyPartitionCreatedTimeout();
        }
    }

    @VisibleForTesting
    public Map<ResultPartitionID, PartitionRequestListenerManager> getListenerManagers() {
        return listenerManagers;
    }

    // ------------------------------------------------------------------------
    // Notifications
    // ------------------------------------------------------------------------

    void onConsumedPartition(ResultPartition partition) {
        LOG.debug("Received consume notification from {}.", partition);

        synchronized (registeredPartitions) {
            final ResultPartition previous =
                    registeredPartitions.remove(partition.getPartitionId());
            // Release the partition if it was successfully removed
            if (partition == previous) {
                partition.release();
                ResultPartitionID partitionId = partition.getPartitionId();
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.remove(partition.getPartitionId());
            checkState(
                    listenerManager == null || listenerManager.isEmpty(),
                    "The partition request listeners is not empty for "
                            + partition.getPartitionId());
        }
    }

    public Collection<ResultPartitionID> getUnreleasedPartitions() {
        synchronized (registeredPartitions) {
            return registeredPartitions.keySet();
        }
    }

    public Optional<ShuffleMetrics> getMetricsOfPartition(ResultPartitionID partitionId) {
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                return Optional.empty();
            }

            return Optional.of(
                    new DefaultShuffleMetrics(
                            partition.getResultPartitionBytes().createSnapshot()));
        }
    }
}
