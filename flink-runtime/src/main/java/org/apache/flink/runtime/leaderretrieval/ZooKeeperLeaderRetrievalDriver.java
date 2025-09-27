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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionDriver;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Watcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.UUID;

import static org.apache.flink.runtime.util.ZooKeeperUtils.RESOURCE_MANAGER_NODE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The counterpart to the {@link ZooKeeperLeaderElectionDriver}. {@link LeaderRetrievalService}
 * implementation for Zookeeper. It retrieves the current leader which has been elected by the
 * {@link ZooKeeperLeaderElectionDriver}. The leader address as well as the current leader session
 * ID is retrieved from ZooKeeper.
 */
public class ZooKeeperLeaderRetrievalDriver implements LeaderRetrievalDriver {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderRetrievalDriver.class);
    //这是与 ZooKeeper 集群的连接客户端，通过它与 ZooKeeper 进行交互,CuratorFramework 是 Curator 库中提供的一个高级 API，用于简化与 ZooKeeper 的交互
    /** Connection to the used ZooKeeper quorum. */
    private final CuratorFramework client;
    //TreeCache 是 Curator 提供的一种缓存机制，用于缓存 ZooKeeper 树形结构中的节点变化。它监控一个特定的 ZooKeeper 节点（在这里是包含领导者信息的节点），并在节点变化时触发事件回调
    /** Curator recipe to watch changes of a specific ZooKeeper node. */
    private final TreeCache cache;
    //ZooKeeper 中存储领导者信息的节点路径。这个路径是通过调用 ZooKeeperUtils.generateConnectionInformationPath(path) 生成的
    private final String connectionInformationPath;
    //监听 ZooKeeper 连接状态的变化，例如连接成功、连接丢失或重连等。在连接状态发生变化时，会调用 handleStateChange 方法处理不同的状态
    private final ConnectionStateListener connectionStateListener =
            (client, newState) -> handleStateChange(newState);
    //处理领导者地址变化的事件。每当领导者发生变化时，会通过该对象通知监听器
    private final LeaderRetrievalEventHandler leaderRetrievalEventHandler;
    //控制何时清除领导者信息，并通知监听器。如果连接状态发生变化（例如丢失或挂起），会根据这个策略决定是否清除当前的领导者信息
    private final LeaderInformationClearancePolicy leaderInformationClearancePolicy;

    private final FatalErrorHandler fatalErrorHandler;

    private volatile boolean running;

    /**
     * Creates a leader retrieval service which uses ZooKeeper to retrieve the leader information.
     *
     * @param client Client which constitutes the connection to the ZooKeeper quorum
     * @param path Path of the ZooKeeper node which contains the leader information
     * @param leaderRetrievalEventHandler Handler to notify the leader changes.
     * @param leaderInformationClearancePolicy leaderInformationClearancePolicy controls when the
     *     leader information is being cleared
     * @param fatalErrorHandler Fatal error handler
     */
    public ZooKeeperLeaderRetrievalDriver(
            CuratorFramework client,
            String path,
            LeaderRetrievalEventHandler leaderRetrievalEventHandler,
            LeaderInformationClearancePolicy leaderInformationClearancePolicy,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        this.client = checkNotNull(client, "CuratorFramework client");
        this.connectionInformationPath = ZooKeeperUtils.generateConnectionInformationPath(path);
        this.cache =
                ZooKeeperUtils.createTreeCache(
                        client,
                        connectionInformationPath,
                        this::retrieveLeaderInformationFromZooKeeper);

        this.leaderRetrievalEventHandler = checkNotNull(leaderRetrievalEventHandler);
        this.leaderInformationClearancePolicy = leaderInformationClearancePolicy;
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

        cache.start();

        client.getConnectionStateListenable().addListener(connectionStateListener);

        LOG.debug(
                "Monitoring data change in {}",
                ZooKeeperUtils.generateZookeeperPath(
                        client.getNamespace(), connectionInformationPath));

        running = true;
    }

    @Override
    public void close() throws Exception {
        if (!running) {
            return;
        }

        running = false;

        LOG.info("Closing {}.", this);

        client.getConnectionStateListenable().removeListener(connectionStateListener);

        cache.close();

        try {
            if (client.getZookeeperClient().isConnected()
                    && !connectionInformationPath.contains(RESOURCE_MANAGER_NODE)) {
                client.watchers()
                        .removeAll()
                        .ofType(Watcher.WatcherType.Any)
                        .forPath(connectionInformationPath);
            }
        } catch (KeeperException.NoWatcherException e) {
            // Ignore the no watcher exception as it's just a safetynet to fix watcher leak issue.
            // For more details, please refer to FLINK-33053.
        }
    }

    private void retrieveLeaderInformationFromZooKeeper() {
        try {
            LOG.debug("Leader node has changed.");

            final ChildData childData = cache.getCurrentData(connectionInformationPath);

            if (childData != null) {
                final byte[] data = childData.getData();
                if (data != null && data.length > 0) {
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    ObjectInputStream ois = new ObjectInputStream(bais);

                    final String leaderAddress = ois.readUTF();
                    final UUID leaderSessionID = (UUID) ois.readObject();
                    leaderRetrievalEventHandler.notifyLeaderAddress(
                            LeaderInformation.known(leaderSessionID, leaderAddress));
                    return;
                }
            }
            notifyNoLeader();
        } catch (Exception e) {
            fatalErrorHandler.onFatalError(
                    new LeaderRetrievalException("Could not handle node changed event.", e));
            ExceptionUtils.checkInterrupted(e);
        }
    }

    private void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                LOG.debug("Connected to ZooKeeper quorum. Leader retrieval can start.");
                break;
            case SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended, waiting for reconnection.");
                if (leaderInformationClearancePolicy
                        == LeaderInformationClearancePolicy.ON_SUSPENDED_CONNECTION) {
                    notifyNoLeader();
                }
                break;
            case RECONNECTED:
                LOG.info(
                        "Connection to ZooKeeper was reconnected. Leader retrieval can be restarted.");
                onReconnectedConnectionState();
                break;
            case LOST:
                LOG.warn(
                        "Connection to ZooKeeper lost. Can no longer retrieve the leader from "
                                + "ZooKeeper.");
                notifyNoLeader();
                break;
        }
    }

    private void notifyNoLeader() {
        leaderRetrievalEventHandler.notifyLeaderAddress(LeaderInformation.empty());
    }

    private void onReconnectedConnectionState() {
        // check whether we find some new leader information in ZooKeeper
        retrieveLeaderInformationFromZooKeeper();
    }

    @Override
    public String toString() {
        return "ZookeeperLeaderRetrievalDriver{"
                + "connectionInformationPath='"
                + connectionInformationPath
                + '\''
                + '}';
    }

    @VisibleForTesting
    public String getConnectionInformationPath() {
        return connectionInformationPath;
    }

    /** Policy when to clear the leader information and to notify the listener about it. */
    public enum LeaderInformationClearancePolicy {
        // clear the leader information as soon as the ZK connection is suspended
        ON_SUSPENDED_CONNECTION,

        // clear the leader information only once the ZK connection is lost
        ON_LOST_CONNECTION
    }
}
