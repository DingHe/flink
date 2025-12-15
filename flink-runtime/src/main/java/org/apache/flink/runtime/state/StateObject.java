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

import java.io.Serializable;
import java.util.EnumMap;

/**
 * Base of all handles that represent checkpointed state in some form. The object may hold the
 * (small) state directly, or contain a file path (state is in the file), or contain the metadata to
 * access the state stored in some external database.
 *
 * <p>State objects define how to {@link #discardState() discard state} and how to access the {@link
 * #getStateSize() size of the state}.
 *
 * <p>State Objects are transported via RPC between <i>JobManager</i> and <i>TaskManager</i> and
 * must be {@link java.io.Serializable serializable} to support that.
 *
 * <p>Some State Objects are stored in the checkpoint/savepoint metadata. For long-term
 * compatibility, they are not stored via {@link java.io.Serializable Java Serialization}, but
 * through custom serializers.
 */
// StateObject 是 Flink 所有被持久化的状态（state snapshot）句柄的基类接口。
// 用于描述：
// Checkpoint 或 Savepoint 保存下来的状态信息；
// 状态的 存储位置（内存、本地磁盘、远程存储等）
public interface StateObject extends Serializable {

    /**
     * Discards the state referred to and solemnly owned by this handle, to free up resources in the
     * persistent storage. This method is called when the state represented by this object will not
     * be used anymore.
     */
    // 释放该状态占用的资源（如删除文件、释放缓存、关闭流等）
    // 调用场景：
    // 当一个 Checkpoint 被废弃或过期；
    // 或 Job 被取消；
    // 或 Flink 清理旧状态 时。
    void discardState() throws Exception;

    /**
     * Returns the size of the state in bytes. If the size is not known, this method should return
     * {@code 0}.
     *
     * <p>The values produced by this method are only used for informational purposes and for
     * metrics/monitoring. If this method returns wrong values, the checkpoints and recovery will
     * still behave correctly. However, efficiency may be impacted (wrong space pre-allocation) and
     * functionality that depends on metrics (like monitoring) will be impacted.
     *
     * <p>Note for implementors: This method should not perform any I/O operations while obtaining
     * the state size (hence it does not declare throwing an {@code IOException}). Instead, the
     * state size should be stored in the state object, or should be computable from the state
     * stored in this object. The reason is that this method is called frequently by several parts
     * of the checkpointing and issuing I/O requests from this method accumulates a heavy I/O load
     * on the storage system at higher scale.
     *
     * @return Size of the state in bytes.
     */
    // 返回当前状态的 大小（字节数）
    long getStateSize();

    /**
     * Collects statistics about state size and location from the state object.
     *
     * @implNote default implementation reports {@link StateObject#getStateSize()} as size and
     *     {@link StateObjectLocation#UNKNOWN} as location.
     * @param collector the statistics collector.
     */
    // 用于收集该状态对象的统计信息（大小 + 存储位置）
    default void collectSizeStats(StateObjectSizeStatsCollector collector) {
        collector.add(StateObjectLocation.UNKNOWN, getStateSize());
    }

    /** Enum for state locations. */
    // 定义状态存储的位置类别
    enum StateObjectLocation {
        LOCAL_MEMORY,
        LOCAL_DISK, // 状态存在本地磁盘（如 RocksDB 本地快照）
        REMOTE,
        UNKNOWN,
    }

    /**
     * Collector for size and location stats from a state object via {@link
     * StateObject#collectSizeStats(StateObjectSizeStatsCollector)}.
     */
    // 用于收集并汇总多个 StateObject 的大小统计信息。
    final class StateObjectSizeStatsCollector {
        private final EnumMap<StateObjectLocation, Long> stats;

        private StateObjectSizeStatsCollector() {
            stats = new EnumMap<>(StateObjectLocation.class);
        }

        public void add(StateObjectLocation key, long value) {
            stats.compute(
                    key,
                    (k, v) -> {
                        if (v != null) {
                            return v + value;
                        } else {
                            return value;
                        }
                    });
        }

        public EnumMap<StateObjectLocation, Long> getStats() {
            return stats;
        }

        public static StateObjectSizeStatsCollector create() {
            return new StateObjectSizeStatsCollector();
        }

        @Override
        public String toString() {
            return "StateObjectSizeStatsCollector{" + "stats=" + stats + '}';
        }
    }
}
