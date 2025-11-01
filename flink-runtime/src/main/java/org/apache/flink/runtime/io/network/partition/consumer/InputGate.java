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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 */
// 抽象化 Flink 任务（Task）接收来自上游任务数据的逻辑入口（或称“输入网关”）
// 一个下游任务（例如 Reduce 2）需要从上游中间结果（Intermediate Result）的不同分区（Partition）中消费数据子分区（Subpartition）。
// InputGate 就是负责聚合和管理所有这些**输入通道（InputChannel）**的实体，并提供统一的、异步的、基于拉取（Pulling）的数据消费接口。
// 结合了以下关键职责：
// 道聚合： 管理一组 InputChannel，每个通道对应上游的一个数据子分区。
// 数据/事件拉取： 提供 pollNext() 和 getNext() 方法，用于非阻塞或阻塞地拉取 BufferOrEvent。
// 可用性通知： 继承 PullingAsyncDataInput 接口，提供 getAvailableFuture()，支持异步调度。
public abstract class InputGate
        implements PullingAsyncDataInput<BufferOrEvent>, AutoCloseable, ChannelStateHolder {
    // 普通数据可用性辅助器。 用于管理 普通数据（非优先级事件）的可用性状态和对应的 CompletableFuture，
    // 这是 getAvailableFuture() 的底层实现。
    protected final AvailabilityHelper availabilityHelper = new AvailabilityHelper();
    // 优先级事件可用性辅助器。
    // 专门用于管理 优先级事件（如 CheckpointBarrier）的可用性状态和对应的 CompletableFuture
    protected final AvailabilityHelper priorityAvailabilityHelper = new AvailabilityHelper();

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (int index = 0, numChannels = getNumberOfInputChannels();
                index < numChannels;
                index++) {
            final InputChannel channel = getChannel(index);
            if (channel instanceof ChannelStateHolder) {
                ((ChannelStateHolder) channel).setChannelStateWriter(channelStateWriter);
            }
        }
    }
    // 获取输入通道数量。
    // 返回该输入网关所连接的输入通道的总数量。
    public abstract int getNumberOfInputChannels();
    // 检查是否结束。
    // 返回 true 表示所有输入通道都已完成并关闭，即整个输入已结束。
    public abstract boolean isFinished();

    /**
     * Blocking call waiting for next {@link BufferOrEvent}.
     *
     * <p>Note: It should be guaranteed that the previous returned buffer has been recycled before
     * getting next one.
     *
     * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
     */
    // 阻塞式获取下一个元素
    public abstract Optional<BufferOrEvent> getNext() throws IOException, InterruptedException;

    /**
     * Poll the {@link BufferOrEvent}.
     *
     * <p>Note: It should be guaranteed that the previous returned buffer has been recycled before
     * polling next one.
     *
     * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()}
     *     returns true.
     */
    // 非阻塞式拉取下一个元素
    public abstract Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException;
    // 发送任务事件。
    // 用于将一个控制事件（如请求分区索引）发送给所有连接的上游生产者（即所有 ResultPartition）
    public abstract void sendTaskEvent(TaskEvent event) throws IOException;

    /**
     * @return a future that is completed if there are more records available. If there are more
     *     records available immediately, {@link #AVAILABLE} should be returned. Previously returned
     *     not completed futures should become completed once there are more records available.
     */
    // 获取数据可用性 Future
    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }
    // 恢复通道消费。
    // 恢复特定输入通道的数据消费。
    // 这在流量控制机制中非常关键。
    public abstract void resumeConsumption(InputChannelInfo channelInfo) throws IOException;
    // 确认所有记录已处理。
    // 通知特定输入通道，所有已接收到的记录都已被任务逻辑处理完毕。
    public abstract void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo)
            throws IOException;

    /** Returns the channel of this gate. */
    // 获取特定通道。
    // 通过索引获取该网关管理的某个 InputChannel 实例。
    public abstract InputChannel getChannel(int channelIndex);
    // 获取所有通道信息
    /** Returns the channel infos of this gate. */
    public List<InputChannelInfo> getChannelInfos() {
        return IntStream.range(0, getNumberOfInputChannels())
                .mapToObj(index -> getChannel(index).getChannelInfo())
                .collect(Collectors.toList());
    }

    /**
     * Notifies when a priority event has been enqueued. If this future is queried from task thread,
     * it is guaranteed that a priority event is available and retrieved through {@link #getNext()}.
     */
    public CompletableFuture<?> getPriorityEventAvailableFuture() {
        return priorityAvailabilityHelper.getAvailableFuture();
    }

    /** Simple pojo for INPUT, DATA and moreAvailable. */
    // 用于封装输入组件（INPUT）、数据（DATA）以及两个布尔标志（moreAvailable 和 morePriorityEvents）。
    // 它主要用于 InputGate 内部实现中，将从底层通道获取的数据和通道本身关联起来。
    protected static class InputWithData<INPUT, DATA> {
        protected final INPUT input;
        protected final DATA data;
        protected final boolean moreAvailable;
        protected final boolean morePriorityEvents;

        InputWithData(INPUT input, DATA data, boolean moreAvailable, boolean morePriorityEvents) {
            this.input = checkNotNull(input);
            this.data = checkNotNull(data);
            this.moreAvailable = moreAvailable;
            this.morePriorityEvents = morePriorityEvents;
        }

        @Override
        public String toString() {
            return "InputWithData{"
                    + "input="
                    + input
                    + ", data="
                    + data
                    + ", moreAvailable="
                    + moreAvailable
                    + ", morePriorityEvents="
                    + morePriorityEvents
                    + '}';
        }
    }

    /** Setup gate, potentially heavy-weight, blocking operation comparing to just creation. */
    // 设置网关。
    // 执行网关的初始化设置，这可能涉及重量级或阻塞的操作，例如建立网络连接、初始化内部结构等。
    public abstract void setup() throws IOException;
    // 请求分区。
    // 通知上游生产者开始发送数据。这是数据流启动的命令。
    public abstract void requestPartitions() throws IOException;
    // 获取状态消费完成 Future。
    // 返回一个 Future，当所有从检查点/保存点恢复的通道状态（如果有）都已被该网关消费完毕时，该 Future 会完成。
    public abstract CompletableFuture<Void> getStateConsumedFuture();
    // 完成恢复状态读取
    public abstract void finishReadRecoveredState() throws IOException;
}
