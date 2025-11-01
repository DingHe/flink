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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Either type for {@link Buffer} or {@link AbstractEvent} instances tagged with the channel index,
 * from which they were received.
 */
// 作为 Flink 任务接收端（Input Gate）从上游获取数据的统一包装器（Wrapper），
// 它只能包含一个数据缓冲区（Buffer）或者一个控制事件（AbstractEvent），并附带元数据，如数据来源通道信息和后续数据的可用性。
// 在 Flink 的运行时网络栈中，数据和控制信号（事件，如检查点屏障、结束标志）流经相同的物理通道。这个类就是为了将这两种类型的信息统一起来，方便下游任务处理：
// Flink 网络数据接收的最小单位，将数据和控制流在同一个对象中传递给任务逻辑
public class BufferOrEvent {
    // 数据缓冲区。 如果此对象包含数据，则此字段非空。缓冲区用于承载用户数据。
    private final Buffer buffer;
    // 控制事件。 如果此对象包含控制信号（如 CheckpointBarrier），则此字段非空。
    private final AbstractEvent event;
    // 是否具有优先级。
    // 仅对包含 event 的情况有效，表示该事件是否是高优先级事件，例如检查点屏障
    private final boolean hasPriority;

    /**
     * Indicate availability of further instances for the union input gate. This is not needed
     * outside of the input gate unioning logic and cannot be set outside of the consumer package.
     */
    // 更多数据可用标志（普通数据）。
    // 表示当前通道/网关中是否还有非优先级数据可供消费。这用于输入网关的联合和调度逻辑。
    private boolean moreAvailable;
    // 更多优先级事件可用标志。
    // 表示当前通道中是否还有优先级事件（例如下一个检查点屏障）紧随其后。
    private final boolean morePriorityEvents;
    // 输入通道信息。
    // 唯一标识该 Buffer 或 Event 来自哪个输入通道。
    private InputChannelInfo channelInfo;
    // 如果是 Buffer，则是缓冲区的大小；如果是 Event，通常为 0，除非事件本身包含数据。
    private final int size;

    public BufferOrEvent(
            Buffer buffer,
            InputChannelInfo channelInfo,
            boolean moreAvailable,
            boolean morePriorityEvents) {
        this.buffer = checkNotNull(buffer);
        this.hasPriority = false;
        this.event = null;
        this.channelInfo = channelInfo;
        this.moreAvailable = moreAvailable;
        this.size = buffer.getSize();
        this.morePriorityEvents = morePriorityEvents;
    }

    public BufferOrEvent(
            AbstractEvent event,
            boolean hasPriority,
            InputChannelInfo channelInfo,
            boolean moreAvailable,
            int size,
            boolean morePriorityEvents) {
        this.buffer = null;
        this.hasPriority = hasPriority;
        this.event = checkNotNull(event);
        this.channelInfo = channelInfo;
        this.moreAvailable = moreAvailable;
        this.size = size;
        this.morePriorityEvents = morePriorityEvents;
    }

    @VisibleForTesting
    public BufferOrEvent(Buffer buffer, InputChannelInfo channelInfo) {
        this(buffer, channelInfo, true, false);
    }

    @VisibleForTesting
    public BufferOrEvent(AbstractEvent event, InputChannelInfo channelInfo) {
        this(event, false, channelInfo, true, 0, false);
    }

    public boolean isBuffer() {
        return buffer != null;
    }

    public boolean isEvent() {
        return event != null;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public AbstractEvent getEvent() {
        return event;
    }

    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public void setChannelInfo(InputChannelInfo channelInfo) {
        this.channelInfo = channelInfo;
    }

    public boolean moreAvailable() {
        return moreAvailable;
    }

    public boolean morePriorityEvents() {
        return morePriorityEvents;
    }

    @Override
    public String toString() {
        return String.format(
                "BufferOrEvent [%s, channelInfo = %s, size = %d]",
                isBuffer() ? buffer : (event + " (prio=" + hasPriority + ")"), channelInfo, size);
    }

    public void setMoreAvailable(boolean moreAvailable) {
        this.moreAvailable = moreAvailable;
    }

    public int getSize() {
        return size;
    }

    public boolean hasPriority() {
        return hasPriority;
    }
}
