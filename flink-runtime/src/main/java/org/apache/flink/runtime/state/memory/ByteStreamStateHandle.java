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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Optional;

/** A state handle that contains stream state in a byte array. */
// ByteStreamStateHandle 是 StreamStateHandle 接口的一个具体实现，它的核心作用是将状态数据直接存储在内存中的一个字节数组 (byte[]) 中
// 内存状态的封装： 用于处理那些体积非常小、适合直接存储在内存中并在 Flink 内部快速传输的状态数据。
// 它是 Flink 的 MemoryStateBackend (内存状态后端) 或 FsStateBackend (文件系统状态后端) 在处理小型元数据或算子状态时的基本构建块。


public class ByteStreamStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = -5280226231202517594L;

    /** The state data. */
    // 状态数据。
    // 存储了实际的状态数据字节数组。
    // 这是该句柄包含的核心状态内容。
    private final byte[] data;

    /**
     * A unique name of by which this state handle is identified and compared. Like a filename, all
     * {@link ByteStreamStateHandle} with the exact same name must also have the exact same content
     * in data.
     */
    // 句柄名称/唯一标识符。
    // 一个唯一的字符串名称，用于标识和比较该状态句柄。
    private final String handleName;

    /** Creates a new ByteStreamStateHandle containing the given data. */
    public ByteStreamStateHandle(String handleName, byte[] data) {
        this.handleName = Preconditions.checkNotNull(handleName);
        this.data = Preconditions.checkNotNull(data);
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        return new ByteStateHandleInputStream(data);
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.of(getData());
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return new PhysicalStateHandleID(handleName);
    }

    public byte[] getData() {
        return data;
    }

    public String getHandleName() {
        return handleName;
    }

    @Override
    public void discardState() {}

    @Override
    public long getStateSize() {
        return data.length;
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        collector.add(StateObjectLocation.LOCAL_MEMORY, getStateSize());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteStreamStateHandle)) {
            return false;
        }

        ByteStreamStateHandle that = (ByteStreamStateHandle) o;
        return handleName.equals(that.handleName);
    }

    @Override
    public int hashCode() {
        return 31 * handleName.hashCode();
    }

    @Override
    public String toString() {
        return "ByteStreamStateHandle{"
                + "handleName='"
                + handleName
                + '\''
                + ", dataBytes="
                + data.length
                + '}';
    }

    /** An input stream view on a byte array. */
    private static final class ByteStateHandleInputStream extends FSDataInputStream {

        private final byte[] data;
        private int index;

        public ByteStateHandleInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public void seek(long desired) throws IOException {
            if (desired >= 0 && desired <= data.length) {
                index = (int) desired;
            } else {
                throw new IOException("position out of bounds");
            }
        }

        @Override
        public long getPos() throws IOException {
            return index;
        }

        @Override
        public int read() throws IOException {
            return index < data.length ? data[index++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            // note that any bounds checking on "byte[] b" happened anyways by the
            // System.arraycopy() call below, so we don't add extra checks here

            final int bytesLeft = data.length - index;
            if (bytesLeft > 0) {
                final int bytesToCopy = Math.min(len, bytesLeft);
                System.arraycopy(data, index, b, off, bytesToCopy);
                index += bytesToCopy;
                return bytesToCopy;
            } else {
                return len == 0 ? 0 : -1;
            }
        }
    }
}
