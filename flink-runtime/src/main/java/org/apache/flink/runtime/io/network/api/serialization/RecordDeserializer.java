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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/** Interface for turning sequences of memory segments into records. */
// Flink 网络栈中一个至关重要的接口，它直接处理从网络接收到的原始内存数据，并将其转换为 Flink 运行时可以理解的记录（Record）或流元素（StreamElement）
// 在 Flink 的数据传输层，数据以**缓冲区（Buffer）**的形式在网络中流动。一个完整的记录可能被分成多个缓冲区传输，或者多个记录可能被打包在一个缓冲区中。
public interface RecordDeserializer<T extends IOReadableWritable> {

    /** Status of the deserialization result. */
    enum DeserializationResult {
        // 部分记录。
        // 表明当前缓冲区已耗尽，但尚未收集到一条完整的记录。
        PARTIAL_RECORD(false, true),
        // 中间记录。
        // 表明已成功反序列化出一条完整的记录，但当前缓冲区尚未耗尽，仍有更多数据可供反序列化。
        INTERMEDIATE_RECORD_FROM_BUFFER(true, false),
        // 缓冲区末尾记录。
        // 表明已成功反序列化出一条完整的记录，并且当前缓冲区中的数据已全部耗尽。
        LAST_RECORD_FROM_BUFFER(true, true);

        private final boolean isFullRecord;

        private final boolean isBufferConsumed;

        private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
            this.isFullRecord = isFullRecord;
            this.isBufferConsumed = isBufferConsumed;
        }

        public boolean isFullRecord() {
            return this.isFullRecord;
        }

        public boolean isBufferConsumed() {
            return this.isBufferConsumed;
        }
    }

    DeserializationResult getNextRecord(T target) throws IOException;

    void setNextBuffer(Buffer buffer) throws IOException;

    void clear();

    /**
     * Gets the unconsumed buffer which needs to be persisted in unaligned checkpoint scenario.
     *
     * <p>Note that the unconsumed buffer might be null if the whole buffer was already consumed
     * before and there are no partial length or data remained in the end of buffer.
     */
    CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
}
