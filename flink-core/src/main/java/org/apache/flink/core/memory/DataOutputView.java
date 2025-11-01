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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Public;

import java.io.DataOutput;
import java.io.IOException;

/**
 * This interface defines a view over some memory that can be used to sequentially write contents to
 * the memory. The view is typically backed by one or more {@link
 * org.apache.flink.core.memory.MemorySegment}.
 */
// 它定义了一个视图（View），用于对底层内存进行顺序写入操作。
// 这个视图通常由一个或多个 MemorySegment（Flink 用于管理堆内/堆外内存的基本单元）支持。
// 它继承自标准的 Java I/O 接口 java.io.DataOutput，因此它除了提供自己的特有方法外，还具备 DataOutput 的所有标准写入方法（如 writeInt(), writeLong(), writeBoolean(), write(byte[]) 等）。
@Public
public interface DataOutputView extends DataOutput {

    /**
     * Skips {@code numBytes} bytes memory. If some program reads the memory that was skipped over,
     * the results are undefined.
     *
     * @param numBytes The number of bytes to skip.
     * @throws IOException Thrown, if any I/O related problem occurred such that the view could not
     *     be advanced to the desired position.
     */
    // 跳过指定数量的字节。此方法将视图内部的写入位置向前推进 numBytes 字节。
    // 用途： 预留空间，例如写入长度前缀或占位符，之后再返回来填充。
    void skipBytesToWrite(int numBytes) throws IOException;

    /**
     * Copies {@code numBytes} bytes from the source to this view.
     *
     * @param source The source to copy the bytes from.
     * @param numBytes The number of bytes to copy.
     * @throws IOException Thrown, if any I/O related problem occurred, such that either the input
     *     view could not be read, or the output could not be written.
     */
    // 从另一个数据输入视图 (DataInputView) 复制指定数量的字节到当前视图。
    // 用途： 实现内存到内存或缓冲区到缓冲区的高效批量数据复制，避免了先读入到临时字节数组再写入的开销。
    void write(DataInputView source, int numBytes) throws IOException;
}
