/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Optional;

/**
 * A {@link StateObject} that represents state that was written to a stream. The data can be read
 * back via {@link #openInputStream()}.
 */
// 抽象地表示持久化到流（Stream）或文件系统（File System）中的状态数据
// 当算子状态被持久化（Checkpointing）时，数据通常会被写入一个输出流（如 HDFS、S3 或本地文件系统的文件）。StreamStateHandle 就是对这个写入操作的结果和引用的封装
// 数据引用： 不直接存储数据，而是持有读取持久化状态所需的信息或引用（例如文件路径、对象存储键等）。
// 数据访问： 提供统一的接口，允许 Flink 在恢复（Recovery）时，重新打开输入流来读取被保存的状态数据。
public interface StreamStateHandle extends StateObject {

    /**
     * Returns an {@link FSDataInputStream} that can be used to read back the data that was
     * previously written to the stream.
     */
    // 返回一个 FSDataInputStream，该流指向之前写入的状态数据。
    // 这是 Flink 在恢复状态时，用于读取状态数据的关键机制。
    // 实现类负责根据其内部的引用信息（如文件路径）创建并返回该输入流
    FSDataInputStream openInputStream() throws IOException;

    /** @return Content of this handle as bytes array if it is already in memory. */
    // 如果状态在内存中，则返回字节数组。
    // 允许实现类（如某些特殊的内存句柄）在不进行 I/O 操作的情况下，直接以字节数组的形式返回状态内容。
    Optional<byte[]> asBytesIfInMemory();

    /**
     * @return Path to an underlying file represented by this {@link StreamStateHandle} or {@link
     *     Optional#empty()} if there is no such file.
     */
    // 获取底层文件路径。
    // 返回一个 Optional，其中可能包含这个状态句柄引用的底层文件系统路径 (Path)
    default Optional<org.apache.flink.core.fs.Path> maybeGetPath() {
        return Optional.empty();
    }

    /** @return a unique identifier of this handle. */
    // 返回一个唯一的标识符，用于在 Flink 运行时系统中唯一地标识和引用这个持久化的状态句柄
    PhysicalStateHandleID getStreamStateHandleID();
}
