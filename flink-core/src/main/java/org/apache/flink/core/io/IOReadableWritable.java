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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This interface must be implemented by every class whose objects have to be serialized to their
 * binary representation and vice-versa. In particular, records have to implement this interface in
 * order to specify how their data can be transferred to a binary representation.
 *
 * <p>When implementing this Interface make sure that the implementing class has a default
 * (zero-argument) constructor!
 */
// IOReadableWritable 接口是 Flink 中用于实现**自定义序列化（Serialization）和反序列化（Deserialization）**的核心契约。
// 数据交换的基础： 任何需要在 Flink 运行时环境（如在网络传输、磁盘 I/O 或内存缓冲区中存储）中以二进制形式表示、传输和存储的对象，都必须实现此接口。
// 定义二进制表示： 它定义了两个方法，write 和 read，要求实现类明确指出如何将自己的内部状态写入到一个二进制流中，以及如何从二进制流中重建自己的内部状态。
@Public
public interface IOReadableWritable {

    /**
     * Writes the object's internal data to the given data output view.
     *
     * @param out the output view to receive the data.
     * @throws IOException thrown if any error occurs while writing to the output stream
     */
    void write(DataOutputView out) throws IOException;

    /**
     * Reads the object's internal data from the given data input view.
     *
     * @param in the input view to read the data from
     * @throws IOException thrown if any error occurs while reading from the input stream
     */
    void read(DataInputView in) throws IOException;
}
