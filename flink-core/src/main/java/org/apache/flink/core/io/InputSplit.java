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

import java.io.Serializable;

/**
 * This interface must be implemented by all kind of input splits that can be assigned to input
 * formats.
 *
 * <p>Input splits are transferred in serialized form via the messages, so they need to be
 * serializable as defined by {@link java.io.Serializable}.
 */
// InputSplit 接口是 Flink 框架中用于实现数据并行读取的核心抽象。
// 定义工作单元： 它代表一个输入数据源的逻辑分片或一个工作单元。例如，当从一个大文件（如 HDFS 文件）读取数据时，InputSplit 可能代表文件中的一个字节范围。
// 实现并行化： Flink 的输入格式（InputFormat）在读取数据之前，会先将整个数据源划分成多个 InputSplit。然后，这些 InputSplit 会被分配给不同的任务并行执行，从而实现数据的分布式和并行处理。
@Public
public interface InputSplit extends Serializable {

    /**
     * Returns the number of this input split.
     *
     * @return the number of this input split
     */
    // 获取分片的编号/索引
    int getSplitNumber();
}
