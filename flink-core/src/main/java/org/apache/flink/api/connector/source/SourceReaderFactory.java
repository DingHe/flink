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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * A factory for creating source reader instances.
 *
 * @param <T> The type of the output elements.
 */
// SourceReaderFactory 的核心作用是 “在并行任务节点（TaskManager）上生产数据读取器”。
// 在 Flink 的分布式架构中，数据读取被分为两个部分：
//SplitEnumerator (分片枚举器)：运行在 JobManager 上，负责发现数据源的分片（如 Kafka 的 Partition 或文件系统的文件）。
//SourceReader (数据读取器)：运行在 TaskManager 的各个并行 Subtask 上，负责真正去读取数据。
// SourceReaderFactory 就是一个“搬运工”和“工厂”： 它定义了如何在各个并行节点上实例化 SourceReader。
// 由于它实现了 Serializable 接口，它会被序列化并发送到集群中的每一个计算节点，然后在本地调用工厂方法来创建读取器。
// 泛型 <T>:代表读取器输出的数据元素类型（例如 RowData 或 String）。
@Public
public interface SourceReaderFactory<T, SplitT extends SourceSplit> extends Serializable {
    /**
     * Creates a new reader to read data from the splits it gets assigned. The reader starts fresh
     * and does not have any state to resume.
     *
     * @param readerContext The {@link SourceReaderContext context} for the source reader.
     * @return A new SourceReader.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause task failure/recovery.
     */
    // 根据提供的上下文环境，创建一个全新的 SourceReader 实例。
    SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception;
}
