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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * The {@code FileEnumerator}'s task is to discover all files to be read and to split them into a
 * set of {@link FileSourceSplit}.
 *
 * <p>This includes possibly, path traversals, file filtering (by name or other patterns) and
 * deciding whether to split files into multiple splits, and how to split them.
 */
// FileEnumerator 接口是 Flink 文件 Source (FileSource) 的一个内部组件，它专门负责执行文件发现和分片切割的逻辑。它在 Flink 的 SplitEnumerator 内部被调用，以完成数据源的初始化工作。
// 文件发现： 根据给定的根路径，递归地遍历文件系统，找到所有符合读取条件（例如文件名模式匹配）的文件
// 分片策略执行： 决定是否需要将单个大文件切割成多个文件分片 (FileSourceSplit)，以及如何进行切割（例如根据块大小、文件大小等）。
// 生成 Splits： 将发现的文件及其切割信息封装成一组 FileSourceSplit 对象，供 Flink 的 SplitEnumerator 进行分配。
// 它是文件 Source 的目录扫描和分块器，将用户指定的路径转化为可供并行读取的最小工作单元 (FileSourceSplit)。
@PublicEvolving
public interface FileEnumerator {

    /**
     * Generates all file splits for the relevant files under the given paths. The {@code
     * minDesiredSplits} is an optional hint indicating how many splits would be necessary to
     * exploit parallelism properly.
     */
    // 枚举并生成文件分片。
    // 这是文件发现和分片切割的主要逻辑方法。
    Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException;

    // ------------------------------------------------------------------------

    /**
     * Factory for the {@code FileEnumerator}, to allow the {@code FileEnumerator} to be eagerly
     * initialized and to not be serializable.
     */
    // FileEnumerator 的创建工厂。
    // 允许 FileEnumerator 实例本身不必是可序列化的，而由可序列化的 Provider 在 TaskManager 或 JobManager 上动态创建。
    @FunctionalInterface
    interface Provider extends Serializable {

        FileEnumerator create();
    }
}
