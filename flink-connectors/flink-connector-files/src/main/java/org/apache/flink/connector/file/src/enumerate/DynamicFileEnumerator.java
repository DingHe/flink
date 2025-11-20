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
import org.apache.flink.table.connector.source.DynamicFilteringData;

/**
 * {@code FileEnumerator} that supports dynamic filtering. The enumerator only enumerates splits
 * that exist in the given {@link DynamicFilteringData}, while enumerates all splits if no
 * DynamicFilteringData is provided when #enumerateSplits is called.
 */
// DynamicFileEnumerator 接口是 Flink 文件 Source (FileSource) 中用于支持**动态过滤（Dynamic Filtering）**功能的 FileEnumerator 扩展。
// 它允许文件枚举器在发现和切割文件分片（Splits）的过程中，利用从 Flink Table/SQL 优化器获得的运行时信息来**裁剪（Prune）**不需要读取的文件或分区。
// 动态过滤裁剪： 它增加了一个机制，允许外部（通常是 Flink Job Coordinator）将 DynamicFilteringData 注入其中。在执行 enumerateSplits 时，枚举器将利用这些数据只生成包含在过滤数据中的文件或分区所对应的 Splits。
// 兼容性： 如果没有提供 DynamicFilteringData，它会退化到标准行为，即枚举所有相关文件。
// 它是一个智能文件扫描器，能够根据动态接收到的过滤条件（例如，Join 小表的分区键值）来有选择地生成需要处理的文件分片，以避免读取不必要的数据，实现 I/O 优化。
@PublicEvolving
public interface DynamicFileEnumerator extends FileEnumerator {

    /**
     * Provides a {@link DynamicFilteringData} for filtering while the enumerator is enumerating
     * splits.
     *
     * <p>The {@link DynamicFilteringData} is typically collected by a collector operator, and
     * transferred here by a coordinating event. The method should never be called directly by
     * users.
     */
    // 设置动态过滤数据。
    void setDynamicFilteringData(DynamicFilteringData data);

    /** Factory for the {@link DynamicFileEnumerator}. */
    // DynamicFileEnumerator 的创建工厂。
    @FunctionalInterface
    interface Provider extends FileEnumerator.Provider {

        DynamicFileEnumerator create();
    }
}
