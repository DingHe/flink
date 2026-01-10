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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Enables to push down a limit (the expected maximum number of produced records) into a {@link
 * ScanTableSource}.
 *
 * <p>It might be beneficial to perform the limiting as early as possible in order to be close to
 * the actual data generation.
 *
 * <p>A source can perform the limiting on a best-effort basis. During runtime, it must not
 * guarantee that the number of emitted records is less than or equal to the limit.
 *
 * <p>Regardless if this interface is implemented or not, a limit is also applied in a subsequent
 * operation after the source.
 */
// 主要作用是将数据读取的最大行数限制（Limit）告知数据源
// 在 SQL 查询中，我们经常使用 LIMIT n（例如 SELECT * FROM users LIMIT 10）。
// 默认行为（不实现此接口）：Flink 会从数据源读取所有匹配的数据，然后在内存中通过一个专门的算子来截断前 10 条数据，丢弃之后的所有数据。
// 优化行为（实现此接口）：Flink 优化器会将 10 这个数值传给物理 Source。Source 在读取外部系统（如 MySQL, HBase 或文件系统）时，可以在读取到 10 条数据后立即停止读取并关闭连接/文件流。

@PublicEvolving
public interface SupportsLimitPushDown {

    /**
     * Provides the expected maximum number of produced records for limiting on a best-effort basis.
     */
    void applyLimit(long limit);
}
