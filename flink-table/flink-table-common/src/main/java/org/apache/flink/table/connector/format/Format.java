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

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

/**
 * Base interface for connector formats.
 *
 * <p>Depending on the kind of external system, a connector might support different encodings for
 * reading and writing rows. This interface is an intermediate representation before constructing
 * actual runtime implementation.
 *
 * <p>Formats can be distinguished along two dimensions:
 *
 * <ul>
 *   <li>Context in which the format is applied ({@link DynamicTableSource} or {@link
 *       DynamicTableSink}).
 *   <li>Runtime implementation interface that is required (e.g. {@link DeserializationSchema} or
 *       some bulk interface).
 * </ul>
 *
 * <p>A {@link DynamicTableFactory} can search for a format that it is accepted by the connector.
 *
 * @see DecodingFormat
 * @see EncodingFormat
 */
// Flink Table API 中连接器格式的基础接口。
// 它的主要作用是作为一种中间表示，用于在 Flink 的 Table 模块中描述外部系统的数据编码和解码方式
//将数据格式的定义与具体的运行时实现（如 DeserializationSchema 或 SerializationSchema）分离，使得 Flink 的连接器工厂（DynamicTableFactory）能够更灵活地配置和构造不同格式的连接器。
// 例如，一个 Kafka 连接器可能支持 JSON、CSV 等多种格式，而 Format 接口就用来抽象这些格式的共同行为
@PublicEvolving
public interface Format {

    /**
     * Returns the set of changes that a connector (and transitively the planner) can expect during
     * runtime.
     */
    ChangelogMode getChangelogMode();
}
