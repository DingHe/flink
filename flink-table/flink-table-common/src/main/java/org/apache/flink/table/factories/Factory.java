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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

/**
 * Base interface for all kind of factories that create object instances from a list of key-value
 * pairs in Flink's Table & SQL API.
 *
 * <p>A factory is uniquely identified by {@link Class} and {@link #factoryIdentifier()}.
 *
 * <p>The list of available factories is discovered using Java's Service Provider Interfaces (SPI).
 * Classes that implement this interface can be added to {@code
 * META_INF/services/org.apache.flink.table.factories.Factory} in JAR files.
 *
 * <p>Every factory declares a set of required and optional options. This information will not be
 * used during discovery but is helpful when generating documentation and performing validation. A
 * factory may discover further (nested) factories, the options of the nested factories must not be
 * declared in the sets of this factory.
 *
 * <p>It is the responsibility of each factory to perform validation before returning an instance.
 *
 * <p>For consistency, the following style for key names of {@link ConfigOption} is recommended:
 *
 * <ul>
 *   <li>Try to <b>reuse</b> key names as much as possible. Use other factory implementations as an
 *       example.
 *   <li>Key names should be declared in <b>lower case</b>. Use "-" instead of dots or camel case to
 *       split words.
 *   <li>Key names should be <b>hierarchical</b> where appropriate. Think about how one would define
 *       such a hierarchy in JSON or YAML file (e.g. {@code sink.bulk-flush.max-actions}).
 *   <li>In case of a hierarchy, try not to use the higher level again in the key name (e.g. do
 *       {@code sink.partitioner} instead of {@code sink.sink-partitioner}) to <b>keep the keys
 *       short</b>.
 *   <li>Key names which can be templated, e.g. to refer to a specific column, should be listed
 *       using '#' as the placeholder symbol. For example, use {@code fields.#.min}.
 * </ul>
 */
//Flink 的 Table & SQL API 中用于从键值对列表创建对象实例的所有类型工厂的基础接口。
//让 Flink 能够发现、识别和创建各种组件，如连接器（Connector）、格式（Format）、函数（Function）等。
// 通过这种方式，Flink 实现了可插拔和可扩展的架构，允许开发者通过实现这个接口来添加新的组件，而无需修改 Flink 核心代码
@PublicEvolving
public interface Factory {

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    //返回一个字符串，用于唯一标识该工厂
    //Flink 使用这个标识符来查找和加载对应的工厂。
    // 例如，当你在 SQL 中写 CREATE TABLE ... WITH ('connector' = 'kafka', ...) 时，
    // Flink 就会根据 'kafka' 这个标识符去找到对应的 Kafka 连接器工厂
    String factoryIdentifier();

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in
     * addition to {@link #optionalOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    //包含了该工厂必须提供的配置选项（ConfigOption）
    Set<ConfigOption<?>> requiredOptions();

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in
     * addition to {@link #requiredOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    //包含了该工厂可选的配置选项（ConfigOption）
    Set<ConfigOption<?>> optionalOptions();
}
