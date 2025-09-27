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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;

/**
 * Creates a {@link DynamicTableSource} instance from a {@link CatalogTable} and additional context
 * information.
 *
 * <p>See {@link Factory} for more information about the general design of a factory.
 */
//用于创建动态表数据源（DynamicTableSource）的工厂接口。
// 它的核心作用是根据目录表（CatalogTable）和上下文信息（Context），实例化一个负责从外部存储系统读取数据的连接器
@PublicEvolving
public interface DynamicTableSourceFactory extends DynamicTableFactory {

    /**
     * Creates a {@link DynamicTableSource} instance from a {@link CatalogTable} and additional
     * context information.
     *
     * <p>An implementation should perform validation and the discovery of further (nested)
     * factories in this method.
     */
    //负责实际创建 DynamicTableSource 实例
    DynamicTableSource createDynamicTableSource(Context context);
}
