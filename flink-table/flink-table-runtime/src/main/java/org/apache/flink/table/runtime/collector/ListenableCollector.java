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

package org.apache.flink.table.runtime.collector;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * A listenable collector for lookup join that can be called when an original record was collected.
 */
// ListenableCollector 是一个专门为 Lookup Join（维表关联） 场景设计的工具类。它继承自 TableFunctionCollector，并引入了“监听者”机制。
// 在 Flink SQL 的 Lookup Join 操作中，当算子去外部系统（如 Redis、MySQL）查询到匹配的数据后，需要通过 Collector（收集器）将结果发送到下游。
// 核心作用是允许在数据被收集（Collect）的那一刻触发一个回调动作。
// 解耦： 它将“收集数据”这一动作与“收集后的副作用处理”解耦。
// 状态同步与统计： 主要用于在 Lookup Join 过程中，当某条记录成功匹配并输出时，通知相关的组件（比如用于异步查找的缓存更新或特定的指标统计）。
@Internal
public abstract class ListenableCollector<T> extends TableFunctionCollector<T> {
    // 持有对监听器接口实现的引用
    // 实现“监听”功能的核心。使用了 @Nullable 注解，表示这个监听器是可选的。
    // 如果没有设置监听器，Collector 依然可以正常工作，只是不会触发回调。
    @Nullable private CollectListener<T> collectListener;

    public void setCollectListener(@Nullable CollectListener<T> collectListener) {
        this.collectListener = collectListener;
    }

    protected Optional<CollectListener<T>> getCollectListener() {
        return Optional.ofNullable(collectListener);
    }

    /** An interface can listen on collecting original record. */
    public interface CollectListener<T> {

        /** A callback method when an original record was collected, do nothing by default. */
        default void onCollect(T record) {}
    }
}
