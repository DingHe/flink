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

package org.apache.flink.table.runtime.operators.window.tvf.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** A wrapper of {@link ListState} which is easier to update based on window namespace. */
// WindowListState<W> 是 Flink Table API 运行时内部使用的类，它的主要作用是作为 Flink 底层 InternalListState 的一个封装器（Wrapper）
// 这个封装器的目的是使 Flink Table/SQL 窗口操作符（Window TVF Operators）能够更方便、更安全地管理基于窗口命名空间（Window Namespace）的列表状态。
// 简化操作： 它将底层的状态操作（如设置命名空间、获取、添加、清理）封装成简洁的方法，确保所有操作都自动与给定的窗口实例 (W) 关联
// 管理窗口输入： 在窗口操作符中，这个状态通常用于存储属于某个特定窗口的所有原始输入记录（RowData），直到窗口被触发计算为止。
public final class WindowListState<W> implements WindowState<W> {
    // 底层列表状态对象。 这是 Flink 运行时提供的内部状态句柄。它是一个三参数泛型：
    // 1. Keyed State Key Type (RowData): 状态的键类型（由 Flink 内部管理）。
    // 2. Namespace Type (W): 窗口命名空间类型，用于区分不同窗口实例的状态。
    // 3. Value Type (RowData): 列表中存储的值的类型，这里是 Flink 内部行数据。
    private final InternalListState<RowData, W, RowData> windowState;

    public WindowListState(InternalListState<RowData, W, RowData> windowState) {
        this.windowState = windowState;
    }

    public void clear(W window) {
        windowState.setCurrentNamespace(window);
        windowState.clear();
    }

    public List<RowData> get(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.getInternal();
    }

    /**
     * Updates the operator state accessible by {@link #get(W)} by adding the given value to the
     * list of values. The next time {@link #get(W)} is called (for the same state partition) the
     * returned state will represent the updated list.
     *
     * <p>If null is passed in, the state value will remain unchanged.
     *
     * @param window The namespace for the state.
     * @param value The new value for the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    public void add(W window, RowData value) throws Exception {
        windowState.setCurrentNamespace(window);
        windowState.add(value);
    }
}
