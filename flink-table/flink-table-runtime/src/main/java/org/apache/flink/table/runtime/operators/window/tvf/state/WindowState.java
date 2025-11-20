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

/** A base interface for manipulate state with window namespace. */
// WindowState<W> 接口是一个基础接口，它定义了对 Flink 状态（State）进行操作的能力，同时将这些操作限定在特定的窗口命名空间 (W) 内
// 定义了窗口状态操作的通用接口。
// 泛型参数 <W> 代表窗口命名空间 (Window Namespace) 的类型。在 Flink 窗口操作符中，状态通常是键控（keyed）的，并且进一步按窗口划分。
// W 确保了状态的存取和清理是针对特定的窗口实例进行的。
public interface WindowState<W> {

    /** Removes the value mapped under current key and the given window. */
    // 该方法负责移除当前 Flink Keyed State 下，且属于给定 window 命名空间的所有状态值。
    void clear(W window);
}
