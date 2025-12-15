/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import java.io.IOException;
import java.io.Serializable;

/** Handle to state that can be read back again via {@link #retrieveState()}. */
// RetrievableStateHandle<T> 接口是 Flink 状态后端体系中用于引用和读取状态数据的核心抽象。
// 主要作用是封装状态数据的位置信息和获取逻辑，使得 Flink JobMaster (CheckpointCoordinator) 在调度状态恢复时，只需要传输这个轻量级的句柄，而不需要传输实际庞大的状态数据。
// 引用可恢复对象： 它是一个指向某个已持久化或存储的状态对象的引用。
// 提供读取能力： 它定义了 retrieveState() 方法，允许 TaskManager 根据句柄中包含的位置信息，将引用的状态对象 (T) 读回到内存中。
// 继承状态对象特性： 它继承了 StateObject 接口，因此它本身也是一个状态对象，具备报告大小 (getStateSize()) 和清理 (discardState()) 的能力。
// 常用于存储算子协调器状态 (Operator Coordinator State) 或用户自定义的 Master State 等需要在 JobMaster 上集中处理的轻量级状态
// 泛型 T 被引用的状态对象的类型。 T 代表通过这个句柄可以最终恢复的 Java 对象。 由于状态需要在网络上传输或持久化，因此 T 必须实现 Serializable 接口。

public interface RetrievableStateHandle<T extends Serializable> extends StateObject {

    /** Retrieves the object that was previously written to state. */
    // 检索状态对象。
    // 这是该接口的核心方法，用于从底层存储中读取该句柄所引用的实际状态对象。
    T retrieveState() throws IOException, ClassNotFoundException;
}
