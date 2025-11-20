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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/** Use cases of managed memory. */
// ManagedMemoryUseCase 类是 Flink 中用于定义和标识 托管内存（Managed Memory）的不同使用场景（Use Cases）的枚举类型。
// 托管内存是 Flink TaskManager 中由框架统一分配和管理的内存区域，主要用于高性能的数据结构、排序、哈希表等，以避免 Java 垃圾回收（GC）的开销。
// 分类内存使用者： 明确哪些 Flink 组件或功能会请求和使用托管内存。
// 定义管理范围： 关键在于为每个使用场景指定一个内存管理范围（Scope），即这部分托管内存应该在 Slot 级别 还是 Operator 级别 进行分配和管理。
// 通过这个枚举，Flink 可以根据不同的使用场景和范围，精确地计算、分配和隔离托管内存，确保不同组件之间不会过度竞争内存资源。
@Internal
public enum ManagedMemoryUseCase {

    /** Currently, weights are defined as mebibyte values. */
    // 代表算子内部使用的托管内存。例如，用于执行排序、哈希连接（Hash Join）或批处理操作中的数据结构的内存。
    OPERATOR(Scope.OPERATOR),
    // 代表状态后端使用的托管内存。例如，RocksDB State Backend 需要 Off-Heap 内存进行缓存和操作。
    STATE_BACKEND(Scope.SLOT),
    // 代表 Flink Python UDF 或 PyFlink 运行时使用的托管内存。
    PYTHON(Scope.SLOT);

    public final Scope scope;

    ManagedMemoryUseCase(Scope scope) {
        this.scope = Preconditions.checkNotNull(scope);
    }

    /** Scope at which memory is managed for a use case. */
    public enum Scope {
        SLOT,
        OPERATOR
    }
}
