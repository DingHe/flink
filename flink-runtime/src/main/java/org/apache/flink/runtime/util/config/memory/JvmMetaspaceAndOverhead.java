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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.MemorySize;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** JVM metaspace and overhead memory sizes. */
// 为了将 Flink 内存模型中两个不由 Flink 直接管理、但由 JVM 进程必须占用的内存区域——元空间 (Metaspace) 和 JVM 开销 (Overhead)——封装在一起，方便在计算和传递参数时作为一个整体来处理。
// 在 Flink 的内存计算逻辑中，总进程内存（Total Process Memory）被分为两大块：
// Flink 能够管控的内存（Total Flink Memory）：包括 Heap、Direct Memory、Managed Memory 等。
// Flink 无法直接管控的本地内存：即本类所封装的 Metaspace 和 Overhead。

public class JvmMetaspaceAndOverhead implements Serializable {
    private static final long serialVersionUID = 1L;
    // 存储 JVM 元空间 的大小
    // 对应配置项 taskmanager.memory.jvm-metaspace.size（默认 256mb）。这部分内存用于存储 Java 类的元数据（Class Metadata）。
    private final MemorySize metaspace;
    // 存储 JVM 执行开销 的大小
    // 对应配置项 taskmanager.memory.jvm-overhead.min/max/fraction。这部分内存用于线程栈、代码缓存、垃圾收集器开销等。
    private final MemorySize overhead;

    public JvmMetaspaceAndOverhead(MemorySize jvmMetaspace, MemorySize jvmOverhead) {
        this.metaspace = checkNotNull(jvmMetaspace);
        this.overhead = checkNotNull(jvmOverhead);
    }

    MemorySize getTotalJvmMetaspaceAndOverheadSize() {
        return getMetaspace().add(getOverhead());
    }

    public MemorySize getMetaspace() {
        return metaspace;
    }

    public MemorySize getOverhead() {
        return overhead;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof JvmMetaspaceAndOverhead) {
            JvmMetaspaceAndOverhead that = (JvmMetaspaceAndOverhead) obj;
            return Objects.equals(this.metaspace, that.metaspace)
                    && Objects.equals(this.overhead, that.overhead);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metaspace, overhead);
    }
}
