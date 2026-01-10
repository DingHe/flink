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

/** Common interface for Flink JVM process memory components. */
// Flink 内存管理模型中的顶层接口，它定义了一个标准的 Flink JVM 进程内存规格。
// 规定了 Flink 在启动任何 JVM 进程时，必须计算清楚的六大关键内存指标。
// getTotalProcessMemorySize：蛋糕的总大小（配置给容器的内存）。
// getJvmMetaspaceSize & getJvmOverheadSize：切掉边缘部分，给盘子和装饰留空间（JVM 自身的开销）。
// getJvmHeapMemorySize：切出一块给 Java 对象（堆）。
// getJvmDirectMemorySize：切出一块给网络传输和堆外操作（直接内存）。
// getTotalFlinkMemorySize：剩下真正能用来干活（跑 Flink 任务）的蛋糕部分。

public interface ProcessMemorySpec extends Serializable {
    // 获取 JVM 堆内存 (Heap Memory) 的大小
    // 对应 JVM 参数：-Xmx 和 -Xms
    MemorySize getJvmHeapMemorySize();
    // 获取 JVM 直接内存 (Direct Memory / Off-heap) 的限制大小
    // 对应 JVM 参数：-XX:MaxDirectMemorySize
    // JVM 堆之外的内存，通常通过 ByteBuffer.allocateDirect() 分配
    // 包含内容：
    // 网络缓存 (Network Buffers)：Flink 使用 Netty 进行网络通信，这部分内存主要用于数据传输。
    // 框架堆外内存 (Framework Off-heap)：Flink 框架自身使用的非堆内存。
    MemorySize getJvmDirectMemorySize();
    // 获取 JVM 元空间 (Metaspace) 的大小。
    // 对应 JVM 参数：-XX:MaxMetaspaceSize
    // 存储类的元数据（Class Metadata）、类加载器（Class Loaders）、静态变量、常量池等。
    MemorySize getJvmMetaspaceSize();
    // 获取 JVM 执行开销 (JVM Overhead) 的大小
    // 这是一块“预留”给 JVM 自身运转和非 Java 代码使用的本地内存
    // 线程栈 (Thread Stacks)：每个线程（Xss）占用的空间。
    // 代码缓存 (Code Cache)：JIT 编译器生成的原生代码。
    // GC 结构：垃圾回收器维护的数据结构。
    MemorySize getJvmOverheadSize();
    // 获取 Flink 总内存 (Total Flink Memory) 的大小
    // 这部分内存是 Flink 可以通过 Slot 资源管理进行逻辑切分的部分
    // Total Flink Memory = Framework Heap/Off-heap + Task Heap/Off-heap + Network + Managed Memory。
    MemorySize getTotalFlinkMemorySize();
    // 获取 进程总内存 (Total Process Memory) 的大小
    // 对应配置项 jobmanager.memory.process.size 或 taskmanager.memory.process.size
    // 这是整个 Java 进程占用的物理内存上限。
    MemorySize getTotalProcessMemorySize();
}
