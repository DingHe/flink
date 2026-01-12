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

package org.apache.flink.runtime.memory;

import javax.annotation.Nonnegative;

import java.util.concurrent.atomic.AtomicLong;

/** Tracker of memory reservation and release within a custom limit. */
// UnsafeMemoryBudget 是一个非常底层且关键的组件。
// 它就像是一个“内存会计”，专门负责在一个设定的限额内，精确地记录和控制内存的预留（Reservation）与释放（Release）。
// UnsafeMemoryBudget 的主要作用是在内存管理层面实现“记账”功能。
// 它并不负责物理内存的分配（即它不会去调用 malloc 或 unsafe.allocateMemory），而是维护一个逻辑上的预算上限。当 Flink 想要通过 MemoryManager 申请托管内存（尤其是堆外内存）时，必须先通过这个类的许可。
// 其核心设计参考了 Java 内部 java.nio.Bits 的内存预留逻辑，但做了简化和适配，以支持 Flink 自定义的内存上限控制，防止进程由于申请超过预期的堆外内存而导致系统崩溃。


class UnsafeMemoryBudget {
    // 预算总额。在构造时确定，表示该预算器管理的内存上限（以字节为单位）。通常对应推导出的托管内存总量。
    private final long totalMemorySize;
    // 当前可用额度。使用原子类型保证多线程环境下（多个算子同时申请内存）记账的准确性和线程安全。
    private final AtomicLong availableMemorySize;

    UnsafeMemoryBudget(long totalMemorySize) {
        this.totalMemorySize = totalMemorySize;
        this.availableMemorySize = new AtomicLong(totalMemorySize);
    }

    long getTotalMemorySize() {
        return totalMemorySize;
    }
    // 获取当前还剩下多少字节可以被预留
    long getAvailableMemorySize() {
        return availableMemorySize.get();
    }

    boolean verifyEmpty() {
        try {
            reserveMemory(totalMemorySize);
        } catch (MemoryReservationException e) {
            return false;
        }
        releaseMemory(totalMemorySize);
        return availableMemorySize.get() == totalMemorySize;
    }

    /**
     * Reserve memory of certain size if it is available.
     *
     * <p>Adjusted version of {@link java.nio.Bits#reserveMemory(long, int)} taken from Java 11.
     */
    // 尝试锁定指定大小的额度
    // 调用 tryReserveMemory。如果成功（返回值 $\ge$ 请求大小），直接返回；
    // 如果余额不足，则抛出 MemoryReservationException。
    @SuppressWarnings({"OverlyComplexMethod", "JavadocReference", "NestedTryStatement"})
    void reserveMemory(long size) throws MemoryReservationException {
        long availableOrReserved = tryReserveMemory(size);
        // optimist!
        if (availableOrReserved >= size) {
            return;
        }
        // no luck
        throw new MemoryReservationException(
                String.format(
                        "Could not allocate %d bytes, only %d bytes are remaining. This usually indicates "
                                + "that you are requesting more memory than you have reserved. "
                                + "However, when running an old JVM version it can also be caused by slow garbage collection. "
                                + "Try to upgrade to Java 8u72 or higher if running on an old Java version.",
                        size, availableOrReserved));
    }
    // 底层的 CAS（Compare and Swap）自旋逻辑
    private long tryReserveMemory(long size) {
        long currentAvailableMemorySize;
        // 使用 while 循环检查当前 availableMemorySize 是否大于等于请求的 size
        while (size <= (currentAvailableMemorySize = availableMemorySize.get())) {
            if (availableMemorySize.compareAndSet(
                    currentAvailableMemorySize, currentAvailableMemorySize - size)) {
                return size;
            }
        }
        return currentAvailableMemorySize;
    }
    // 归还之前预留的额度，使之重新变为可用
    void releaseMemory(@Nonnegative long size) {
        if (size == 0) {
            return;
        }
        boolean released = false;
        long currentAvailableMemorySize = 0L;
        while (!released
                && totalMemorySize
                        >= (currentAvailableMemorySize = availableMemorySize.get()) + size) {
            released =
                    availableMemorySize.compareAndSet(
                            currentAvailableMemorySize, currentAvailableMemorySize + size);
        }
        if (!released) {
            throw new IllegalStateException(
                    String.format(
                            "Trying to release more managed memory (%d bytes) than has been allocated (%d bytes), the total size is %d bytes",
                            size, currentAvailableMemorySize, totalMemorySize));
        }
    }
}
