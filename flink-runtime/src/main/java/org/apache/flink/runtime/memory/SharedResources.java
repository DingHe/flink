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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.function.LongFunctionWithException;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

import static org.apache.flink.util.Preconditions.checkState;

/** A map that keeps track of acquired shared resources and handles their allocation disposal. */
// 在 Flink 的内存管理体系中，SharedResources 类扮演着“共享资源管家”的角色。
// 它主要用于管理那些可以被多个对象（Leased Holders）共同使用的资源，典型的应用场景是 RocksDB 的共享内存消耗（如共享 Cache 或 Write Buffer Manager）
// SharedResources 的核心作用是实现 引用计数管理（Reference Counting）。
// 在 Flink 中，同一个 TaskManager 上的多个 Slot 可能运行着相同的作业任务，它们可以共享某些昂贵的资源（如内存池）
// 单例创建：确保同一种类型的资源只被初始化一次。
// 租约追踪：记录目前有多少个“租户”（leaseHolder）正在使用该资源。
// 自动释放：当最后一个租户离开（释放租约）时，自动调用资源的关闭方法（close()）以回收物理内存。
public final class SharedResources {
    // 互斥锁。
    // 保证在多线程环境下（多个任务同时申请或释放共享资源），对内部 Map 的操作是线程安全的。
    private final ReentrantLock lock = new ReentrantLock();
    // 资源注册表
    // Key 是资源类型的标识（String），Value 是被封装的受控资源（LeasedResource）。
    // 它记录了当前所有活跃的共享资源
    @GuardedBy("lock")
    private final HashMap<String, LeasedResource<?>> reservedResources = new HashMap<>();

    /**
     * Gets the shared memory resource for the given owner and registers a lease. If the resource
     * does not yet exist, it will be created via the given initializer function.
     *
     * <p>The resource must be released when no longer used. That releases the lease. When all
     * leases are released, the resource is disposed.
     */
    // 确保了跨任务的共享资源（如 RocksDB 的内存池）能够线程安全地初始化、按需共享、并正确计数
    public <T extends AutoCloseable> ResourceAndSize<T> getOrAllocateSharedResource(
            String type, // 资源的唯一标识符（例如 "RocksDB_Shared_Cache"）
            Object leaseHolder, // 租户对象（通常是申请内存的任务或算子实例），用于追踪是谁在使用资源。
            LongFunctionWithException<T, Exception> initializer, // 初始化函数。如果资源还没创建，将调用它来创建
            long sizeForInitialization) // 传给初始化函数的参数，通常是预留的内存字节数
            throws Exception {

        // We could be stuck on this lock for a while, in cases where another initialization is
        // currently
        // happening and the initialization is expensive.
        // We lock interruptibly here to allow for faster exit in case of cancellation errors.
        try {
            // 使用可中断锁
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MemoryAllocationException("Interrupted while acquiring memory");
        }

        try {
            // we cannot use "computeIfAbsent()" here because the computing function may throw an
            // exception.
            // 尝试从现有的资源注册表（reservedResources）中根据类型名称获取资源
            @SuppressWarnings("unchecked")
            LeasedResource<T> resource = (LeasedResource<T>) reservedResources.get(type);
            // 如果资源不存在，则调用 createResource
            if (resource == null) {
                resource = createResource(initializer, sizeForInitialization);
                reservedResources.put(type, resource);
            }
            // 将当前的 leaseHolder（租户）添加到该资源的 HashSet 中。
            resource.addLeaseHolder(leaseHolder);
            return resource;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Releases a lease (identified by the lease holder object) for the given type. If no further
     * leases exist, the resource is disposed.
     */
    void release(String type, Object leaseHolder) throws Exception {
        release(type, leaseHolder, (value) -> {});
    }

    /**
     * Releases a lease (identified by the lease holder object) for the given type. If no further
     * leases exist, the resource is disposed.
     *
     * <p>This method takes an additional hook that is called when the resource is disposed.
     */
    public void release(String type, Object leaseHolder, LongConsumer releaser) throws Exception {
        lock.lock();
        try {
            final LeasedResource<?> resource = reservedResources.get(type);
            if (resource == null) {
                return;
            }

            if (resource.removeLeaseHolder(leaseHolder)) {
                try {
                    reservedResources.remove(type);
                    resource.dispose();
                } finally {
                    releaser.accept(resource.size());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @VisibleForTesting
    int getNumResources() {
        return reservedResources.size();
    }

    private static <T extends AutoCloseable> LeasedResource<T> createResource(
            LongFunctionWithException<T, Exception> initializer, long size) throws Exception {

        final T resource = initializer.apply(size);
        return new LeasedResource<>(resource, size);
    }

    // ------------------------------------------------------------------------

    /** A resource handle with size. */
    public interface ResourceAndSize<T extends AutoCloseable> {

        T resourceHandle();

        long size();
    }

    // ------------------------------------------------------------------------

    private static final class LeasedResource<T extends AutoCloseable>
            implements ResourceAndSize<T> {
        // 存储所有持有该资源引用的对象。
        // 利用 HashSet 自动去重，确保同一个对象多次申请只算一个租约。
        private final HashSet<Object> leaseHolders = new HashSet<>();
        // 实际的物理资源对象（必须实现 AutoCloseable）
        private final T resourceHandle;

        private final long size;
        // 标记位，防止资源被重复关闭。
        private boolean disposed;

        private LeasedResource(T resourceHandle, long size) {
            this.resourceHandle = resourceHandle;
            this.size = size;
        }

        public T resourceHandle() {
            return resourceHandle;
        }

        public long size() {
            return size;
        }

        void addLeaseHolder(Object leaseHolder) {
            checkState(!disposed);
            leaseHolders.add(leaseHolder);
        }

        boolean removeLeaseHolder(Object leaseHolder) {
            checkState(!disposed);
            leaseHolders.remove(leaseHolder);
            return leaseHolders.isEmpty();
        }

        void dispose() throws Exception {
            if (!disposed) {
                disposed = true;
                resourceHandle.close();
            }
        }
    }
}
