/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.Callable;

/**
 * Executes {@link Runnable}, {@link ThrowingRunnable}, or {@link Callable}. Intended to customize
 * execution in sub-types of {@link org.apache.flink.streaming.runtime.tasks.StreamTask StreamTask},
 * e.g. synchronization in {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask
 * SourceStreamTask}.
 */
// Flink 流处理任务（StreamTask）内部用于抽象和定制代码执行方式的关键组件，特别关注于并发控制和同步。
// 核心作用是装饰 (Decorate) 任务内部的逻辑执行。
// 它不直接执行业务逻辑，而是提供一个执行框架，允许任务的子类（如 SourceStreamTask）定制这些逻辑（Runnable 或 Callable）的执行上下文
//
@Internal
public interface StreamTaskActionExecutor {
    // 执行一个不返回结果但允许抛出 Exception 的操作。这是最常用的执行方法
    void run(RunnableWithException runnable) throws Exception;

    // 执行一个不返回结果，但可以抛出任何指定类型 E 的受检异常的操作
    <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E;

    // 执行一个返回结果 R 且可能抛出 Exception 的操作。用于需要计算结果的逻辑。
    <R> R call(Callable<R> callable) throws Exception;

    // 立即执行器（默认实现）
    // 用途： 当任务不需要任何特殊的执行环境（如同步锁）时，使用此默认实现。
    StreamTaskActionExecutor IMMEDIATE =
            new StreamTaskActionExecutor() {
                @Override
                public void run(RunnableWithException runnable) throws Exception {
                    runnable.run();
                }

                @Override
                public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable)
                        throws E {
                    runnable.run();
                }

                @Override
                public <R> R call(Callable<R> callable) throws Exception {
                    return callable.call();
                }
            };
   // 创建带默认锁的同步执行器
    /** Returns an ExecutionDecorator that synchronizes each invocation. */
    static SynchronizedStreamTaskActionExecutor synchronizedExecutor() {
        return synchronizedExecutor(new Object());
    }
    // 创建带指定锁的同步执行器
    /** Returns an ExecutionDecorator that synchronizes each invocation on a given object. */
    static SynchronizedStreamTaskActionExecutor synchronizedExecutor(Object mutex) {
        return new SynchronizedStreamTaskActionExecutor(mutex);
    }

    /**
     * A {@link StreamTaskActionExecutor} that synchronizes every operation on the provided mutex.
     *
     * @deprecated this class should only be used in {@link SourceStreamTask} which exposes the
     *     checkpoint lock as part of Public API.
     */
    // 同步任务动作执行器。
    // 实现了 StreamTaskActionExecutor 接口，其核心功能是在执行任何操作前加锁
    @Deprecated
    class SynchronizedStreamTaskActionExecutor implements StreamTaskActionExecutor {
        private final Object mutex;

        public SynchronizedStreamTaskActionExecutor(Object mutex) {
            this.mutex = mutex;
        }

        @Override
        public void run(RunnableWithException runnable) throws Exception {
            synchronized (mutex) {
                runnable.run();
            }
        }

        @Override
        public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E {
            synchronized (mutex) {
                runnable.run();
            }
        }

        @Override
        public <R> R call(Callable<R> callable) throws Exception {
            synchronized (mutex) {
                return callable.call();
            }
        }

        /**
         * @return an object used for mutual exclusion of all operations that involve data and state
         *     mutation. (a.k.a. checkpoint lock).
         */
        public Object getMutex() {
            return mutex;
        }
    }
}
