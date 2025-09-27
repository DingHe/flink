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

package org.apache.flink.util;

import org.apache.flink.api.common.JobID;

import org.slf4j.MDC;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkArgument;
 //Mapped Diagnostic Context,MDC 是日志记录中的一个重要机制，它允许为每个线程绑定一些上下文信息（如 JobID），方便在日志中追踪和区分不同线程的执行情况
/** Utility class to manage common Flink attributes in {@link MDC} (only {@link JobID} ATM). */
public class MdcUtils {

    public static final String JOB_ID = "flink-job-id";

    /**
     * Replace MDC contents with the provided one and return a closeable object that can be used to
     * restore the original MDC.
     *  替换当前 MDC 中的内容，并返回一个可以关闭的对象，关闭时恢复原始的 MDC 内容。
     * @param context to put into MDC
     */
    public static MdcCloseable withContext(Map<String, String> context) {
        final Map<String, String> orig = MDC.getCopyOfContextMap();
        MDC.setContextMap(context);
        return () -> MDC.setContextMap(orig);
    }

    /** {@link AutoCloseable } that restores the {@link MDC} contents on close. */
    public interface MdcCloseable extends AutoCloseable {
        @Override
        void close();
    }

    /** 包装一个 Runnable，使得在执行任务前将 contextData 添加到 MDC 中，执行完毕后再移除
     * Wrap the given {@link Runnable} so that the given data is added to {@link MDC} before its
     * execution and removed afterward.
     */
    public static Runnable wrapRunnable(Map<String, String> contextData, Runnable command) {
        return () -> {
            try (MdcCloseable ctx = withContext(contextData)) {
                command.run();
            }
        };
    }

    /**  包装一个 Callable，使得在执行任务前将 contextData 添加到 MDC 中，执行完毕后再移除
     * Wrap the given {@link Callable} so that the given data is added to {@link MDC} before its
     * execution and removed afterward.
     */
    public static <T> Callable<T> wrapCallable(
            Map<String, String> contextData, Callable<T> command) {
        return () -> {
            try (MdcCloseable ctx = withContext(contextData)) {
                return command.call();
            }
        };
    }

    /** 将给定的 Executor 包装成一个可以在执行任务时自动设置 MDC 上下文的 Executor,这个方法为 Flink 中的作业执行提供了一个全局的 MDC 上下文，确保任务执行时所有的日志都包含正确的 JobID
     * Wrap the given {@link Executor} so that the given {@link JobID} is added before it executes
     * any submitted commands and removed afterward.
     */
    public static Executor scopeToJob(JobID jobID, Executor executor) {
        checkArgument(!(executor instanceof MdcAwareExecutor));
        return new MdcAwareExecutor<>(executor, asContextData(jobID));
    }

    /**
     * Wrap the given {@link ExecutorService} so that the given {@link JobID} is added before it
     * executes any submitted commands and removed afterward.
     */
    public static ExecutorService scopeToJob(JobID jobID, ExecutorService delegate) {
        checkArgument(!(delegate instanceof MdcAwareExecutorService));
        return new MdcAwareExecutorService<>(delegate, asContextData(jobID));
    }

    /**
     * Wrap the given {@link ScheduledExecutorService} so that the given {@link JobID} is added
     * before it executes any submitted commands and removed afterward.
     */
    public static ScheduledExecutorService scopeToJob(JobID jobID, ScheduledExecutorService ses) {
        checkArgument(!(ses instanceof MdcAwareScheduledExecutorService));
        return new MdcAwareScheduledExecutorService(ses, asContextData(jobID));
    }

    public static Map<String, String> asContextData(JobID jobID) {
        return Collections.singletonMap(JOB_ID, jobID.toHexString());
    }
}
