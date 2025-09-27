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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherOperationCaches;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.rest.RestService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** 用于在同一个进程中启动和管理 Dispatcher（任务调度器）、ResourceManager（资源管理器）和 WebMonitorEndpoint（Web监控端点）
 * Component which starts a {@link Dispatcher}, {@link ResourceManager} and {@link
 * WebMonitorEndpoint} in the same process.
 */
public class DispatcherResourceManagerComponent implements AutoCloseableAsync {

    private static final Logger LOG =
            LoggerFactory.getLogger(DispatcherResourceManagerComponent.class);
    //负责启动并管理 Dispatcher 的生命周期，调度 Flink 的任务
    @Nonnull private final DispatcherRunner dispatcherRunner;
    //管理 Flink 集群的资源分配和容错能力
    @Nonnull private final ResourceManagerService resourceManagerService;
    //用于检索当前 Dispatcher 的领导者信息（在 HA 模式下尤为重要）
    @Nonnull private final LeaderRetrievalService dispatcherLeaderRetrievalService;

    @Nonnull private final LeaderRetrievalService resourceManagerRetrievalService;
    //提供 Web 界面和 REST API 端点，用于监控和管理 Flink 集群
    @Nonnull private final RestService webMonitorEndpoint;
    //标记组件的终止状态，所有子组件关闭后，该 future 会完成
    private final CompletableFuture<Void> terminationFuture;
    //标记应用程序关闭的状态，包括退出码等信息
    private final CompletableFuture<ApplicationStatus> shutDownFuture;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    private final FatalErrorHandler fatalErrorHandler;

    private final DispatcherOperationCaches dispatcherOperationCaches;

    DispatcherResourceManagerComponent(
            @Nonnull DispatcherRunner dispatcherRunner,
            @Nonnull ResourceManagerService resourceManagerService,
            @Nonnull LeaderRetrievalService dispatcherLeaderRetrievalService,
            @Nonnull LeaderRetrievalService resourceManagerRetrievalService,
            @Nonnull RestService webMonitorEndpoint,
            @Nonnull FatalErrorHandler fatalErrorHandler,
            @Nonnull DispatcherOperationCaches dispatcherOperationCaches) {
        this.dispatcherRunner = dispatcherRunner;
        this.resourceManagerService = resourceManagerService;
        this.dispatcherLeaderRetrievalService = dispatcherLeaderRetrievalService;
        this.resourceManagerRetrievalService = resourceManagerRetrievalService;
        this.webMonitorEndpoint = webMonitorEndpoint;
        this.fatalErrorHandler = fatalErrorHandler;
        this.terminationFuture = new CompletableFuture<>();
        this.shutDownFuture = new CompletableFuture<>();
        this.dispatcherOperationCaches = dispatcherOperationCaches;

        registerShutDownFuture();  //将dispatcherRunner的返回值关联到shutDownFuture属性上
        handleUnexpectedResourceManagerTermination(); //resourceManagerService终止后，如果服务还运行(isRunning is true），则抛出异常
    }

    private void handleUnexpectedResourceManagerTermination() {
        resourceManagerService
                .getTerminationFuture()
                .whenComplete(
                        (ignored, throwable) -> {
                            if (isRunning.get()) {
                                fatalErrorHandler.onFatalError(
                                        new FlinkException(
                                                "Unexpected termination of ResourceManagerService.",
                                                throwable));
                            }
                        });
    }

    private void registerShutDownFuture() {
        FutureUtils.forward(dispatcherRunner.getShutDownFuture(), shutDownFuture);
    }

    public final CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    /**
     * Deregister the Flink application from the resource management system by signalling the {@link
     * ResourceManager} and also stop the process.
     *
     * @param applicationStatus to terminate the application with
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    public CompletableFuture<Void> stopApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {
        return internalShutdown(
                () -> resourceManagerService.deregisterApplication(applicationStatus, diagnostics));
    }

    /**
     * Close the web monitor and cluster components. This method will not deregister the Flink
     * application from the resource management and only stop the process.
     *
     * @return Future which is completed once the shut down
     */
    public CompletableFuture<Void> stopProcess() {
        return internalShutdown(FutureUtils::completedVoidFuture);
    }

    private CompletableFuture<Void> internalShutdown(
            final Supplier<CompletableFuture<?>> additionalShutdownAction) {
        if (isRunning.compareAndSet(true, false)) {
            final CompletableFuture<Void> operationsConsumedFuture =
                    dispatcherOperationCaches.closeAsync(); //关闭所有缓存Future
            final CompletableFuture<Void> webMonitorShutdownFuture =
                    FutureUtils.composeAfterwards(  //确认operationsConsumedFuture关闭后，再关闭webMonitorEndpoint
                            operationsConsumedFuture, webMonitorEndpoint::closeAsync);
            final CompletableFuture<Void> closeWebMonitorAndAdditionalShutdownActionFuture =
                    FutureUtils.composeAfterwards( //确认webMonitorShutdownFuture关闭后，关闭传入的参数additionalShutdownAction
                            webMonitorShutdownFuture, additionalShutdownAction);

            return FutureUtils.composeAfterwards(
                    closeWebMonitorAndAdditionalShutdownActionFuture, this::closeAsyncInternal);
        } else {
            return terminationFuture;  //如果isRunning false，则直接返回
        }
    }

    private CompletableFuture<Void> closeAsyncInternal() {
        LOG.info("Closing components.");

        Exception exception = null;

        final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

        try {
            dispatcherLeaderRetrievalService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            resourceManagerRetrievalService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        terminationFutures.add(dispatcherRunner.closeAsync());

        terminationFutures.add(resourceManagerService.closeAsync());

        if (exception != null) {
            terminationFutures.add(FutureUtils.completedExceptionally(exception));
        }

        final CompletableFuture<Void> componentTerminationFuture =
                FutureUtils.completeAll(terminationFutures);

        componentTerminationFuture.whenComplete(
                (aVoid, throwable) -> {
                    if (throwable != null) {
                        terminationFuture.completeExceptionally(throwable);
                    } else {
                        terminationFuture.complete(aVoid);
                    }
                });

        return terminationFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return stopApplication(
                ApplicationStatus.CANCELED, "DispatcherResourceManagerComponent has been closed.");
    }

    public int getRestPort() {
        return webMonitorEndpoint.getRestPort();
    }
}
