/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** A base {@link DispatcherLeaderProcess}.它负责封装 Flink 中 Dispatcher 的生命周期管理逻辑，包括初始化、运行、关闭等操作。定义了 State 枚举（CREATED, RUNNING, STOPPED），用于表示 DispatcherLeaderProcess 的生命周期状态，管理 DispatcherGatewayService，DispatcherGatewayService 是 Dispatcher 的主要访问入口，负责与外部组件的交互 */
@Internal
public abstract class AbstractDispatcherLeaderProcess implements DispatcherLeaderProcess {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Object lock = new Object();

    private final UUID leaderSessionId;

    private final FatalErrorHandler fatalErrorHandler;
    //Dispatcher的Gateway
    private final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture;
    //Dispatcher的地址
    private final CompletableFuture<String> leaderAddressFuture;
    //终止的Future
    private final CompletableFuture<Void> terminationFuture;
    //
    private final CompletableFuture<ApplicationStatus> shutDownFuture;

    private State state;
    //持有的Gateway
    @Nullable private DispatcherGatewayService dispatcherService;

    AbstractDispatcherLeaderProcess(UUID leaderSessionId, FatalErrorHandler fatalErrorHandler) {
        this.leaderSessionId = leaderSessionId;
        this.fatalErrorHandler = fatalErrorHandler;

        this.dispatcherGatewayFuture = new CompletableFuture<>();
        this.leaderAddressFuture = dispatcherGatewayFuture.thenApply(RestfulGateway::getAddress);
        this.terminationFuture = new CompletableFuture<>();
        this.shutDownFuture = new CompletableFuture<>();

        this.state = State.CREATED;
    }

    @VisibleForTesting
    State getState() {
        synchronized (lock) {
            return state;
        }
    }

    @Override
    public final void start() {
        runIfStateIs(State.CREATED, this::startInternal);
    }

    private void startInternal() {
        log.info("Start {}.", getClass().getSimpleName());
        state = State.RUNNING;
        onStart();
    }

    @Override
    public final UUID getLeaderSessionId() {
        return leaderSessionId;
    }

    @Override
    public final CompletableFuture<DispatcherGateway> getDispatcherGateway() {
        return dispatcherGatewayFuture;
    }

    @Override
    public final CompletableFuture<String> getLeaderAddressFuture() {
        return leaderAddressFuture;
    }

    @Override
    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    protected final Optional<DispatcherGatewayService> getDispatcherService() {
        return Optional.ofNullable(dispatcherService);
    }

    @Override
    public final CompletableFuture<Void> closeAsync() {
        runIfStateIsNot(State.STOPPED, this::closeInternal);

        return terminationFuture;
    }

    private void closeInternal() {
        log.info("Stopping {}.", getClass().getSimpleName());

        state = State.STOPPED;

        final CompletableFuture<Void> dispatcherServiceTerminationFuture = closeDispatcherService();

        final CompletableFuture<Void> onCloseTerminationFuture =
                FutureUtils.composeAfterwards(dispatcherServiceTerminationFuture, this::onClose);

        FutureUtils.forward(onCloseTerminationFuture, this.terminationFuture);
    }

    private CompletableFuture<Void> closeDispatcherService() {
        if (dispatcherService != null) {
            return dispatcherService.closeAsync();
        } else {
            return FutureUtils.completedVoidFuture();
        }
    }

    protected abstract void onStart();

    protected CompletableFuture<Void> onClose() {
        return FutureUtils.completedVoidFuture();
    }

    final void completeDispatcherSetup(DispatcherGatewayService dispatcherService) {
        runIfStateIs(State.RUNNING, () -> completeDispatcherSetupInternal(dispatcherService));
    }

    private void completeDispatcherSetupInternal(
            DispatcherGatewayService createdDispatcherService) {
        Preconditions.checkState(
                dispatcherService == null, "The DispatcherGatewayService can only be set once.");
        dispatcherService = createdDispatcherService;
        dispatcherGatewayFuture.complete(createdDispatcherService.getGateway()); //启动完成后就可以获取Gateway了
        FutureUtils.forward(createdDispatcherService.getShutDownFuture(), shutDownFuture); //Gateway的停止Future关联到shutDownFuture
        handleUnexpectedDispatcherServiceTermination(createdDispatcherService);
    }

    private void handleUnexpectedDispatcherServiceTermination(
            DispatcherGatewayService createdDispatcherService) {
        createdDispatcherService
                .getTerminationFuture()
                .whenComplete(
                        (ignored, throwable) ->
                                runIfStateIs(
                                        State.RUNNING,
                                        () ->
                                                handleError(
                                                        new FlinkException(
                                                                "Unexpected termination of DispatcherService.",
                                                                throwable))));
    }

    final <V> Optional<V> supplyUnsynchronizedIfRunning(Supplier<V> supplier) {
        synchronized (lock) {
            if (state != State.RUNNING) {
                return Optional.empty();
            }
        }

        return Optional.of(supplier.get());
    }

    final <V> Optional<V> supplyIfRunning(Supplier<V> supplier) {
        synchronized (lock) {
            if (state != State.RUNNING) {
                return Optional.empty();
            }

            return Optional.of(supplier.get());
        }
    }

    final void runIfStateIs(State expectedState, Runnable action) {
        runIfState(expectedState::equals, action);
    }

    private void runIfStateIsNot(State notExpectedState, Runnable action) {
        runIfState(state -> !notExpectedState.equals(state), action);
    }
    //谓词判断，再运行
    private void runIfState(Predicate<State> actionPredicate, Runnable action) {
        synchronized (lock) {
            if (actionPredicate.test(state)) {
                action.run();
            }
        }
    }
    //处理运行状态时的错误
    final <T> Void onErrorIfRunning(T ignored, Throwable throwable) {
        synchronized (lock) {
            if (state != State.RUNNING) {
                return null;
            }
        }

        if (throwable != null) {
            handleError(throwable);
        }

        return null;
    }

    private void handleError(Throwable throwable) {
        closeAsync();
        fatalErrorHandler.onFatalError(throwable);
    }

    /** The state of the {@link DispatcherLeaderProcess}. */
    protected enum State {
        CREATED,
        RUNNING,
        STOPPED
    }

    // ------------------------------------------------------------
    // Internal classes
    // ------------------------------------------------------------

    /** Factory for {@link DispatcherGatewayService}. */
    public interface DispatcherGatewayServiceFactory {
        DispatcherGatewayService create(
                DispatcherId dispatcherId,
                Collection<JobGraph> recoveredJobs,
                Collection<JobResult> recoveredDirtyJobResults,
                JobGraphWriter jobGraphWriter,
                JobResultStore jobResultStore);
    }

    /** An accessor of the {@link DispatcherGateway}. */
    public interface DispatcherGatewayService extends AutoCloseableAsync {
        DispatcherGateway getGateway();

        CompletableFuture<Void> onRemovedJobGraph(JobID jobId);

        CompletableFuture<ApplicationStatus> getShutDownFuture();

        CompletableFuture<Void> getTerminationFuture();
    }
}
