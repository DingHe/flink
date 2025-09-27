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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;
//管理 JobMaster 的执行。它的主要职责是协调作业的生命周期，包括启动、状态请求、取消、以及异常处理
/** Interface for a runner which executes a {@link JobMaster}. */
public interface JobManagerRunner extends AutoCloseableAsync {

    /**
     * Start the execution of the {@link JobMaster}.
     * 启动 JobMaster 的执行
     * @throws Exception if the JobMaster cannot be started
     */
    void start() throws Exception;

    /**
     * Get the {@link JobMasterGateway} of the {@link JobMaster}. The future is only completed if
     * the JobMaster becomes leader.
     * 获取 JobMasterGateway，它是用于与 JobMaster 进行通信的接口
     * @return Future with the JobMasterGateway once the underlying JobMaster becomes leader
     */
    CompletableFuture<JobMasterGateway> getJobMasterGateway();

    /**
     * Get the result future of this runner. The future is completed once the executed job reaches a
     * globally terminal state or if the initialization of the {@link JobMaster} fails. If the
     * result future is completed exceptionally via {@link JobNotFinishedException}, then this
     * signals that the job has not been completed successfully. All other exceptional completions
     * denote an unexpected exception which leads to a process restart.
     * 获取作业的结果状态  1、作业达到全局终止状态（globally terminal state），如成功完成或被取消  2、JobMaster 初始化失败
     * @return Future which is completed with the job result
     */
    CompletableFuture<JobManagerRunnerResult> getResultFuture();

    /**
     * Get the job id of the executed job.
     *
     * @return job id of the executed job
     */
    JobID getJobID();

    /**
     * Cancels the currently executed job.
     * 取消当前正在执行的作业
     * @param timeout of this operation
     * @return Future acknowledge of the operation
     */
    CompletableFuture<Acknowledge> cancel(Time timeout);

    /**
     * Requests the current job status.
     * 获取当前作业的状态 CREATED：作业被创建但尚未提交
     * @param timeout for the rpc call
     * @return Future containing the current job status
     */
    CompletableFuture<JobStatus> requestJobStatus(Time timeout);

    /**
     * Request the details of the executed job.
     * 获取作业的详细信息
     * @param timeout for the rpc call
     * @return Future details of the executed job
     */
    CompletableFuture<JobDetails> requestJobDetails(Time timeout);

    /**
     * Requests the {@link ExecutionGraphInfo} of the executed job.
     *  获取作业的执行图
     * @param timeout for the rpc call
     * @return Future which is completed with the {@link ExecutionGraphInfo} of the executed job
     */
    CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout);

    /**
     * Flag indicating if the JobManagerRunner has been initialized.
     * 标志 JobManagerRunner 是否已经初始化
     * @return true if the JobManagerRunner has been initialized.
     */
    boolean isInitialized();
}
