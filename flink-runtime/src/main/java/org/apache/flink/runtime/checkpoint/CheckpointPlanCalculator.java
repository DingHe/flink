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

package org.apache.flink.runtime.checkpoint;

import java.util.concurrent.CompletableFuture;

/**
 * Calculates the plan of the next checkpoint, including the tasks to trigger, wait or commit for
 * each checkpoint.
 */
// 在 Flink 的检查点触发流程中，JobManager 需要确定哪些任务需要参与到本次检查点中（哪些任务处于运行状态，哪些任务已完成）。
// 由于计算这个计划可能涉及到遍历执行图和检查任务状态，这个过程被设计为异步操作，以避免阻塞主调度线程。
public interface CheckpointPlanCalculator {

    /**
     * Calculates the plan of the next checkpoint.
     *
     * @return The result plan.
     */
    // 计算检查点计划。
    CompletableFuture<CheckpointPlan> calculateCheckpointPlan();
}
