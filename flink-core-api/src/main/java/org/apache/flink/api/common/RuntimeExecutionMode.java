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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Runtime execution mode of DataStream programs. Among other things, this controls task scheduling,
 * network shuffle behavior, and time semantics. Some operations will also change their record
 * emission behaviour based on the configured execution mode.
 *
 * @see <a
 *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-134%3A+Batch+execution+for+the+DataStream+API">
 *     https://cwiki.apache.org/confluence/display/FLINK/FLIP-134%3A+Batch+execution+for+the+DataStream+API</a>
 */
// 定义了 Flink DataStream API 程序在执行时可以采用的三种主要运行时模式。
// 旨在实现统一的流批处理，允许用户使用 DataStream API 编写的程序能够以高性能的批处理语义运行，而不仅仅是传统的流处理语义。
// 任务调度 (Task Scheduling): 决定任务是全部立即部署（流式）还是逐步部署（批式/阶段性）。
// 网络数据交换 (Network Shuffle): 决定任务间的数据传输是流式（边生产边消费）还是阻塞式（先生产完所有数据再消费）。
// 时间语义 (Time Semantics): 影响 Flink 如何处理水位线（Watermarks）和事件时间/处理时间。
@PublicEvolving
public enum RuntimeExecutionMode {

    /**
     * The Pipeline will be executed with Streaming Semantics. All tasks will be deployed before
     * execution starts, checkpoints will be enabled, and both processing and event time will be
     * fully supported.
     */
    // 流式执行模式。
    // 所有任务在作业启动前都会被部署 (All tasks will be deployed before execution starts)。
    // 检查点: 启用检查点 (checkpoints will be enabled)，用于容错和状态持久化。
    STREAMING,

    /**
     * The Pipeline will be executed with Batch Semantics. Tasks will be scheduled gradually based
     * on the scheduling region they belong, shuffles between regions will be blocking, watermarks
     * are assumed to be "perfect" i.e. no late data, and processing time is assumed to not advance
     * during execution.
     */
    // 批处理执行模式。
    // 调度: 任务是基于它们所属的调度区域（Scheduling Region）逐步调度的 (Tasks will be scheduled gradually)，通常是阶段性调度。
    // 网络: 区域间的数据交换是阻塞式的 (shuffles between regions will be blocking)，即上游任务完成后，下游任务才开始消费数据。
    // 时间: 假设水位线是“完美”的，即没有迟到数据 (no late data)，且在执行过程中处理时间不推进。
    BATCH,

    /**
     * Flink will set the execution mode to {@link RuntimeExecutionMode#BATCH} if all sources are
     * bounded, or {@link RuntimeExecutionMode#STREAMING} if there is at least one source which is
     * unbounded.
     */
    // 自动判断模式。
    //Flink 会根据作业的数据源类型自动选择最合适的执行模式。
    AUTOMATIC
}
