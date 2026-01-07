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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.util.stream.Stream;

/**
 * An interface to be implemented by the entity responsible for finding the correct {@link
 * PipelineExecutor} to execute a given {@link org.apache.flink.api.dag.Pipeline}.
 */
// 核心作用是定位和加载负责执行 Flink 数据流图（Pipeline/DAG）的正确执行器（PipelineExecutor）
// Flink 体系中，一个应用程序的逻辑流图（如 StreamGraph 或 JobGraph）可以由不同的运行时环境（例如：MiniCluster 上的本地执行、Standalone 集群、Yarn/K8s 上的远程执行、或者不同的部署模式）来执行。
// 这个接口就是为了在运行时根据用户的配置和环境，找到最合适的那个执行器工厂 (PipelineExecutorFactory)。
// 核心职责：
//服务发现： 负责发现所有在 classpath 中注册的 PipelineExecutorFactory 实现。
//兼容性判断： 根据提供的 Flink 配置 (Configuration)，判断哪个工厂能够创建兼容并执行该作业的执行器。
//工厂提供者： 返回兼容的执行器工厂，以便客户端（如 StreamExecutionEnvironment）能够创建实际的执行器 (PipelineExecutor) 来提交和管理作业。
@Internal
public interface PipelineExecutorServiceLoader {

    /**
     * Loads the {@link PipelineExecutorFactory} which is compatible with the provided
     * configuration. There can be at most one compatible factory among the available ones,
     * otherwise an exception will be thrown.
     *
     * @return a compatible {@link PipelineExecutorFactory}.
     * @throws Exception if there is more than one compatible factories, or something went wrong
     *     when loading the registered factories.
     */
    // 获取兼容的执行器工厂。
    // 这是该接口的核心方法。
    // 它接收作业的运行时配置 (Configuration)，并在所有已注册的 PipelineExecutorFactory 中进行迭代，找到唯一一个声明与当前配置兼容的工厂。
    PipelineExecutorFactory getExecutorFactory(final Configuration configuration) throws Exception;

    /** Loads and returns a stream of the names of all available executors. */
    // 获取所有可用执行器名称。
    Stream<String> getExecutorNames();
}
