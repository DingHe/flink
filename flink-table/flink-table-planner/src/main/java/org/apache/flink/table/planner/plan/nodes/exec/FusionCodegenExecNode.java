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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;

/** A {@link ExecNode} which support operator fusion codegen. */
// FusionCodegenExecNode 是一个非常高级且重要的接口。
// 它标志着 Flink SQL 引擎从**单算子代码生成（Operator-level Codegen）向全阶段代码生成（Whole-stage Codegen / Operator Fusion Codegen）**的演进
// 在传统的 Flink 代码生成中，每个算子（如 Filter, Project）都会生成自己的 Java 类。
// 虽然比解释执行快，但算子之间的数据传递仍然涉及虚函数调用和数据拷贝。
// FusionCodegenExecNode 的核心作用是实现“算子融合”：
// 消除函数调用开销：它允许将多个连续的算子（例如 Source -> Filter -> Project）融合进同一个 Java 函数中。数据通过局部变量直接传递，而不是通过 Collector 发送。
// 全阶段代码生成 (OFCG)：类似于 Spark SQL 的 Whole-stage Codegen，它通过将整个算子树的一部分“压扁”成一段紧凑的循环代码，极大地提高了 CPU 的利用率和缓存命中率。
// 指令优化：编译器（Janino）可以更好地优化融合后的单段代码，减少寄存器压力和流水线停顿。

public interface FusionCodegenExecNode {
    // 用于询问当前的 ExecNode 在特定条件下是否能够参与融合。
    /** Whether this ExecNode supports OFCG or not. */
    boolean supportFusionCodegen();

    /**
     * Translates this node into a {@link OpFusionCodegenSpecGenerator}.
     *
     * <p>NOTE: This method should return same spec generator result if called multiple times.
     *
     * @param planner The {@link Planner} of the translated graph.
     * @param parentCtx Parent CodeGeneratorContext.
     */
    // 融合逻辑的“核心翻译器”
    // CodeGeneratorContext parentCtx：父级代码生成上下文。
    // 这是融合过程的关键，它允许子节点和父节点共享常量池、成员变量和类定义，从而生成的代码能在一个类里协同工作。
    OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(
            Planner planner, CodeGeneratorContext parentCtx);
}
