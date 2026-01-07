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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The representation of an edge connecting two {@link ExecNode}s.
 *
 * <p>The edge's json serialization/deserialization will be delegated to {@code JsonPlanEdge}, which
 * only stores the {@link ExecNode}'s id instead of instance.
 *
 * <p>{@code JsonPlanEdge} should also be updated with this class if the fields are added/removed.
 */
// 在 Apache Flink Table Planner 的物理计划层中，ExecEdge 是一个至关重要的类。
// 它定义了执行图（ExecNodeGraph）中节点之间的连接，并描述了数据如何在这些节点之间流转。
// 定义拓扑连接：它通过持有 source（源节点）和 target（目标节点）的引用，构建了 ExecNode 之间的 DAG（有向无环图）结构。
// 描述数据分布（Shuffle）：它详细说明了数据从源节点到目标节点时的重分区策略（如 Hash 分区、广播等）。这决定了下游算子看到的并行数据流是什么样的。
// 定义交换模式（Exchange Mode）：它指明了数据传输的物理属性。例如，是流水线式（Pipelined）实时传输，还是批处理式的落盘传输。
// 作为持久化与翻译的桥梁：它是 SQL 已编译计划（Compiled Plan）的一部分，负责将逻辑上的连接翻译成 Flink DataStream 层的 Transformation。


public class ExecEdge {
    /** The source node of this edge. */
    // 这条边的起点节点，数据的生产者
    private final ExecNode<?> source;
    /** The target node of this edge. */
    // 这条边的终点节点， 数据的消费者
    private final ExecNode<?> target;
    /** The {@link Shuffle} on this edge from source to target. */
    // 定义数据如何分布到目标节点的子任务中
    // 比如 HashShuffle 决定了按哪些字段进行数据倾斜治理或 Join 关联
    private final Shuffle shuffle;
    /** The {@link StreamExchangeMode} defines the data exchange mode on this edge. */
    // 定义数据交换的物理行为
    // 目前主要支持 PIPELINED（上游产出立即发给下游）
    private final StreamExchangeMode exchangeMode;

    public ExecEdge(
            ExecNode<?> source,
            ExecNode<?> target,
            Shuffle shuffle,
            StreamExchangeMode exchangeMode) {
        this.source = checkNotNull(source);
        this.target = checkNotNull(target);
        this.shuffle = checkNotNull(shuffle);
        this.exchangeMode = checkNotNull(exchangeMode);

        // TODO once FLINK-21224 [Remove BatchExecExchange and StreamExecExchange, and replace their
        //  functionality with ExecEdge] is finished, we should remove the following validation.
        if (shuffle.getType() != Shuffle.Type.FORWARD) {
            throw new TableException("Only FORWARD shuffle is supported now.");
        }
        if (exchangeMode != StreamExchangeMode.PIPELINED) {
            throw new TableException("Only PIPELINED shuffle mode is supported now.");
        }
    }

    public ExecNode<?> getSource() {
        return source;
    }

    public ExecNode<?> getTarget() {
        return target;
    }

    public Shuffle getShuffle() {
        return shuffle;
    }

    public StreamExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    /** Returns the output {@link LogicalType} of the data passing this edge. */
    public LogicalType getOutputType() {
        return source.getOutputType();
    }

    @Override
    public String toString() {
        return "ExecEdge{"
                + "source="
                + source.getDescription()
                + ", target="
                + target.getDescription()
                + ", shuffle="
                + shuffle
                + ", exchangeMode="
                + exchangeMode
                + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of the {@link ExecEdge}. */
    public static class Builder {
        private ExecNode<?> source;
        private ExecNode<?> target;
        private Shuffle shuffle = FORWARD_SHUFFLE;
        private StreamExchangeMode exchangeMode = StreamExchangeMode.PIPELINED;

        public Builder from(ExecEdge original) {
            this.source = original.source;
            this.target = original.target;
            this.shuffle = original.shuffle;
            this.exchangeMode = original.exchangeMode;
            return this;
        }

        public Builder source(ExecNode<?> source) {
            this.source = source;
            return this;
        }

        public Builder target(ExecNode<?> target) {
            this.target = target;
            return this;
        }

        public Builder shuffle(Shuffle shuffle) {
            this.shuffle = shuffle;
            return this;
        }

        public Builder requiredDistribution(
                InputProperty.RequiredDistribution requiredDistribution) {
            return shuffle(fromRequiredDistribution(requiredDistribution));
        }

        public Builder exchangeMode(StreamExchangeMode exchangeMode) {
            this.exchangeMode = exchangeMode;
            return this;
        }

        public ExecEdge build() {
            return new ExecEdge(source, target, shuffle, exchangeMode);
        }

        private Shuffle fromRequiredDistribution(
                InputProperty.RequiredDistribution requiredDistribution) {
            switch (requiredDistribution.getType()) {
                case ANY:
                    return ANY_SHUFFLE;
                case SINGLETON:
                    return SINGLETON_SHUFFLE;
                case BROADCAST:
                    return BROADCAST_SHUFFLE;
                case HASH:
                    InputProperty.HashDistribution hashDistribution =
                            (InputProperty.HashDistribution) requiredDistribution;
                    return hashShuffle(hashDistribution.getKeys());
                default:
                    throw new TableException(
                            "Unsupported RequiredDistribution type: "
                                    + requiredDistribution.getType());
            }
        }
    }

    /** The {@link Shuffle} defines how to exchange the records between {@link ExecNode}s. */
    public abstract static class Shuffle {
        private final Type type;

        protected Shuffle(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }

        @Override
        public String toString() {
            return type.name();
        }

        /** Enumeration which describes the shuffle type for records when passing this edge. */
        public enum Type {
            /** Any type of shuffle is OK when passing through this edge. */
            // 任意分发策略均可
            ANY,

            /** Records are shuffled by hash when passing through this edge. */
            // 按 Key 进行哈希取模分发
            HASH,

            /** Full records are provided for each parallelism of the target node. */
            // 每一行都复制到目标节点的所有并行任务中。
            BROADCAST,

            /** Records are shuffled to one node, the parallelism of the target node must be 1. */
            // 将所有数据发送到下游唯一的并行任务中（并行度为1）
            SINGLETON,

            /** Records are shuffled in same parallelism (the shuffle behavior is function call). */
            // 1对1发送，要求并行度一致，性能最高。
            FORWARD
        }
    }

    /** Records are shuffled by hash when passing through this edge. */
    public static class HashShuffle extends Shuffle {
        private final int[] keys;

        public HashShuffle(int[] keys) {
            super(Type.HASH);
            this.keys = checkNotNull(keys);
            checkArgument(keys.length > 0, "Hash keys must no be empty.");
        }

        public int[] getKeys() {
            return keys;
        }

        @Override
        public String toString() {
            return "HASH" + Arrays.toString(keys);
        }
    }

    /** Any type of shuffle is OK when passing through this edge. */
    public static final Shuffle ANY_SHUFFLE = new Shuffle(Shuffle.Type.ANY) {};

    /** Full records are provided for each parallelism of the target node. */
    public static final Shuffle BROADCAST_SHUFFLE = new Shuffle(Shuffle.Type.BROADCAST) {};

    /** The parallelism of the target node must be 1. */
    public static final Shuffle SINGLETON_SHUFFLE = new Shuffle(Shuffle.Type.SINGLETON) {};

    /** Records are shuffled in same parallelism (function call). */
    public static final Shuffle FORWARD_SHUFFLE = new Shuffle(Shuffle.Type.FORWARD) {};

    /**
     * Return hash {@link Shuffle}.
     *
     * @param keys hash keys
     */
    public static Shuffle hashShuffle(int[] keys) {
        return new HashShuffle(keys);
    }

    /**
     * Translates this edge into a Flink operator.
     *
     * @param planner The {@link Planner} of the translated Table.
     */
    public Transformation<?> translateToPlan(Planner planner) {
        return source.translateToPlan(planner);
    }

    /**
     * Translates this edge into operator fusion codegen spec generator.
     *
     * @param planner The {@link Planner} of the translated Table.
     * @param parentCtx Parent CodeGeneratorContext.
     */
    public OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(
            Planner planner, CodeGeneratorContext parentCtx) {
        return source.translateToFusionCodegenSpec(planner, parentCtx);
    }
}
