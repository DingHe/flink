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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines the chaining scheme for the operator. When an operator is chained to the predecessor, it
 * means that they run in the same thread. They become one operator consisting of multiple steps.
 *
 * <p>The default value used by the StreamOperator is {@link #HEAD}, which means that the operator
 * is not chained to its predecessor. Most operators override this with {@link #ALWAYS}, meaning
 * they will be chained to predecessors whenever possible.
 */
@PublicEvolving
public enum ChainingStrategy {

    /**
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal chaining and
     * increase operator parallelism.
     */
    ALWAYS, //操作符会尽可能地被连接到前一个操作符，即 总是链式连接

    /** The operator will not be chained to the preceding or succeeding operators. */
    NEVER,  //操作符 不会与前一个或后续操作符进行链式连接，即 不进行操作符链连接

    /**
     * The operator will not be chained to the predecessor, but successors may chain to this
     * operator.
     */
    HEAD,  //当设置为 HEAD 时，操作符 不会与前一个操作符链在一起，但是它允许后续操作符与其链在一起

    /**
     * This operator will run at the head of a chain (similar as in {@link #HEAD}, but it will
     * additionally try to chain source inputs if possible. This allows multi-input operators to be
     * chained with multiple sources into one task.
     */
    HEAD_WITH_SOURCES;  //但是它 会尽可能尝试将多输入的操作符链到多个源（source）中。也就是说，除了将后续操作符链到头部操作符之后外，它还会尝试将输入源（例如 Source 操作符）与头部操作符进行链式连接

    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;
}
