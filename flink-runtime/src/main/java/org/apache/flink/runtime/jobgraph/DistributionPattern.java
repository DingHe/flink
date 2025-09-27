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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.executiongraph.EdgeManagerBuildUtil;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;

/**
 * A distribution pattern determines, which sub tasks of a producing task are connected to which
 * consuming sub tasks.
 *
 * <p>It affects how {@link ExecutionVertex} and {@link IntermediateResultPartition} are connected
 * in {@link EdgeManagerBuildUtil}
 */
public enum DistributionPattern {
    //这种模式表示生产者任务的每个输出分区都会被传递给所有消费者任务，在并行度相同的情况下，所有下游消费者都将接收生产者的所有数据分区
    /** Each producing sub task is connected to each sub task of the consuming task. */
    ALL_TO_ALL,
  //数据会按照一定的规则进行分发。通常，生产者的每个输出分区只会被发送到与之对应的消费者分区，这种模式实现的是数据一对一的传递
    /** Each producing sub task is connected to one or more subtask(s) of the consuming task. */
    POINTWISE
}
