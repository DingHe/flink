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

package org.apache.flink.runtime.topology;

/**
 * Represents a logical or execution task. Each vertex can consume data from multiple {@link
 * Result}. Each vertex can produce multiple {@link Result}.
 */
public interface Vertex<
        VID extends VertexID,
        RID extends ResultID,
        V extends Vertex<VID, RID, V, R>, //自引用
        R extends Result<VID, RID, V, R>> {

    VID getId();
    //每个顶点可能从其他顶点的输出（Result）中消费数据，返回一个 Iterable 集合，包含当前顶点所依赖的所有结果。通过这些结果，当前顶点可以获得输入数据进行计算
    Iterable<? extends R> getConsumedResults();
    //获取当前顶点产生的数据结果
    Iterable<? extends R> getProducedResults();
}
