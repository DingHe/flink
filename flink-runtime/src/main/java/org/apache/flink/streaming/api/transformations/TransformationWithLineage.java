/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.lineage.LineageVertex;

/**
 * A {@link Transformation} that contains lineage information.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}
 * @see Transformation
 */
// 的核心作用是为 Flink 的逻辑/物理操作（Transformation）附加血缘信息（Lineage Information）
// 血缘数据携带： 引入了 lineageVertex 属性，用于存储与数据血缘跟踪相关联的元数据。
// 支持血缘追踪： 在 Flink 的某些高级或内部特性中，需要知道数据的来源、转换路径和去向，即数据血缘。这个类确保了图中的每个关键操作符都能携带这些血缘信息。
@Internal
public abstract class TransformationWithLineage<T> extends PhysicalTransformation<T> {
    // 血缘顶点。
    // 存储了与此 Transformation 对应的血缘跟踪元数据。
    // LineageVertex 是一个专门用于描述数据血缘图中节点的类，包含了识别此操作符在血缘关系中的唯一信息。
    private LineageVertex lineageVertex;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    TransformationWithLineage(String name, TypeInformation<T> outputType, int parallelism) {
        super(name, outputType, parallelism);
    }

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     * @param parallelismConfigured If true, the parallelism of the transformation is explicitly set
     *     and should be respected. Otherwise the parallelism can be changed at runtime.
     */
    TransformationWithLineage(
            String name,
            TypeInformation<T> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        super(name, outputType, parallelism, parallelismConfigured);
    }

    /** Returns the lineage vertex of this {@code Transformation}. */
    public LineageVertex getLineageVertex() {
        return lineageVertex;
    }

    /** Change the lineage vertex of this {@code Transformation}. */
    public void setLineageVertex(LineageVertex lineageVertex) {
        this.lineageVertex = lineageVertex;
    }
}
