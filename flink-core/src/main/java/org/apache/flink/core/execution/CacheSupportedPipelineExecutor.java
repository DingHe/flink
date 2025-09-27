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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The pipeline executor that support caching intermediate dataset. */
@Internal
public interface CacheSupportedPipelineExecutor extends PipelineExecutor {

    /**
     * Return a set of ids of the completed cluster dataset.
     *  已完成的数据集通常是指作业执行过程中生成的缓存数据集，这个方法的目的是列出所有已经完成的缓存数据集的 ID。在 Flink 中，某些作业的中间数据集可以被缓存，以便重用。这些缓存的数据集在执行作业时可以加速后续的计算
     * @param configuration the {@link Configuration} with the required parameters
     * @param userCodeClassloader the {@link ClassLoader} to deserialize usercode
     * @return A set of ids of the completely cached intermediate dataset.
     */
    CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds(
            final Configuration configuration, final ClassLoader userCodeClassloader)
            throws Exception;

    /**
     * Invalidate the cluster dataset with the given id.
     * 此方法使得指定的集群数据集失效，即它从缓存中被移除。如果之后需要使用该数据集，Flink 将需要重新计算该数据集，而不是直接使用缓存的结果
     * @param clusterDatasetId id of the cluster dataset to be invalidated.
     * @param configuration the {@link Configuration} with the required parameters
     * @param userCodeClassloader the {@link ClassLoader} to deserialize usercode
     * @return Future which will be completed when the cached dataset is invalidated.
     */
    CompletableFuture<Void> invalidateClusterDataset(
            AbstractID clusterDatasetId,
            final Configuration configuration,
            final ClassLoader userCodeClassloader)
            throws Exception;
}
