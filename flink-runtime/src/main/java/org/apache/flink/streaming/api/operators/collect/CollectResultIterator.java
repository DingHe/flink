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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * An iterator which iterates through the results of a query job.
 *
 * <p>The behavior of the iterator is slightly different under different checkpointing mode.
 *
 * <ul>
 *   <li>If the user does not specify any checkpointing, results are immediately delivered but
 *       exceptions will be thrown when the job restarts.
 *   <li>If the user specifies exactly-once checkpointing, results are guaranteed to be exactly-once
 *       but they're only visible after the corresponding checkpoint completes.
 *   <li>If the user specifies at-least-once checkpointing, results are immediately delivered but
 *       the same result may be delivered multiple times.
 * </ul>
 *
 * <p>NOTE: After using this iterator, the close method MUST be called in order to release job
 * related resources.
 */
// Flink 用户获取查询结果（例如 Flink SQL 的 SELECT 语句或 DataStream API 的 collect() 操作）时，在客户端侧使用的最终迭代器。
// 核心作用是将底层复杂的结果获取和容错机制（由 CollectResultFetcher 管理）封装成一个标准的、用户友好的 Java 迭代器 (Iterator) 接口。
// 迭代器接口实现： 提供了标准的 hasNext() 和 next() 方法，允许用户像遍历本地集合一样，逐条获取 Flink 集群中计算出的结果数据。
// 生命周期管理： 实现了 CloseableIterator，要求用户必须调用 close() 方法，以确保在数据消费完毕或提前退出时，底层的 Flink Job 资源能够被正确释放（通常是取消作业）
// 结果获取委托： 将实际的数据拉取工作委托给 CollectResultFetcher。
//
public class CollectResultIterator<T> implements CloseableIterator<T> {

    private final CollectResultFetcher<T> fetcher;
    private T bufferedResult;

    private CompletableFuture<OperatorID> operatorIdFuture;
    private TypeSerializer<T> serializer;
    private String accumulatorName;
    private CheckpointConfig checkpointConfig;
    private long resultFetchTimeout;

    public CollectResultIterator(
            CompletableFuture<OperatorID> operatorIdFuture,
            TypeSerializer<T> serializer,
            String accumulatorName,
            CheckpointConfig checkpointConfig,
            long resultFetchTimeout) {
        this.operatorIdFuture = operatorIdFuture;
        this.serializer = serializer;
        this.accumulatorName = accumulatorName;
        this.checkpointConfig = checkpointConfig;
        this.resultFetchTimeout = resultFetchTimeout;
        AbstractCollectResultBuffer<T> buffer = createBuffer(serializer, checkpointConfig);
        this.fetcher =
                new CollectResultFetcher<>(
                        buffer, operatorIdFuture, accumulatorName, resultFetchTimeout);
        this.bufferedResult = null;
    }

    @VisibleForTesting
    public CollectResultIterator(
            AbstractCollectResultBuffer<T> buffer,
            CompletableFuture<OperatorID> operatorIdFuture,
            String accumulatorName,
            int retryMillis) {
        this.fetcher =
                new CollectResultFetcher<>(
                        buffer,
                        operatorIdFuture,
                        accumulatorName,
                        retryMillis,
                        RpcOptions.ASK_TIMEOUT_DURATION.defaultValue().toMillis());
        this.bufferedResult = null;
    }

    @Override
    public boolean hasNext() {
        // we have to make sure that the next result exists
        // it is possible that there is no more result but the job is still running
        if (bufferedResult == null) {
            bufferedResult = nextResultFromFetcher();
        }
        return bufferedResult != null;
    }

    @Override
    public T next() {
        if (bufferedResult == null) {
            bufferedResult = nextResultFromFetcher();
        }
        T ret = bufferedResult;
        bufferedResult = null;
        return ret;
    }

    @Override
    public void close() throws Exception {
        fetcher.close();
    }

    public void setJobClient(JobClient jobClient) {
        fetcher.setJobClient(jobClient);
    }

    private T nextResultFromFetcher() {
        try {
            return fetcher.next();
        } catch (IOException e) {
            fetcher.close();
            throw new RuntimeException("Failed to fetch next result", e);
        }
    }

    private AbstractCollectResultBuffer<T> createBuffer(
            TypeSerializer<T> serializer, CheckpointConfig checkpointConfig) {
        if (checkpointConfig.isCheckpointingEnabled()) {
            if (checkpointConfig.getCheckpointingConsistencyMode()
                    == CheckpointingMode.EXACTLY_ONCE) {
                return new CheckpointedCollectResultBuffer<>(serializer);
            } else {
                return new UncheckpointedCollectResultBuffer<>(serializer, true);
            }
        } else {
            return new UncheckpointedCollectResultBuffer<>(serializer, false);
        }
    }

    public CollectResultIterator<T> copy() {
        return new CollectResultIterator<>(
                this.operatorIdFuture,
                this.serializer,
                this.accumulatorName,
                this.checkpointConfig,
                this.resultFetchTimeout);
    }
}
