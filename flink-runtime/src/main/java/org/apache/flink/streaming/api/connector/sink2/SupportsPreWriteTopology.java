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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Allows expert users to implement a custom topology before {@link SinkWriter}. */
// SupportsPreWriteTopology 是 Flink Sink v2 体系中提供的一个扩展接口（Extension Interface），
//用于 在真正进入 SinkWriter 写入数据之前，允许 Sink 自己插入一段自定义的 DataStream 拓扑。
// 允许 Sink 在写数据之前，主动改造、增强、重排（repartition）输入数据流的执行拓扑。
// InputT 表示：写入 Sink 的元素类型，与 SinkWriter<InputT> 的泛型一致
@Experimental
public interface SupportsPreWriteTopology<InputT> {

    /**
     * Adds an arbitrary topology before the writer. The topology may be used to repartition the
     * data.
     *
     * @param inputDataStream the stream of input records.
     * @return the custom topology before {@link SinkWriter}.
     */
    // 在 SinkWriter 之前插入一段用户自定义的 DataStream 拓扑
    // inputDataStream  ： Flink 为 Sink 准备好的 原始输入流
    // 返回的新 DataStream ： 作为 真正输入到 SinkWriter 的流
    // addPreWriteTopology 不是在运行时调用的，而是在：
    // Job 提交阶段
    // 场景 1：按 Key 重分区写入外部系统
    // public DataStream<MyEvent> addPreWriteTopology(
    //        DataStream<MyEvent> input) {
    //
    //    return input
    //        .keyBy(MyEvent::getUserId)
    //        .rebalance();
    //}
    // 保证同一个用户的数据进入同一个 SinkWriter
    // 场景 2：写前路由到不同分区
    // ublic DataStream<Record> addPreWriteTopology(
    //        DataStream<Record> input) {
    //
    //    return input.partitionCustom(
    //        new ShardPartitioner(),
    //        Record::getShardKey
    //    );
    //}外部系统是多 shard / bucket 架构（如 Iceberg / Hudi / Kafka-like）
    DataStream<InputT> addPreWriteTopology(DataStream<InputT> inputDataStream);
}
