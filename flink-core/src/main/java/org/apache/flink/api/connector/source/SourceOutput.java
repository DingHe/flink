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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * The {@code SourceOutput} is the gateway for a {@link SourceReader}) to emit the produced records
 * and watermarks.
 *
 * <p>A {@code SourceReader} may have multiple SourceOutputs, scoped to individual <i>Source
 * Splits</i>. That way, streams of events from different splits can be identified and treated
 * separately, for example for watermark generation, or event-time skew handling.
 */
//SourceReader 向 Flink 运行时发送数据记录和水印的“网关”
@Public
public interface SourceOutput<T> extends WatermarkOutput {

    /**
     * Emit a record without a timestamp.
     *
     * <p>Use this method if the source system does not have a notion of records with timestamps.
     *
     * <p>The events later pass through a {@link TimestampAssigner}, which attaches a timestamp to
     * the event based on the event's contents. For example a file source with JSON records would
     * not have a generic timestamp from the file reading and JSON parsing process, and thus use
     * this method to produce initially a record without a timestamp. The {@code TimestampAssigner}
     * in the next step would be used to extract timestamp from a field of the JSON object.
     *
     * @param record the record to emit.
     */
    // 发送一条不带时间戳的数据记录
    // 当数据源中的记录没有内嵌时间戳时使用。例如，从文件中读取的普通 JSON 记录。
    // 在这种情况下，时间戳的分配工作会留给下游的 TimestampAssigner，它会根据记录的内容（如某个字段）来提取或生成时间戳
    void collect(T record);

    /**
     * Emit a record with a timestamp.
     *
     * <p>Use this method if the source system has timestamps attached to records. Typical examples
     * would be Logs, PubSubs, or Message Queues, like Kafka or Kinesis, which store a timestamp
     * with each event.
     *
     * <p>The events typically still pass through a {@link TimestampAssigner}, which may decide to
     * either use this source-provided timestamp, or replace it with a timestamp stored within the
     * event (for example if the event was a JSON object one could configure aTimestampAssigner that
     * extracts one of the object's fields and uses that as a timestamp).
     *
     * @param record the record to emit.
     * @param timestamp the timestamp of the record.
     */
    // 发送一条带时间戳的数据记录
    // long timestamp：数据记录的时间戳
    // 当数据源本身（如 Kafka、Kinesis）就为每条记录附加了时间戳时使用。
    // 虽然这些记录通常仍会经过 TimestampAssigner，但 TimestampAssigner 可能会选择直接使用这个时间戳，而不是自己重新生成
    void collect(T record, long timestamp);
}
