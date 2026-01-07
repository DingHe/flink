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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

/**
 * Emit a record to the downstream.
 *
 * @param <E> the type of the record emitted by the {@link SplitReader}
 * @param <T> the type of records that are eventually emitted to the {@link SourceOutput}.
 * @param <SplitStateT> the mutable type of split state.
 */
// 在 Flink 的新版 Source 架构中，数据读取被解耦为两个步骤：
// 读取 (Fetch)：SplitReader 负责从外部系统（如 MySQL Binlog、Kafka）拉取原始数据块。
// 发送 (Emit)：RecordEmitter 负责将拉取到的原始数据“发射”到 Flink 的下游。
// 它的核心职责包括：
// 类型转换 (Transformation)：将 SplitReader 读取到的中间格式（如 SourceRecords）转换为 Flink 框架需要的最终格式（如 RowData）。
// 状态更新 (State Tracking)：在发送数据的同时，更新该分片（Split）的偏移量（Offset）或进度。这是实现 Exactly-once（精确一次） 语义的关键，因为它确保了“发送数据”和“更新位点”在同一个执行周期内完成。
// <E> 由 SplitReader 读取到的中间元素类型。在 MySQL CDC 中，这通常是 SourceRecords（包含了一批从 Debezium 拿到的原始记录）。
// <T> 最终发送到 Flink 下游算子的数据类型。
// <SplitStateT> 分片状态的可变类型。对于 MySQL 来说，这可能是当前读取到的 Binlog 位点信息。它用于记录当前读到哪了，以便在 Checkpoint 时保存进度。
@PublicEvolving
public interface RecordEmitter<E, T, SplitStateT> {

    /**
     * Process and emit the records to the {@link SourceOutput}. A few recommendations to the
     * implementation are following:
     *
     * <ul>
     *   <li>The method maybe interrupted in the middle. In that case, the same set of records will
     *       be passed to the record emitter again later. The implementation needs to make sure it
     *       reades
     *   <li>
     * </ul>
     *
     * @param element The intermediate element read by the SplitReader.
     * @param output The output to which the final records are emit to.
     * @param splitState The state of the split.
     */
    void emitRecord(E element, SourceOutput<T> output, SplitStateT splitState) throws Exception;
}
