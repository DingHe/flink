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
import org.apache.flink.streaming.api.graph.StreamGraph;

/** The data exchange mode between operators during {@link StreamGraph} generation. */
@Internal
public enum StreamExchangeMode {
    /**
     * Producer and consumer are online at the same time. Produced data is received by consumer
     * immediately.
     */
    PIPELINED, //数据源源不断地产生数据，并立即发送给下游算子进行处理

    /**
     * The producer first produces its entire result and finishes. After that, the consumer is
     * started and may consume the data.
     */
    BATCH, //数据被分成批次进行处理

    /**
     * The consumer can start consuming data anytime as long as the producer has started producing.
     *
     * <p>This exchange mode is re-consumable.
     */
    HYBRID_FULL, //结合了管道式和批量处理的特点，下游算子可以多次消费相同的数据

    /**
     * The consumer can start consuming data anytime as long as the producer has started producing.
     *
     * <p>This exchange mode is not re-consumable.
     */
    HYBRID_SELECTIVE,//与HYBRID_FULL类似，但不能完全重消费数据，下游算子只能重消费部分数据

    /**
     * The exchange mode is undefined. It leaves it up to the framework to decide the exchange mode.
     * The framework will pick one of {@link StreamExchangeMode#BATCH} or {@link
     * StreamExchangeMode#PIPELINED} in the end.
     */
    UNDEFINED //由Flink框架根据具体情况决定交换模式
}
