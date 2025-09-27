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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;

/**
 * It is an internal equivalent of {@link org.apache.flink.core.io.InputStatus} that provides
 * additional non public statuses.
 *
 * <p>An {@code InputStatus} indicates the availability of data from an asynchronous input. When
 * asking an asynchronous input to produce data, it returns this status to indicate how to proceed.
 */
@Internal
public enum DataInputStatus {

    /**
     * Indicator that more data is available and the input can be called immediately again to
     * produce more data.
     */
    MORE_AVAILABLE, //表示有更多数据可供立即获取

    /**
     * Indicator that no data is currently available, but more data will be available in the future
     * again.
     */
    NOTHING_AVAILABLE, //表示当前没有可用数据，但未来可能会有

    /** Indicator that all persisted data of the data exchange has been successfully restored. */
    END_OF_RECOVERY, //表示数据源已经成功恢复了持久化的状态，在从保存点恢复作业时，数据源会经历这个状态

    /** Indicator that the input was stopped because of stop-with-savepoint without drain. */
    STOPPED, //表示数据源已经停止，可能是由于用户手动停止或者系统故障导致

    /** Indicator that the input has reached the end of data. */
    END_OF_DATA, //表示数据源已经到达数据末尾，不会再有新的数据产生，适用于有界数据集

    /**
     * Indicator that the input has reached the end of data and control events. The input is about
     * to close.
     */
    END_OF_INPUT //表示数据源已经到达数据和控制事件的末尾，这是数据源的最终状态，表示数据输入已经完全结束
}
