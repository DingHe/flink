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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 * 代表一个数据源（Source）算子。
 * 它的主要作用是从外部系统（如文件、Kafka、Socket 等）中读取数据，并作为数据流的第一个元素进入整个 Flink 作业
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {
    //用于指示该数据源是否可以并行执行
    private boolean isParallel;
    // 用于创建旧版（Legacy）数据源的构造函数。
    // 它接收执行环境、输出类型信息、StreamSource 算子、并行性标志和名称。
    // 在内部，它创建了一个 LegacySourceTransformation 来表示这个旧版数据源操作
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName) {
        this(
                environment,
                outTypeInfo,
                operator,
                isParallel,
                sourceName,
                Boundedness.CONTINUOUS_UNBOUNDED);
    }

    /** The constructor used to create legacy sources. */
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName,
            Boundedness boundedness) {
        super(
                environment,
                new LegacySourceTransformation<>(
                        sourceName,
                        operator,
                        outTypeInfo,
                        environment.getParallelism(),
                        boundedness,
                        false));

        this.isParallel = isParallel;
        if (!isParallel) {
            setParallelism(1);
        }
    }

    /** 用于创建**“深层”数据源**。
     * 它接收一个已经配置好的 SingleOutputStreamOperator，通常用于一些手动配置的复杂数据源，这些数据源可能由多个操作符组成
     * Constructor for "deep" sources that manually set up (one or more) custom configured complex
     * operators.
     */
    public DataStreamSource(SingleOutputStreamOperator<T> operator) {
        super(operator.environment, operator.getTransformation());
        this.isParallel = true;
    }

    /** Constructor for new Sources (FLIP-27). */
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            Source<T, ?, ?> source,
            WatermarkStrategy<T> watermarkStrategy,
            TypeInformation<T> outTypeInfo,
            String sourceName) {
        super(
                environment,
                new SourceTransformation<>(
                        sourceName,
                        source,
                        watermarkStrategy,
                        outTypeInfo,
                        environment.getParallelism(),
                        false));
        this.isParallel = true;
    }

    @VisibleForTesting
    boolean isParallel() {
        return isParallel;
    }

    @Override
    public DataStreamSource<T> setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, isParallel);
        super.setParallelism(parallelism);
        return this;
    }
}
