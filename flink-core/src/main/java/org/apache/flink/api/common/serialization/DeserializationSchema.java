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

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain data
 * sources (for example Apache Kafka) into data types (Java/Scala objects) that are processed by
 * Flink.
 *
 * <p>In addition, the DeserializationSchema describes the produced type ({@link
 * #getProducedType()}), which lets Flink create internal serializers and structures to handle the
 * type.
 *
 * <p><b>Note:</b> In most cases, one should start from {@link AbstractDeserializationSchema}, which
 * takes care of producing the return type information automatically.
 *
 * <p>A DeserializationSchema must be {@link Serializable} because its instances are often part of
 * an operator or transformation function.
 *
 * @param <T> The type created by the deserialization schema.
 */
// Flink 中一个核心接口，其作用是定义如何将外部数据源（如 Kafka、文件、Socket 等）的字节消息反序列化为 Flink 内部可以处理的 Java/Scala 对象。
// 它充当了 Flink 外部输入数据和内部处理逻辑之间的桥梁
@Public
public interface DeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    //初始化方法
    //在反序列化工作开始前调用，可用于执行一次性设置，如注册用户指标或初始化连接等
    @PublicEvolving
    default void open(InitializationContext context) throws Exception {}

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    //反序列化单个字节消息
    //接收一个字节数组 message，并返回一个反序列化后的 T 类型对象。如果消息无法反序列化，可以返回 null
    T deserialize(byte[] message) throws IOException;

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param message The message, as a byte array.
     * @param out The collector to put the resulting messages.
     */
    // 反序列化单个字节消息，并允许输出零个或多个结果
    // 其默认实现是调用 deserialize(byte[] message)，然后将结果通过 Collector 发出。
    // 它允许一个字节消息产生多个结果，这在某些场景下非常有用，例如当一条消息包含多个记录时
    @PublicEvolving
    default void deserialize(byte[] message, Collector<T> out) throws IOException {
        T deserialize = deserialize(message);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }

    /**
     * Method to decide whether the element signals the end of the stream. If true is returned the
     * element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    //判断一个反序列化后的对象是否是流的结束信号
    boolean isEndOfStream(T nextElement);

    /**
     * A contextual information provided for {@link #open(InitializationContext)} method. It can be
     * used to:
     *
     * <ul>
     *   <li>Register user metrics via {@link InitializationContext#getMetricGroup()}
     *   <li>Access the user code class loader.
     * </ul>
     */
    @PublicEvolving
    interface InitializationContext {
        /**
         * Returns the metric group for the parallel subtask of the source that runs this {@link
         * DeserializationSchema}.
         *
         * <p>Instances of this class can be used to register new metrics with Flink and to create a
         * nested hierarchy based on the group names. See {@link MetricGroup} for more information
         * for the metrics system.
         *
         * @see MetricGroup
         */
        //获取一个指标组，允许用户在此方法中注册自定义指标，以监控反序列化过程
        MetricGroup getMetricGroup();

        /**
         * Gets the {@link UserCodeClassLoader} to load classes that are not in system's classpath,
         * but are part of the jar file of a user job.
         *
         * @see UserCodeClassLoader
         */
        //获取用户代码的类加载器，这对于动态加载类或资源非常有用
        UserCodeClassLoader getUserCodeClassLoader();
    }
}
