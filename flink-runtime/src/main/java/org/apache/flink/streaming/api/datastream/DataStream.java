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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A DataStream represents a stream of elements of the same type. A DataStream can be transformed
 * into another DataStream by applying a transformation as for example:
 *
 * <ul>
 *   <li>{@link DataStream#map}
 *   <li>{@link DataStream#filter}
 * </ul>
 *
 * @param <T> The type of the elements in this stream.
 */
// 表示数据流的核心类。它是一个不可变的、代表特定类型元素流的编程对象。
// 在Flink中，您无法直接操作DataStream中的数据，而是通过调用它的各种方法来创建新的DataStream，从而构建一个数据处理的**转换（Transformation）**链
@Public
public class DataStream<T> {
    //执行环境，定义了作业的全局配置（如默认并行度、重启策略等），并负责最终执行数据流图
    protected final StreamExecutionEnvironment environment;
    //引用当前DataStream所代表的转换。在Flink内部，DataStream上的每个操作（如map、filter）都会被翻译成一个Transformation对象
    protected final Transformation<T> transformation;

    /**
     * Create a new {@link DataStream} in the given execution environment with partitioning set to
     * forward by default.
     *
     * @param environment The StreamExecutionEnvironment
     */
    public DataStream(StreamExecutionEnvironment environment, Transformation<T> transformation) {
        this.environment =
                Preconditions.checkNotNull(environment, "Execution Environment must not be null.");
        this.transformation =
                Preconditions.checkNotNull(
                        transformation, "Stream Transformation must not be null.");
    }

    /**
     * Returns the ID of the {@link DataStream} in the current {@link StreamExecutionEnvironment}.
     * 获取此DataStream在当前StreamExecutionEnvironment中的唯一ID
     * @return ID of the DataStream
     */
    @Internal
    public int getId() {
        return transformation.getId();
    }

    /**
     * Gets the parallelism for this operator.
     * 获取此操作（DataStream）的并行度。并行度决定了多少个并发实例会执行此操作
     * @return The parallelism set for this operator.
     */
    public int getParallelism() {
        return transformation.getParallelism();
    }

    /**
     * Gets the minimum resources for this operator.
     * 获取此操作所需的最小资源规格（如CPU核数、内存）
     * @return The minimum resources set for this operator.
     */
    @PublicEvolving
    public ResourceSpec getMinResources() {
        return transformation.getMinResources();
    }

    /**
     * Gets the preferred resources for this operator.
     * 获取此操作所偏好的资源规格。Flink调度器在资源充足时会优先使用此配置
     * @return The preferred resources set for this operator.
     */
    @PublicEvolving
    public ResourceSpec getPreferredResources() {
        return transformation.getPreferredResources();
    }

    /**
     * Gets the type of the stream.
     * 获取此DataStream中元素的类型信息。TypeInformation对于Flink的序列化、反序列化以及类型推断非常重要
     * @return The type of the datastream.
     */
    public TypeInformation<T> getType() {
        return transformation.getOutputType();
    }

    /**
     * Invokes the {@link org.apache.flink.api.java.ClosureCleaner} on the given function if closure
     * cleaning is enabled in the {@link ExecutionConfig}.
     * 对用户提供的函数（如MapFunction）进行闭包清理
     * @return The cleaned Function
     */
    protected <F> F clean(F f) {
        return getExecutionEnvironment().clean(f);
    }

    /**
     * Returns the {@link StreamExecutionEnvironment} that was used to create this {@link
     * DataStream}.
     * 返回创建此DataStream的执行环境
     * @return The Execution Environment
     */
    public StreamExecutionEnvironment getExecutionEnvironment() {
        return environment;
    }

    //返回当前执行环境的执行配置
    public ExecutionConfig getExecutionConfig() {
        return environment.getConfig();
    }

    /**
     * Creates a new {@link DataStream} by merging {@link DataStream} outputs of the same type with
     * each other. The DataStreams merged using this operator will be transformed simultaneously.
     * 将当前DataStream与一个或多个同类型DataStream合并成一个单一的DataStream。所有合并的流中的元素都会被发送到后续操作中，并且它们的元素类型必须相同
     * @param streams The DataStreams to union output with.
     * @return The {@link DataStream}.
     */
    @SafeVarargs
    public final DataStream<T> union(DataStream<T>... streams) {
        List<Transformation<T>> unionedTransforms = new ArrayList<>();
        unionedTransforms.add(this.transformation);

        for (DataStream<T> newStream : streams) {
            if (!getType().equals(newStream.getType())) {
                throw new IllegalArgumentException(
                        "Cannot union streams of different types: "
                                + getType()
                                + " and "
                                + newStream.getType());
            }

            unionedTransforms.add(newStream.getTransformation());
        }
        return new DataStream<>(this.environment, new UnionTransformation<>(unionedTransforms));
    }

    /**
     * Creates a new {@link ConnectedStreams} by connecting {@link DataStream} outputs of (possible)
     * different types with each other. The DataStreams connected using this operator can be used
     * with CoFunctions to apply joint transformations.
     * 将当前DataStream与另一个可能不同类型的DataStream连接起来，创建一个ConnectedStreams。
     * 这允许您在两个流上使用 CoFunction 来进行协同处理，但两个流中的元素会分别处理
     * @param dataStream The DataStream with which this stream will be connected.
     * @return The {@link ConnectedStreams}.
     */
    public <R> ConnectedStreams<T, R> connect(DataStream<R> dataStream) {
        return new ConnectedStreams<>(environment, this, dataStream);
    }

    /**
     * Creates a new {@link BroadcastConnectedStream} by connecting the current {@link DataStream}
     * or {@link KeyedStream} with a {@link BroadcastStream}.
     *
     * <p>The latter can be created using the {@link #broadcast(MapStateDescriptor[])} method.
     *
     * <p>The resulting stream can be further processed using the {@code
     * BroadcastConnectedStream.process(MyFunction)} method, where {@code MyFunction} can be either
     * a {@link org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
     * KeyedBroadcastProcessFunction} or a {@link
     * org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
     * BroadcastProcessFunction} depending on the current stream being a {@link KeyedStream} or not.
     * 将当前DataStream与一个广播流（BroadcastStream）连接起来。
     * 这使得主流的每个元素都可以访问广播流中的所有状态数据，常用于配置数据或规则的动态更新
     * @param broadcastStream The broadcast stream with the broadcast state to be connected with
     *     this stream.
     * @return The {@link BroadcastConnectedStream}.
     */
    @PublicEvolving
    public <R> BroadcastConnectedStream<T, R> connect(BroadcastStream<R> broadcastStream) {
        return new BroadcastConnectedStream<>(
                environment,
                this,
                Preconditions.checkNotNull(broadcastStream),
                broadcastStream.getBroadcastStateDescriptors());
    }

    /**
     * It creates a new {@link KeyedStream} that uses the provided key for partitioning its operator
     * states.
     *  根据提供的 KeySelector 抽取键值，将数据流分区为KeyedStream。
     *  同一键值的所有元素都会被发送到同一个并行任务中。这是执行有状态操作（如聚合、窗口计算）的前提
     * @param key The KeySelector to be used for extracting the key for partitioning
     * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
     */
    public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key) {
        Preconditions.checkNotNull(key);
        return new KeyedStream<>(this, clean(key));
    }

    /**
     * It creates a new {@link KeyedStream} that uses the provided key with explicit type
     * information for partitioning its operator states.
     *
     * 与上一个keyBy类似，但允许用户显式指定键的类型，
     * 这在类型推断失败或不准确时非常有用
     *
     * @param key The KeySelector to be used for extracting the key for partitioning.
     * @param keyType The type information describing the key type.
     * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
     */
    public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key, TypeInformation<K> keyType) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(keyType);
        return new KeyedStream<>(this, clean(key), keyType);
    }

    /**
     * Partitions the operator state of a {@link DataStream} by the given key positions.
     * 已弃用。根据元组（Tuple）或数组中指定位置的字段进行分组
     * @deprecated Use {@link DataStream#keyBy(KeySelector)}.
     * @param fields The position of the fields on which the {@link DataStream} will be grouped.
     * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
     */
    @Deprecated
    public KeyedStream<T, Tuple> keyBy(int... fields) {
        if (getType() instanceof BasicArrayTypeInfo
                || getType() instanceof PrimitiveArrayTypeInfo) {
            return keyBy(KeySelectorUtil.getSelectorForArray(fields, getType()));
        } else {
            return keyBy(new Keys.ExpressionKeys<>(fields, getType()));
        }
    }

    /**
     * Partitions the operator state of a {@link DataStream} using field expressions. A field
     * expression is either the name of a public field or a getter method with parentheses of the
     * {@link DataStream}'s underlying type. A dot can be used to drill down into objects, as in
     * {@code "field1.getInnerField2()" }.
     *
     * 已弃用。根据POJO类型中指定字段名称进行分组
     *
     * @deprecated Use {@link DataStream#keyBy(KeySelector)}.
     * @param fields One or more field expressions on which the state of the {@link DataStream}
     *     operators will be partitioned.
     * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
     */
    @Deprecated
    public KeyedStream<T, Tuple> keyBy(String... fields) {
        return keyBy(new Keys.ExpressionKeys<>(fields, getType()));
    }

    private KeyedStream<T, Tuple> keyBy(Keys<T> keys) {
        return new KeyedStream<>(
                this,
                clean(KeySelectorUtil.getSelectorForKeys(keys, getType(), getExecutionConfig())));
    }

    /**
     * Partitions a tuple DataStream on the specified key fields using a custom partitioner. This
     * method takes the key position to partition on, and a partitioner that accepts the key type.
     *
     * <p>Note: This method works only on single field keys.
     * 使用自定义分区器和键选择器来控制元素的分区方式。这允许用户完全自定义数据流的物理分布
     * @deprecated use {@link DataStream#partitionCustom(Partitioner, KeySelector)}.
     * @param partitioner The partitioner to assign partitions to keys.
     * @param field The field index on which the DataStream is partitioned.
     * @return The partitioned DataStream.
     */
    @Deprecated
    public <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, int field) {
        Keys.ExpressionKeys<T> outExpressionKeys =
                new Keys.ExpressionKeys<>(new int[] {field}, getType());
        return partitionCustom(partitioner, outExpressionKeys);
    }

    /**
     * Partitions a POJO DataStream on the specified key fields using a custom partitioner. This
     * method takes the key expression to partition on, and a partitioner that accepts the key type.
     *
     * <p>Note: This method works only on single field keys.
     *
     * @deprecated use {@link DataStream#partitionCustom(Partitioner, KeySelector)}.
     * @param partitioner The partitioner to assign partitions to keys.
     * @param field The expression for the field on which the DataStream is partitioned.
     * @return The partitioned DataStream.
     */
    @Deprecated
    public <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, String field) {
        Keys.ExpressionKeys<T> outExpressionKeys =
                new Keys.ExpressionKeys<>(new String[] {field}, getType());
        return partitionCustom(partitioner, outExpressionKeys);
    }

    /**
     * Partitions a DataStream on the key returned by the selector, using a custom partitioner. This
     * method takes the key selector to get the key to partition on, and a partitioner that accepts
     * the key type.
     *
     * <p>Note: This method works only on single field keys, i.e. the selector cannot return tuples
     * of fields.
     *
     * @param partitioner The partitioner to assign partitions to keys.
     * @param keySelector The KeySelector with which the DataStream is partitioned.
     * @return The partitioned DataStream.
     * @see KeySelector
     */
    public <K> DataStream<T> partitionCustom(
            Partitioner<K> partitioner, KeySelector<T, K> keySelector) {
        return setConnectionType(
                new CustomPartitionerWrapper<>(clean(partitioner), clean(keySelector)));
    }

    //	private helper method for custom partitioning
    private <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, Keys<T> keys) {
        KeySelector<T, K> keySelector =
                KeySelectorUtil.getSelectorForOneKey(
                        keys, partitioner, getType(), getExecutionConfig());

        return setConnectionType(
                new CustomPartitionerWrapper<>(clean(partitioner), clean(keySelector)));
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output elements are broadcasted
     * to every parallel instance of the next operation.
     * 将数据流中的每个元素都发送到下游算子的所有并行实例中，实现一对多的广播模式
     * @return The DataStream with broadcast partitioning set.
     */
    public DataStream<T> broadcast() {
        return setConnectionType(new BroadcastPartitioner<T>());
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output elements are broadcasted
     * to every parallel instance of the next operation. In addition, it implicitly as many {@link
     * org.apache.flink.api.common.state.BroadcastState broadcast states} as the specified
     * descriptors which can be used to store the element of the stream.
     *
     * 将数据流广播，并创建一个带有广播状态的BroadcastStream。这用于在连接时，向主流提供广播状态
     *
     * @param broadcastStateDescriptors the descriptors of the broadcast states to create.
     * @return A {@link BroadcastStream} which can be used in the {@link #connect(BroadcastStream)}
     *     to create a {@link BroadcastConnectedStream} for further processing of the elements.
     */
    @PublicEvolving
    public BroadcastStream<T> broadcast(
            final MapStateDescriptor<?, ?>... broadcastStateDescriptors) {
        Preconditions.checkNotNull(broadcastStateDescriptors);
        final DataStream<T> broadcastStream = setConnectionType(new BroadcastPartitioner<>());
        return new BroadcastStream<>(environment, broadcastStream, broadcastStateDescriptors);
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output elements are shuffled
     * uniformly randomly to the next operation.
     * 以随机、均匀的方式将数据流元素分发到下游算子的所有并行实例
     * @return The DataStream with shuffle partitioning set.
     */
    @PublicEvolving
    public DataStream<T> shuffle() {
        return setConnectionType(new ShufflePartitioner<T>());
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output elements are forwarded to
     * the local subtask of the next operation.
     * 将数据流元素发送到下游算子的本地（或同一槽位）实例，如果下游算子与上游算子具有相同的并行度，则不发生网络传输
     * @return The DataStream with forward partitioning set.
     */
    public DataStream<T> forward() {
        return setConnectionType(new ForwardPartitioner<T>());
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output elements are distributed
     * evenly to instances of the next operation in a round-robin fashion.
     *  以循环（round-robin）的方式将数据流元素均匀地分发到下游算子的所有并行实例，以平衡负载
     * @return The DataStream with rebalance partitioning set.
     */
    public DataStream<T> rebalance() {
        return setConnectionType(new RebalancePartitioner<T>());
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output elements are distributed
     * evenly to a subset of instances of the next operation in a round-robin fashion.
     *
     * <p>The subset of downstream operations to which the upstream operation sends elements depends
     * on the degree of parallelism of both the upstream and downstream operation. For example, if
     * the upstream operation has parallelism 2 and the downstream operation has parallelism 4, then
     * one upstream operation would distribute elements to two downstream operations while the other
     * upstream operation would distribute to the other two downstream operations. If, on the other
     * hand, the downstream operation has parallelism 2 while the upstream operation has parallelism
     * 4 then two upstream operations will distribute to one downstream operation while the other
     * two upstream operations will distribute to the other downstream operations.
     *
     * <p>In cases where the different parallelisms are not multiples of each other one or several
     * downstream operations will have a differing number of inputs from upstream operations.
     *
     * 将数据流元素以循环的方式分发到下游算子的一个子集，相比rebalance可以减少网络连接数量，适用于并行度呈倍数关系的情况
     *
     * @return The DataStream with rescale partitioning set.
     */
    @PublicEvolving
    public DataStream<T> rescale() {
        return setConnectionType(new RescalePartitioner<T>());
    }

    /**
     * Sets the partitioning of the {@link DataStream} so that the output values all go to the first
     * instance of the next processing operator. Use this setting with care since it might cause a
     * serious performance bottleneck in the application.
     *  将数据流中的所有元素都发送到下游算子的第一个并行实例。这可能导致严重的性能瓶颈
     * @return The DataStream with shuffle partitioning set.
     */
    @PublicEvolving
    public DataStream<T> global() {
        return setConnectionType(new GlobalPartitioner<T>());
    }

    /**
     * Initiates an iterative part of the program that feeds back data streams. The iterative part
     * needs to be closed by calling {@link IterativeStream#closeWith(DataStream)}. The
     * transformation of this IterativeStream will be the iteration head. The data stream given to
     * the {@link IterativeStream#closeWith(DataStream)} method is the data stream that will be fed
     * back and used as the input for the iteration head. The user can also use different feedback
     * type than the input of the iteration and treat the input and feedback streams as a {@link
     * ConnectedStreams} be calling {@link IterativeStream#withFeedbackType(TypeInformation)}
     *
     * <p>A common usage pattern for streaming iterations is to use output splitting to send a part
     * of the closing data stream to the head. Refer to {@link
     * ProcessFunction.Context#output(OutputTag, Object)} for more information.
     *
     * <p>The iteration edge will be partitioned the same way as the first input of the iteration
     * head unless it is changed in the {@link IterativeStream#closeWith(DataStream)} call.
     *
     * <p>By default a DataStream with iteration will never terminate, but the user can use the
     * maxWaitTime parameter to set a max waiting time for the iteration head. If no data received
     * in the set time, the stream terminates.
     *
     * @return The iterative data stream created.
     * @deprecated This method is deprecated since Flink 1.19. The only known use case of this
     *     Iteration API comes from Flink ML, which already has its own implementation of iteration
     *     and no longer uses this API. If there's any use cases other than Flink ML that needs
     *     iteration support, please reach out to dev@flink.apache.org and we can consider making
     *     the Flink ML iteration implementation a separate common library.
     * @see <a
     *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-357%3A+Deprecate+Iteration+API+of+DataStream">
     *     FLIP-357: Deprecate Iteration API of DataStream </a>
     * @see <a href="https://nightlies.apache.org/flink/flink-ml-docs-stable/">Flink ML </a>
     */
    //已弃用。启动一个流式迭代，允许将数据流的一部分结果反馈回迭代的头部
    @Deprecated
    public IterativeStream<T> iterate() {
        return new IterativeStream<>(this, 0);
    }

    /**
     * Initiates an iterative part of the program that feeds back data streams. The iterative part
     * needs to be closed by calling {@link IterativeStream#closeWith(DataStream)}. The
     * transformation of this IterativeStream will be the iteration head. The data stream given to
     * the {@link IterativeStream#closeWith(DataStream)} method is the data stream that will be fed
     * back and used as the input for the iteration head. The user can also use different feedback
     * type than the input of the iteration and treat the input and feedback streams as a {@link
     * ConnectedStreams} be calling {@link IterativeStream#withFeedbackType(TypeInformation)}
     *
     * <p>A common usage pattern for streaming iterations is to use output splitting to send a part
     * of the closing data stream to the head. Refer to {@link
     * ProcessFunction.Context#output(OutputTag, Object)} for more information.
     *
     * <p>The iteration edge will be partitioned the same way as the first input of the iteration
     * head unless it is changed in the {@link IterativeStream#closeWith(DataStream)} call.
     *
     * <p>By default a DataStream with iteration will never terminate, but the user can use the
     * maxWaitTime parameter to set a max waiting time for the iteration head. If no data received
     * in the set time, the stream terminates.
     *
     * @param maxWaitTimeMillis Number of milliseconds to wait between inputs before shutting down
     * @return The iterative data stream created.
     * @deprecated This method is deprecated since Flink 1.19. The only known use case of this
     *     Iteration API comes from Flink ML, which already has its own implementation of iteration
     *     and no longer uses this API. If there's any use cases other than Flink ML that needs
     *     iteration support, please reach out to dev@flink.apache.org and we can consider making
     *     the Flink ML iteration implementation a separate common library.
     * @see <a
     *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-357%3A+Deprecate+Iteration+API+of+DataStream">
     *     FLIP-357: Deprecate Iteration API of DataStream </a>
     * @see <a href="https://nightlies.apache.org/flink/flink-ml-docs-stable/">Flink ML </a>
     */
    //已弃用。与上一个方法类似，但允许设置最大等待时间，超时后迭代将终止
    @Deprecated
    public IterativeStream<T> iterate(long maxWaitTimeMillis) {
        return new IterativeStream<>(this, maxWaitTimeMillis);
    }

    /**
     * Applies a Map transformation on a {@link DataStream}. The transformation calls a {@link
     * MapFunction} for each element of the DataStream. Each MapFunction call returns exactly one
     * element. The user can also extend {@link RichMapFunction} to gain access to other features
     * provided by the {@link org.apache.flink.api.common.functions.RichFunction} interface.
     *
     * 应用 Map 转换。
     * 对流中每个元素调用MapFunction，并返回一个新元素。输入和输出元素是一对一的关系
     *
     * @param mapper The MapFunction that is called for each element of the DataStream.
     * @param <R> output type
     * @return The transformed {@link DataStream}.
     */
    public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {

        TypeInformation<R> outType =
                TypeExtractor.getMapReturnTypes(
                        clean(mapper), getType(), Utils.getCallLocationName(), true);

        return map(mapper, outType);
    }

    /**
     * Applies a Map transformation on a {@link DataStream}. The transformation calls a {@link
     * MapFunction} for each element of the DataStream. Each MapFunction call returns exactly one
     * element. The user can also extend {@link RichMapFunction} to gain access to other features
     * provided by the {@link org.apache.flink.api.common.functions.RichFunction} interface.
     *
     * 与上一个方法类似，
     * 但允许指定输出类型，避免类型推断问题
     *
     * @param mapper The MapFunction that is called for each element of the DataStream.
     * @param outputType {@link TypeInformation} for the result type of the function.
     * @param <R> output type
     * @return The transformed {@link DataStream}.
     */
    public <R> SingleOutputStreamOperator<R> map(
            MapFunction<T, R> mapper, TypeInformation<R> outputType) {
        return transform("Map", outputType, new StreamMap<>(clean(mapper)));
    }

    /**
     * Applies a FlatMap transformation on a {@link DataStream}. The transformation calls a {@link
     * FlatMapFunction} for each element of the DataStream. Each FlatMapFunction call can return any
     * number of elements including none. The user can also extend {@link RichFlatMapFunction} to
     * gain access to other features provided by the {@link
     * org.apache.flink.api.common.functions.RichFunction} interface.
     *
     * 应用 FlatMap 转换。
     * 对流中每个元素调用FlatMapFunction，可以返回零个、一个或多个元素。输入和输出是一对多或一对零的关系
     *
     * @param flatMapper The FlatMapFunction that is called for each element of the DataStream
     * @param <R> output type
     * @return The transformed {@link DataStream}.
     */
    public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {

        TypeInformation<R> outType =
                TypeExtractor.getFlatMapReturnTypes(
                        clean(flatMapper), getType(), Utils.getCallLocationName(), true);

        return flatMap(flatMapper, outType);
    }

    /**
     * Applies a FlatMap transformation on a {@link DataStream}. The transformation calls a {@link
     * FlatMapFunction} for each element of the DataStream. Each FlatMapFunction call can return any
     * number of elements including none. The user can also extend {@link RichFlatMapFunction} to
     * gain access to other features provided by the {@link
     * org.apache.flink.api.common.functions.RichFunction} interface.
     *
     * 与上一个方法类似，但允许指定输出类型
     *
     * @param flatMapper The FlatMapFunction that is called for each element of the DataStream
     * @param outputType {@link TypeInformation} for the result type of the function.
     * @param <R> output type
     * @return The transformed {@link DataStream}.
     */
    public <R> SingleOutputStreamOperator<R> flatMap(
            FlatMapFunction<T, R> flatMapper, TypeInformation<R> outputType) {
        return transform("Flat Map", outputType, new StreamFlatMap<>(clean(flatMapper)));
    }

    /**
     * Applies the given {@link ProcessFunction} on the input stream, thereby creating a transformed
     * output stream.
     *
     * <p>The function will be called for every element in the input streams and can produce zero or
     * more output elements.
     *
     *  应用 Process 转换。
     *  这是一个底层、功能强大的转换，可以访问事件的时间戳、处理时间，并设置定时器。
     *  它对流中的每个元素调用ProcessFunction，可以产生零个、一个或多个元素
     *
     * @param processFunction The {@link ProcessFunction} that is called for each element in the
     *     stream.
     * @param <R> The type of elements emitted by the {@code ProcessFunction}.
     * @return The transformed {@link DataStream}.
     */
    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> process(ProcessFunction<T, R> processFunction) {

        TypeInformation<R> outType =
                TypeExtractor.getUnaryOperatorReturnType(
                        processFunction,
                        ProcessFunction.class,
                        0,
                        1,
                        TypeExtractor.NO_INDEX,
                        getType(),
                        Utils.getCallLocationName(),
                        true);

        return process(processFunction, outType);
    }

    /**
     * Applies the given {@link ProcessFunction} on the input stream, thereby creating a transformed
     * output stream.
     *
     * <p>The function will be called for every element in the input streams and can produce zero or
     * more output elements.
     *
     * 应用一个Process 转换。
     * ProcessFunction 是一个功能强大的底层 API，它提供了对时间和状态的细粒度控制。
     * 你可以使用它来访问事件时间戳、处理时间，并注册定时器。
     * 该方法会为流中的每个元素调用 ProcessFunction，并可以产生零个、一个或多个输出元素。
     * outputType 参数显式指定了输出元素的类型
     *
     * @param processFunction The {@link ProcessFunction} that is called for each element in the
     *     stream.
     * @param outputType {@link TypeInformation} for the result type of the function.
     * @param <R> The type of elements emitted by the {@code ProcessFunction}.
     * @return The transformed {@link DataStream}.
     */
    @Internal
    public <R> SingleOutputStreamOperator<R> process(
            ProcessFunction<T, R> processFunction, TypeInformation<R> outputType) {

        ProcessOperator<T, R> operator = new ProcessOperator<>(clean(processFunction));

        return transform("Process", outputType, operator);
    }

    /**
     * Applies a Filter transformation on a {@link DataStream}. The transformation calls a {@link
     * FilterFunction} for each element of the DataStream and retains only those element for which
     * the function returns true. Elements for which the function returns false are filtered. The
     * user can also extend {@link RichFilterFunction} to gain access to other features provided by
     * the {@link org.apache.flink.api.common.functions.RichFunction} interface.
     *
     * 为流中的每个元素调用一个 FilterFunction，并只保留那些函数返回 true 的元素。返回 false 的元素将被丢弃
     *
     * @param filter The FilterFunction that is called for each element of the DataStream.
     * @return The filtered DataStream.
     */
    public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
        return transform("Filter", getType(), new StreamFilter<>(clean(filter)));
    }

    /**
     * Initiates a Project transformation on a {@link Tuple} {@link DataStream}.<br>
     * <b>Note: Only Tuple DataStreams can be projected.</b>
     *
     * <p>The transformation projects each Tuple of the DataSet onto a (sub)set of fields.
     *
     * 对元组类型的 DataStream 应用 Project 转换。
     * 这个方法允许你从一个元组中提取一个或多个字段，并按照指定的顺序创建一个新的元组。
     * 例如，你可以从一个包含三个字段的元组中提取第一个和第三个字段。注意：此方法仅适用于 Tuple 类型的数据流
     *
     * @param fieldIndexes The field indexes of the input tuples that are retained. The order of
     *     fields in the output tuple corresponds to the order of field indexes.
     * @return The projected DataStream
     * @see Tuple
     * @see DataStream
     */
    @PublicEvolving
    public <R extends Tuple> SingleOutputStreamOperator<R> project(int... fieldIndexes) {
        return new StreamProjection<>(this, fieldIndexes).projectTupleX();
    }

    /**
     * Creates a join operation. See {@link CoGroupedStreams} for an example of how the keys and
     * window can be specified.
     * coGroup 操作类似于 SQL 中的 GROUP BY，
     * 它在两个数据流上执行，根据指定的键和窗口将它们的分组。这个方法返回一个 CoGroupedStreams 对象，你可以用它来进一步配置键和窗口
     */
    public <T2> CoGroupedStreams<T, T2> coGroup(DataStream<T2> otherStream) {
        return new CoGroupedStreams<>(this, otherStream);
    }

    /**  两个数据流上执行，根据指定的键和窗口将它们连接起来。这个方法返回一个 JoinedStreams 对象，用于后续的键和窗口配置
     * Creates a join operation. See {@link JoinedStreams} for an example of how the keys and window
     * can be specified.
     */
    public <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream) {
        return new JoinedStreams<>(this, otherStream);
    }

    /**
     * Windows this {@code DataStream} into tumbling time windows.
     *
     * <p>This is a shortcut for either {@code .window(TumblingEventTimeWindows.of(size))} or {@code
     * .window(TumblingProcessingTimeWindows.of(size))} depending on the time characteristic set
     * using
     *
     * <p>Note: This operation is inherently non-parallel since all elements have to pass through
     * the same operator instance.
     *  所有 windowAll 相关的方法都处理非键控（non-keyed）流。
     *  这意味着所有元素都会被发送到同一个操作实例进行处理，因此并行度为 1，可能会成为性能瓶颈。
     *  对于大多数用例，都应使用键控的窗口
     * <p>{@link
     * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}
     *
     * @param size The size of the window.
     * @deprecated Please use {@link #windowAll(WindowAssigner)} with either {@link
     *     TumblingEventTimeWindows} or {@link TumblingProcessingTimeWindows}. For more information,
     *     see the deprecation notice on {@link TimeCharacteristic}
     */
    //将非键控流划分为滚动时间窗口
    @Deprecated
    public AllWindowedStream<T, TimeWindow> timeWindowAll(Time size) {
        if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
            return windowAll(TumblingProcessingTimeWindows.of(size));
        } else {
            return windowAll(TumblingEventTimeWindows.of(size));
        }
    }

    /**
     * Windows this {@code DataStream} into sliding time windows.
     *
     * <p>This is a shortcut for either {@code .window(SlidingEventTimeWindows.of(size, slide))} or
     * {@code .window(SlidingProcessingTimeWindows.of(size, slide))} depending on the time
     * characteristic set using {@link
     * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}
     *
     * <p>Note: This operation is inherently non-parallel since all elements have to pass through
     * the same operator instance.
     *
     * @param size The size of the window.
     * @deprecated Please use {@link #windowAll(WindowAssigner)} with either {@link
     *     SlidingEventTimeWindows} or {@link SlidingProcessingTimeWindows}. For more information,
     *     see the deprecation notice on {@link TimeCharacteristic}
     */
    //将非键控流划分为滑动时间窗口。它同样根据时间特性自动选择窗口分配器，并允许你指定窗口大小和滑动间隔
    @Deprecated
    public AllWindowedStream<T, TimeWindow> timeWindowAll(Time size, Time slide) {
        if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
            return windowAll(SlidingProcessingTimeWindows.of(size, slide));
        } else {
            return windowAll(SlidingEventTimeWindows.of(size, slide));
        }
    }

    /**
     * Windows this {@code DataStream} into tumbling count windows.
     *
     * <p>Note: This operation is inherently non-parallel since all elements have to pass through
     * the same operator instance.
     *
     * @param size The size of the windows in number of elements.
     */
    //将非键控流划分为滚动计数窗口。当收集到的元素数量达到 size 时，窗口会触发计算
    public AllWindowedStream<T, GlobalWindow> countWindowAll(long size) {
        return windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
    }

    /**
     * Windows this {@code DataStream} into sliding count windows.
     *
     * <p>Note: This operation is inherently non-parallel since all elements have to pass through
     * the same operator instance.
     *
     * @param size The size of the windows in number of elements.
     * @param slide The slide interval in number of elements.
     */
    //将非键控流划分为滑动计数窗口。当收集到的元素数量达到 size 时，窗口会触发计算，但每隔 slide 个元素就会滑动一次
    public AllWindowedStream<T, GlobalWindow> countWindowAll(long size, long slide) {
        return windowAll(GlobalWindows.create())
                .evictor(CountEvictor.of(size))
                .trigger(CountTrigger.of(slide));
    }

    /**
     * Windows this data stream to a {@code AllWindowedStream}, which evaluates windows over a non
     * key grouped stream. Elements are put into windows by a {@link
     * org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. The grouping of elements
     * is done by window.
     *
     * <p>A {@link org.apache.flink.streaming.api.windowing.triggers.Trigger} can be defined to
     * specify when windows are evaluated. However, {@code WindowAssigners} have a default {@code
     * Trigger} that is used if a {@code Trigger} is not specified.
     *
     * <p>Note: This operation is inherently non-parallel since all elements have to pass through
     * the same operator instance.
     *
     * @param assigner The {@code WindowAssigner} that assigns elements to windows.
     * @return The trigger windows data stream.
     */
    //将非键控流泛化为 AllWindowedStream。
    // 这是所有 windowAll 方法的底层通用方法，它接受一个 WindowAssigner 来定义如何将元素分配到窗口中
    @PublicEvolving
    public <W extends Window> AllWindowedStream<T, W> windowAll(
            WindowAssigner<? super T, W> assigner) {
        return new AllWindowedStream<>(this, assigner);
    }

    // ------------------------------------------------------------------------
    //  Timestamps and watermarks
    // ------------------------------------------------------------------------

    /**
     * Assigns timestamps to the elements in the data stream and generates watermarks to signal
     * event time progress. The given {@link WatermarkStrategy} is used to create a {@link
     * TimestampAssigner} and {@link WatermarkGenerator}.
     *
     * <p>For each element in the data stream, the {@link TimestampAssigner#extractTimestamp(Object,
     * long)} method is called to assign an event timestamp.
     *
     * <p>For each event in the data stream, the {@link WatermarkGenerator#onEvent(Object, long,
     * WatermarkOutput)} will be called.
     *
     * <p>Periodically (defined by the {@link ExecutionConfig#getAutoWatermarkInterval()}), the
     * {@link WatermarkGenerator#onPeriodicEmit(WatermarkOutput)} method will be called.
     *
     * <p>Common watermark generation patterns can be found as static methods in the {@link
     * org.apache.flink.api.common.eventtime.WatermarkStrategy} class.
     *
     * @param watermarkStrategy The strategy to generate watermarks based on event timestamps.
     * @return The stream after the transformation, with assigned timestamps and watermarks.
     */
    //为数据流分配时间戳和生成水位线。这是在 Flink 中启用事件时间处理的推荐方法
    public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
            WatermarkStrategy<T> watermarkStrategy) {
        final WatermarkStrategy<T> cleanedStrategy = clean(watermarkStrategy);
        // match parallelism to input, to have a 1:1 source -> timestamps/watermarks relationship
        // and chain
        final int inputParallelism = getTransformation().getParallelism();
        final TimestampsAndWatermarksTransformation<T> transformation =
                new TimestampsAndWatermarksTransformation<>(
                        "Timestamps/Watermarks",
                        inputParallelism,
                        getTransformation(),
                        cleanedStrategy,
                        false);
        getExecutionEnvironment().addOperator(transformation);
        return new SingleOutputStreamOperator<>(getExecutionEnvironment(), transformation);
    }

    /**
     * Assigns timestamps to the elements in the data stream and periodically creates watermarks to
     * signal event time progress.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to {@link
     * #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the new interfaces instead. The new
     * interfaces support watermark idleness and no longer need to differentiate between "periodic"
     * and "punctuated" watermarks.
     *
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     */
    //已弃用。这是旧版本的 API，用于定期生成水位线
    @Deprecated
    public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
            AssignerWithPeriodicWatermarks<T> timestampAndWatermarkAssigner) {

        final AssignerWithPeriodicWatermarks<T> cleanedAssigner =
                clean(timestampAndWatermarkAssigner);
        final WatermarkStrategy<T> wms =
                new AssignerWithPeriodicWatermarksAdapter.Strategy<>(cleanedAssigner);

        return assignTimestampsAndWatermarks(wms);
    }

    /**
     * Assigns timestamps to the elements in the data stream and creates watermarks based on events,
     * to signal event time progress.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to {@link
     * #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the new interfaces instead. The new
     * interfaces support watermark idleness and no longer need to differentiate between "periodic"
     * and "punctuated" watermarks.
     *
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     */
    //已弃用。这是旧版本的 API，用于基于事件生成水位线
    @Deprecated
    public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
            AssignerWithPunctuatedWatermarks<T> timestampAndWatermarkAssigner) {

        final AssignerWithPunctuatedWatermarks<T> cleanedAssigner =
                clean(timestampAndWatermarkAssigner);
        final WatermarkStrategy<T> wms =
                new AssignerWithPunctuatedWatermarksAdapter.Strategy<>(cleanedAssigner);

        return assignTimestampsAndWatermarks(wms);
    }

    // ------------------------------------------------------------------------
    //  Data sinks
    // ------------------------------------------------------------------------

    /**
     * Writes a DataStream to the standard output stream (stdout).
     *
     * <p>For each element of the DataStream the result of {@link Object#toString()} is written.
     *
     * <p>NOTE: This will print to stdout on the machine where the code is executed, i.e. the Flink
     * worker.
     *  将数据流中的每个元素转换成字符串，并写入标准输出流 (stdout)。
     *  打印操作会在执行 Flink 任务的工作节点上进行
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> print() {
        PrintSinkFunction<T> printFunction = new PrintSinkFunction<>();
        return addSink(printFunction).name("Print to Std. Out");
    }

    /**
     * Writes a DataStream to the standard error stream (stderr).
     *
     * <p>For each element of the DataStream the result of {@link Object#toString()} is written.
     *
     * <p>NOTE: This will print to stderr on the machine where the code is executed, i.e. the Flink
     * worker.
     * 类似 print()，但将输出写入标准错误流 (stderr)
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> printToErr() {
        PrintSinkFunction<T> printFunction = new PrintSinkFunction<>(true);
        return addSink(printFunction).name("Print to Std. Err");
    }

    /**
     * Writes a DataStream to the standard output stream (stdout).
     *
     * <p>For each element of the DataStream the result of {@link Object#toString()} is written.
     *
     * <p>NOTE: This will print to stdout on the machine where the code is executed, i.e. the Flink
     * worker.
     * 类似 print()，但允许您指定一个标识符作为输出的前缀，这在调试时区分不同数据流的输出很有用
     * @param sinkIdentifier The string to prefix the output with.
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> print(String sinkIdentifier) {
        PrintSinkFunction<T> printFunction = new PrintSinkFunction<>(sinkIdentifier, false);
        return addSink(printFunction).name("Print to Std. Out");
    }

    /**
     * Writes a DataStream to the standard error stream (stderr).
     *
     * <p>For each element of the DataStream the result of {@link Object#toString()} is written.
     *
     * <p>NOTE: This will print to stderr on the machine where the code is executed, i.e. the Flink
     * worker.
     * 类似 printToErr()，并允许您指定一个标识符作为输出的前缀
     * @param sinkIdentifier The string to prefix the output with.
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> printToErr(String sinkIdentifier) {
        PrintSinkFunction<T> printFunction = new PrintSinkFunction<>(sinkIdentifier, true);
        return addSink(printFunction).name("Print to Std. Err");
    }

    /**
     * Writes a DataStream to the file specified by path in text format.
     *
     * <p>For every element of the DataStream the result of {@link Object#toString()} is written.
     *  已弃用。将数据流写入指定路径的文本文件。每个元素都会调用 toString() 方法后写入。推荐使用 StreamingFileSink
     * @param path The path pointing to the location the text file is written to.
     * @return The closed DataStream.
     * @deprecated Please use the {@link
     *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly
     *     using the {@link #addSink(SinkFunction)} method.
     */
    @Deprecated
    @PublicEvolving
    public DataStreamSink<T> writeAsText(String path) {
        return writeUsingOutputFormat(new TextOutputFormat<T>(new Path(path)));
    }

    /**
     * Writes a DataStream to the file specified by path in text format.
     *
     * <p>For every element of the DataStream the result of {@link Object#toString()} is written.
     * 已弃用。将数据流写入文本文件，并允许指定写入模式（如覆盖或不覆盖）
     * @param path The path pointing to the location the text file is written to
     * @param writeMode Controls the behavior for existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @return The closed DataStream.
     * @deprecated Please use the {@link
     *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly
     *     using the {@link #addSink(SinkFunction)} method.
     */
    @Deprecated
    @PublicEvolving
    public DataStreamSink<T> writeAsText(String path, WriteMode writeMode) {
        TextOutputFormat<T> tof = new TextOutputFormat<>(new Path(path));
        tof.setWriteMode(writeMode);
        return writeUsingOutputFormat(tof);
    }

    /**
     * Writes a DataStream to the file specified by the path parameter.
     *
     * <p>For every field of an element of the DataStream the result of {@link Object#toString()} is
     * written. This method can only be used on data streams of tuples.
     * 已弃用。将元组类型的数据流写入 CSV 文件
     * @param path the path pointing to the location the text file is written to
     * @return the closed DataStream
     * @deprecated Please use the {@link
     *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly
     *     using the {@link #addSink(SinkFunction)} method.
     */
    @Deprecated
    @PublicEvolving
    public DataStreamSink<T> writeAsCsv(String path) {
        return writeAsCsv(
                path,
                null,
                CsvOutputFormat.DEFAULT_LINE_DELIMITER,
                CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
    }

    /**
     * Writes a DataStream to the file specified by the path parameter.
     *
     * <p>For every field of an element of the DataStream the result of {@link Object#toString()} is
     * written. This method can only be used on data streams of tuples.
     * 已弃用。将元组数据流写入 CSV 文件，并指定写入模式
     * @param path the path pointing to the location the text file is written to
     * @param writeMode Controls the behavior for existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @return the closed DataStream
     * @deprecated Please use the {@link
     *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly
     *     using the {@link #addSink(SinkFunction)} method.
     */
    @Deprecated
    @PublicEvolving
    public DataStreamSink<T> writeAsCsv(String path, WriteMode writeMode) {
        return writeAsCsv(
                path,
                writeMode,
                CsvOutputFormat.DEFAULT_LINE_DELIMITER,
                CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
    }

    /**
     * Writes a DataStream to the file specified by the path parameter. The writing is performed
     * periodically every millis milliseconds.
     *
     * <p>For every field of an element of the DataStream the result of {@link Object#toString()} is
     * written. This method can only be used on data streams of tuples.
     *
     * @param path the path pointing to the location the text file is written to
     * @param writeMode Controls the behavior for existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @param rowDelimiter the delimiter for two rows
     * @param fieldDelimiter the delimiter for two fields
     * @return the closed DataStream
     * @deprecated Please use the {@link
     *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly
     *     using the {@link #addSink(SinkFunction)} method.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    @PublicEvolving
    public <X extends Tuple> DataStreamSink<T> writeAsCsv(
            String path, WriteMode writeMode, String rowDelimiter, String fieldDelimiter) {
        Preconditions.checkArgument(
                getType().isTupleType(),
                "The writeAsCsv() method can only be used on data streams of tuples.");

        CsvOutputFormat<X> of = new CsvOutputFormat<>(new Path(path), rowDelimiter, fieldDelimiter);

        if (writeMode != null) {
            of.setWriteMode(writeMode);
        }

        return writeUsingOutputFormat((OutputFormat<T>) of);
    }

    /**
     * Writes the DataStream to a socket as a byte array. The format of the output is specified by a
     * {@link SerializationSchema}.
     *  将数据流中的元素序列化后，作为字节数组写入指定的网络套接字。此操作的并行度固定为1，以避免多个实例同时连接到同一个端口
     * @param hostName host of the socket
     * @param port port of the socket
     * @param schema schema for serialization
     * @return the closed DataStream
     */
    @PublicEvolving
    public DataStreamSink<T> writeToSocket(
            String hostName, int port, SerializationSchema<T> schema) {
        DataStreamSink<T> returnStream = addSink(new SocketClientSink<>(hostName, port, schema, 0));
        returnStream.setParallelism(
                1); // It would not work if multiple instances would connect to the same port
        return returnStream;
    }

    /**
     * Writes the dataStream into an output, described by an OutputFormat.
     *
     * <p>The output is not participating in Flink's checkpointing!
     *
     * <p>For writing to a file system periodically, the use of the {@link
     * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} is recommended.
     * 使用自定义的 OutputFormat 将数据流写入外部系统。不参与 Flink 的检查点机制，因此不推荐使用
     * @param format The output format
     * @return The closed DataStream
     * @deprecated Please use the {@link
     *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly
     *     using the {@link #addSink(SinkFunction)} method.
     */
    @Deprecated
    @PublicEvolving
    public DataStreamSink<T> writeUsingOutputFormat(OutputFormat<T> format) {
        return addSink(new OutputFormatSinkFunction<>(format));
    }

    /**
     * Method for passing user defined operators along with the type information that will transform
     * the DataStream.
     *
     * @param operatorName name of the operator, for logging purposes
     * @param outTypeInfo the output type of the operator
     * @param operator the object containing the transformation logic
     * @param <R> type of the return stream
     * @return the data stream constructed
     * @see #transform(String, TypeInformation, OneInputStreamOperatorFactory)
     */
    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> transform(
            String operatorName,
            TypeInformation<R> outTypeInfo,
            OneInputStreamOperator<T, R> operator) {

        return doTransform(operatorName, outTypeInfo, SimpleOperatorFactory.of(operator));
    }

    /**
     * Method for passing user defined operators created by the given factory along with the type
     * information that will transform the DataStream.
     *
     * <p>This method uses the rather new operator factories and should only be used when custom
     * factories are needed.
     *  这是 DataStream API 的底层通用转换方法。
     *  它允许您使用自定义的 OneInputStreamOperator 或 OneInputStreamOperatorFactory 来实现任何一对一的流转换。
     *  这通常用于实现 Flink API 本身，普通用户很少直接调用此方法
     * @param operatorName name of the operator, for logging purposes
     * @param outTypeInfo the output type of the operator
     * @param operatorFactory the factory for the operator.
     * @param <R> type of the return stream
     * @return the data stream constructed.
     */
    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> transform(
            String operatorName,
            TypeInformation<R> outTypeInfo,
            OneInputStreamOperatorFactory<T, R> operatorFactory) {

        return doTransform(operatorName, outTypeInfo, operatorFactory);
    }
    //内部方法，用于实际创建 OneInputTransformation 并将其添加到执行环境中
    protected <R> SingleOutputStreamOperator<R> doTransform(
            String operatorName,
            TypeInformation<R> outTypeInfo,
            StreamOperatorFactory<R> operatorFactory) {

        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        OneInputTransformation<T, R> resultTransform =
                new OneInputTransformation<>(
                        this.transformation,
                        operatorName,
                        operatorFactory,
                        outTypeInfo,
                        environment.getParallelism(),
                        false);

        @SuppressWarnings({"unchecked", "rawtypes"})
        SingleOutputStreamOperator<R> returnStream =
                new SingleOutputStreamOperator(environment, resultTransform);

        getExecutionEnvironment().addOperator(resultTransform);

        return returnStream;
    }

    /**
     * Internal function for setting the partitioner for the DataStream.
     *  内部方法，用于设置数据流的分区策略（如 rebalance, forward, shuffle 等）。它创建了一个 PartitionTransformation
     * @param partitioner Partitioner to set.
     * @return The modified DataStream.
     */
    protected DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
        return new DataStream<>(
                this.getExecutionEnvironment(),
                new PartitionTransformation<>(this.getTransformation(), partitioner));
    }

    /**
     * Adds the given sink to this DataStream. Only streams with sinks added will be executed once
     * the {@link StreamExecutionEnvironment#execute()} method is called.
     * 向数据流添加一个用户自定义的 SinkFunction。这是将数据流写入外部系统的最常用方法
     * @param sinkFunction The object containing the sink's invoke function.
     * @return The closed DataStream.
     */
    public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {

        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        // configure the type if needed
        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction).setInputType(getType(), getExecutionConfig());
        }

        return DataStreamSink.forSinkFunction(this, clean(sinkFunction));
    }

    /**
     * Adds the given {@link Sink} to this DataStream. Only streams with sinks added will be
     * executed once the {@link StreamExecutionEnvironment#execute()} method is called.
     * 使用 新的 Sink API (v2) 将数据流写入外部系统。这是 Flink 1.15+ 中推荐的 Sink 实现方式，提供了更丰富的语义和功能
     * @param sink The user defined sink.
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> sinkTo(org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?> sink) {
        return this.sinkTo(sink, CustomSinkOperatorUidHashes.DEFAULT);
    }

    /**
     * Adds the given {@link Sink} to this DataStream. Only streams with sinks added will be
     * executed once the {@link StreamExecutionEnvironment#execute()} method is called.
     *
     * <p>This method is intended to be used only to recover a snapshot where no uids have been set
     * before taking the snapshot.
     *  这是 sinkTo 方法的一个重载，用于新的 Sink API
     * @param sink The user defined sink.
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> sinkTo(
            org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?> sink,
            CustomSinkOperatorUidHashes customSinkOperatorUidHashes) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        return DataStreamSink.forSinkV1(this, sink, customSinkOperatorUidHashes);
    }

    /**
     * Adds the given {@link Sink} to this DataStream. Only streams with sinks added will be
     * executed once the {@link StreamExecutionEnvironment#execute()} method is called.
     *
     * @param sink The user defined sink.
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> sinkTo(Sink<T> sink) {
        return this.sinkTo(sink, CustomSinkOperatorUidHashes.DEFAULT);
    }

    /**
     * Adds the given {@link Sink} to this DataStream. Only streams with sinks added will be
     * executed once the {@link StreamExecutionEnvironment#execute()} method is called.
     *
     * <p>This method is intended to be used only to recover a snapshot where no uids have been set
     * before taking the snapshot.
     *
     * @param customSinkOperatorUidHashes operator hashes to support state binding
     * @param sink The user defined sink.
     * @return The closed DataStream.
     */
    @PublicEvolving
    public DataStreamSink<T> sinkTo(
            Sink<T> sink, CustomSinkOperatorUidHashes customSinkOperatorUidHashes) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        return DataStreamSink.forSink(this, sink, customSinkOperatorUidHashes);
    }

    /**
     * Triggers the distributed execution of the streaming dataflow and returns an iterator over the
     * elements of the given DataStream.
     *
     * <p>The DataStream application is executed in the regular distributed manner on the target
     * environment, and the events from the stream are polled back to this application process and
     * thread through Flink's REST API.
     *  执行 Flink 作业并返回一个迭代器，
     *  通过该迭代器可以收集数据流中的所有元素。
     *  这在本地测试和调试时非常方便。重要提示：必须关闭此迭代器以释放集群资源
     * <p><b>IMPORTANT</b> The returned iterator must be closed to free all cluster resources.
     */
    public CloseableIterator<T> executeAndCollect() throws Exception {
        return executeAndCollect("DataStream Collect");
    }

    /**
     * Triggers the distributed execution of the streaming dataflow and returns an iterator over the
     * elements of the given DataStream.
     *
     * <p>The DataStream application is executed in the regular distributed manner on the target
     * environment, and the events from the stream are polled back to this application process and
     * thread through Flink's REST API.
     *
     * <p><b>IMPORTANT</b> The returned iterator must be closed to free all cluster resources.
     */
    public CloseableIterator<T> executeAndCollect(String jobExecutionName) throws Exception {
        return executeAndCollectWithClient(jobExecutionName).iterator;
    }

    /**
     * Triggers the distributed execution of the streaming dataflow and returns an iterator over the
     * elements of the given DataStream.
     *  执行作业并收集前 limit 个元素到一个列表中
     * <p>The DataStream application is executed in the regular distributed manner on the target
     * environment, and the events from the stream are polled back to this application process and
     * thread through Flink's REST API.
     */
    public List<T> executeAndCollect(int limit) throws Exception {
        return executeAndCollect("DataStream Collect", limit);
    }

    /**
     * Triggers the distributed execution of the streaming dataflow and returns an iterator over the
     * elements of the given DataStream.
     *
     * <p>The DataStream application is executed in the regular distributed manner on the target
     * environment, and the events from the stream are polled back to this application process and
     * thread through Flink's REST API.
     */
    public List<T> executeAndCollect(String jobExecutionName, int limit) throws Exception {
        Preconditions.checkState(limit > 0, "Limit must be greater than 0");

        try (ClientAndIterator<T> clientAndIterator =
                executeAndCollectWithClient(jobExecutionName)) {
            List<T> results = new ArrayList<>(limit);
            while (limit > 0 && clientAndIterator.iterator.hasNext()) {
                results.add(clientAndIterator.iterator.next());
                limit--;
            }

            return results;
        }
    }

    /**
     * Sets up the collection of the elements in this {@link DataStream}, and returns an iterator
     * over the collected elements that can be used to retrieve elements once the job execution has
     * started.
     *
     * <p>Caution: When multiple streams are being collected it is recommended to consume all
     * streams in parallel to not back-pressure the job.
     *
     * <p>Caution: Closing the returned iterator cancels the job! It is recommended to close all
     * iterators once you are no longer interested in any of the collected streams.
     *
     * <p>This method is functionally equivalent to {@link #collectAsync(Collector)}.
     *
     * @return iterator over the contained elements
     */
    @Experimental
    public CloseableIterator<T> collectAsync() {
        final Collector<T> collector = new Collector<>();
        collectAsync(collector);
        return collector.getOutput();
    }

    /**
     * Sets up the collection of the elements in this {@link DataStream}, which can be retrieved
     * later via the given {@link Collector}.
     *
     * <p>Caution: When multiple streams are being collected it is recommended to consume all
     * streams in parallel to not back-pressure the job.
     *
     * <p>Caution: Closing the iterator from the collector cancels the job! It is recommended to
     * close all iterators once you are no longer interested in any of the collected streams.
     *
     * <p>This method is functionally equivalent to {@link #collectAsync()}.
     *
     * <p>This method is meant to support use-cases where the application of a sink is done via a
     * {@code Consumer<DataStream<T>>}, where it wouldn't be possible (or inconvenient) to return an
     * iterator.
     *
     * @param collector a collector that can be used to retrieve the elements
     */
    @Experimental
    public void collectAsync(Collector<T> collector) {
        TypeSerializer<T> serializer =
                getType()
                        .createSerializer(
                                getExecutionEnvironment().getConfig().getSerializerConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID().toString();

        StreamExecutionEnvironment env = getExecutionEnvironment();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        long resultFetchTimeout =
                env.getConfiguration().get(RpcOptions.ASK_TIMEOUT_DURATION).toMillis();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig(),
                        resultFetchTimeout);
        CollectStreamSink<T> sink = new CollectStreamSink<>(this, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());

        env.registerCollectIterator(iterator);
        collector.setIterator(iterator);
    }

    /**
     * Collect records from each partition into a separate full window. The window emission will be
     * triggered at the end of inputs. For this non-keyed data stream(each record has no key), a
     * partition contains all records of a subtask.
     *
     * @return The full windowed data stream on partition.
     */
    @PublicEvolving
    public PartitionWindowedStream<T> fullWindowPartition() {
        return new NonKeyedPartitionWindowedStream<>(environment, this);
    }

    /**
     * This class acts as an accessor to elements collected via {@link #collectAsync(Collector)}.
     *
     * @param <T> the element type
     */
    @Experimental
    public static class Collector<T> {
        private CloseableIterator<T> iterator;

        @Internal
        void setIterator(CloseableIterator<T> iterator) {
            this.iterator = iterator;
        }

        /**
         * Returns an iterator over the collected elements. The returned iterator must only be used
         * once the job execution was triggered.
         *
         * <p>This method will always return the same iterator instance.
         *
         * @return iterator over collected elements
         */
        public CloseableIterator<T> getOutput() {
            // we intentionally fail here instead of waiting, because it indicates a
            // misunderstanding on the user and would usually just block the application
            Preconditions.checkNotNull(iterator, "The job execution was not yet started.");
            return iterator;
        }
    }

    ClientAndIterator<T> executeAndCollectWithClient(String jobExecutionName) throws Exception {
        final CloseableIterator<T> iterator = collectAsync();

        final JobClient jobClient = getExecutionEnvironment().executeAsync(jobExecutionName);

        return new ClientAndIterator<>(jobClient, iterator);
    }

    /**
     * Returns the {@link Transformation} that represents the operation that logically creates this
     * {@link DataStream}.
     *
     * @return The Transformation
     */
    @Internal
    public Transformation<T> getTransformation() {
        return transformation;
    }
}
