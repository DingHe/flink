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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The WatermarkStrategy defines how to generate {@link Watermark}s in the stream sources. The
 * WatermarkStrategy is a builder/factory for the {@link WatermarkGenerator} that generates the
 * watermarks and the {@link TimestampAssigner} which assigns the internal timestamp of a record.
 *
 * <p>This interface is split into three parts: 1) methods that an implementor of this interface
 * needs to implement, 2) builder methods for building a {@code WatermarkStrategy} on a base
 * strategy, 3) convenience methods for constructing a {@code WatermarkStrategy} for common built-in
 * strategies or based on a {@link WatermarkGeneratorSupplier}
 *
 * <p>Implementors of this interface need only implement {@link
 * #createWatermarkGenerator(WatermarkGeneratorSupplier.Context)}. Optionally, you can implement
 * {@link #createTimestampAssigner(TimestampAssignerSupplier.Context)}.
 *
 * <p>The builder methods, like {@link #withIdleness(Duration)} or {@link
 * #createTimestampAssigner(TimestampAssignerSupplier.Context)} create a new {@code
 * WatermarkStrategy} that wraps and enriches a base strategy. The strategy on which the method is
 * called is the base strategy.
 *
 * <p>The convenience methods, for example {@link #forBoundedOutOfOrderness(Duration)}, create a
 * {@code WatermarkStrategy} for common built in strategies.
 *
 * <p>This interface is {@link Serializable} because watermark strategies may be shipped to workers
 * during distributed execution.
 */
//用于定义水印生成和时间戳分配策略的核心接口。
// 它是一个“构建器”或“工厂”，旨在将时间戳分配器（TimestampAssigner）和水印生成器（WatermarkGenerator）的创建逻辑封装在一起
@Public
public interface WatermarkStrategy<T>
        extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {

    // ------------------------------------------------------------------------
    //  Methods that implementors need to implement.
    // ------------------------------------------------------------------------

    /** Instantiates a WatermarkGenerator that generates watermarks according to this strategy. */
    //实例化并返回一个 WatermarkGenerator
    @Override
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);

    /**
     * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this strategy.
     */
    //实例化并返回一个 TimestampAssigner。这是一个可选实现的方法，因为它有默认实现
    @Override
    default TimestampAssigner<T> createTimestampAssigner(
            TimestampAssignerSupplier.Context context) {
        // By default, this is {@link RecordTimestampAssigner},
        // for cases where records come out of a source with valid timestamps, for example from
        // Kafka.
        return new RecordTimestampAssigner<>();
    }

    /**
     * Provides configuration for watermark alignment of a maximum watermark of multiple
     * sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     */
    //获取水印对齐的配置参数。这也是一个可选方法，默认返回禁用水印对齐的参数
    @PublicEvolving
    default WatermarkAlignmentParams getAlignmentParameters() {
        return WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED;
    }

    // ------------------------------------------------------------------------
    //  Builder methods for enriching a base WatermarkStrategy
    // ------------------------------------------------------------------------

    /**
     * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
     * {@link TimestampAssigner} (via a {@link TimestampAssignerSupplier}).
     *
     * <p>You can use this when a {@link TimestampAssigner} needs additional context, for example
     * access to the metrics system.
     *
     * <pre>
     * {@code WatermarkStrategy<Object> wmStrategy = WatermarkStrategy
     *   .forMonotonousTimestamps()
     *   .withTimestampAssigner((ctx) -> new MetricsReportingAssigner(ctx));
     * }</pre>
     */
    //创建一个新的 WatermarkStrategy，它使用指定的 TimestampAssigner
    default WatermarkStrategy<T> withTimestampAssigner(
            TimestampAssignerSupplier<T> timestampAssigner) {
        checkNotNull(timestampAssigner, "timestampAssigner");
        return new WatermarkStrategyWithTimestampAssigner<>(this, timestampAssigner);
    }

    /**
     * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
     * {@link SerializableTimestampAssigner}.
     *
     * <p>You can use this in case you want to specify a {@link TimestampAssigner} via a lambda
     * function.
     *
     * <pre>
     * {@code WatermarkStrategy<CustomObject> wmStrategy = WatermarkStrategy
     *   .<CustomObject>forMonotonousTimestamps()
     *   .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
     * }</pre>
     */
    default WatermarkStrategy<T> withTimestampAssigner(
            SerializableTimestampAssigner<T> timestampAssigner) {
        checkNotNull(timestampAssigner, "timestampAssigner");
        return new WatermarkStrategyWithTimestampAssigner<>(
                this, TimestampAssignerSupplier.of(timestampAssigner));
    }

    /**
     * Creates a new enriched {@link WatermarkStrategy} that also does idleness detection in the
     * created {@link WatermarkGenerator}.
     *
     * <p>Add an idle timeout to the watermark strategy. If no records flow in a partition of a
     * stream for that amount of time, then that partition is considered "idle" and will not hold
     * back the progress of watermarks in downstream operators.
     *
     * <p>Idleness can be important if some partitions have little data and might not have events
     * during some periods. Without idleness, these streams can stall the overall event time
     * progress of the application.
     */
    //创建一个新的 WatermarkStrategy，在其中添加空闲检测功能
    default WatermarkStrategy<T> withIdleness(Duration idleTimeout) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        return new WatermarkStrategyWithIdleness<>(this, idleTimeout);
    }

    /**
     * Creates a new {@link WatermarkStrategy} that configures the maximum watermark drift from
     * other sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     *
     * @param watermarkGroup A group of sources to align watermarks
     * @param maxAllowedWatermarkDrift Maximal drift, before we pause consuming from the
     *     source/task/partition
     */
    //创建一个新的 WatermarkStrategy，以配置水印对齐功能
    //水位对齐（Watermark Alignment） 解决多个数据源或分区之间水印时间戳差异过大的特性
    //当你的 Flink 作业消费来自多个并行源头（例如，Kafka 的多个分区）的数据时，如果某些源头的数据流速非常快，而另一些则很慢甚至停止，就会导致一个严重的性能问题：反压和内存溢出
    @PublicEvolving
    default WatermarkStrategy<T> withWatermarkAlignment(
            String watermarkGroup, Duration maxAllowedWatermarkDrift) {
        return withWatermarkAlignment(
                watermarkGroup,
                maxAllowedWatermarkDrift,
                WatermarksWithWatermarkAlignment.DEFAULT_UPDATE_INTERVAL);
    }

    /**
     * Creates a new {@link WatermarkStrategy} that configures the maximum watermark drift from
     * other sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     *
     * @param watermarkGroup A group of sources to align watermarks
     * @param maxAllowedWatermarkDrift Maximal drift, before we pause consuming from the
     *     source/task/partition
     * @param updateInterval How often tasks should notify coordinator about the current watermark
     *     and how often the coordinator should announce the maximal aligned watermark.
     */
    @PublicEvolving
    default WatermarkStrategy<T> withWatermarkAlignment(
            String watermarkGroup, Duration maxAllowedWatermarkDrift, Duration updateInterval) {
        checkNotNull(watermarkGroup, "watermarkGroup cannot be null");
        checkNotNull(maxAllowedWatermarkDrift, "maxAllowedWatermarkDrift cannot be null");
        checkNotNull(updateInterval, "updateInterval cannot be null");
        checkArgument(
                !maxAllowedWatermarkDrift.isNegative(),
                "maxAllowedWatermarkDrift must be greater than or equal to zero");
        checkArgument(
                !(updateInterval.isZero() || updateInterval.isNegative()),
                "updateInterval must be positive");
        return new WatermarksWithWatermarkAlignment<T>(
                this, watermarkGroup, maxAllowedWatermarkDrift, updateInterval);
    }

    // ------------------------------------------------------------------------
    //  Convenience methods for common watermark strategies
    // ------------------------------------------------------------------------

    /**
     * Creates a watermark strategy for situations with monotonously ascending timestamps.
     *
     * <p>The watermarks are generated periodically and tightly follow the latest timestamp in the
     * data. The delay introduced by this strategy is mainly the periodic interval in which the
     * watermarks are generated.
     *创建一个适用于单调递增时间戳的水位线策略。生成的水位线紧随事件的时间戳进展
     * @see AscendingTimestampsWatermarks
     */
    static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
        return (ctx) -> new AscendingTimestampsWatermarks<>();
    }

    /**
     * Creates a watermark strategy for situations where records are out of order, but you can place
     * an upper bound on how far the events are out of order. An out-of-order bound B means that
     * once the an event with timestamp T was encountered, no events older than {@code T - B} will
     * follow any more.
     *
     * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy
     * is the periodic interval length, plus the out of orderness bound.
     *创建一个适用于事件有最大乱序边界的水位线策略。该策略支持一定范围的乱序事件
     * @see BoundedOutOfOrdernessWatermarks
     */
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return (ctx) -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
    }

    /** Creates a watermark strategy based on an existing {@link WatermarkGeneratorSupplier}. 创建一个自定义的水位线生成器策略，适用于需要自定义水位线生成逻辑的场景*/
    static <T> WatermarkStrategy<T> forGenerator(WatermarkGeneratorSupplier<T> generatorSupplier) {
        return generatorSupplier::createWatermarkGenerator;
    }

    /**
     * Creates a watermark strategy that generates no watermarks at all. This may be useful in
     * scenarios that do pure processing-time based stream processing.创建一个不生成水位线的策略，适用于基于处理时间而非事件时间的流处理应用
     */
    static <T> WatermarkStrategy<T> noWatermarks() {
        return (ctx) -> new NoWatermarksGenerator<>();
    }
}
