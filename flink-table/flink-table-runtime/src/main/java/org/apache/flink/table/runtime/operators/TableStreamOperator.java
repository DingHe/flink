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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Table operator to invoke close always. This is a base class for both batch and stream operators
 * without key.
 */
// TableStreamOperator<OUT> 是 Flink Table API 和 SQL 运行时中所有非 Keyed（非分组/非键控）流和批处理算子的抽象基类。
// 统一基类： 为 Flink Table 算子提供一个统一的起点，继承自 Flink DataStream API 的 AbstractStreamOperator，从而接入 Flink 的底层运行时框架。
// 强制 Chaining： 将算子链策略设置为 ChainingStrategy.ALWAYS，确保该算子默认总是与前后的算子链在一起（如果可能），以减少序列化/反序列化和网络传输的开销，提高效率
// 计时器和水位线管理： 为非 Keyed 算子提供访问当前水位线和处理时间的服务，尽管它本身不支持注册基于键的计时器。
// 内存计算： 提供了一个工具方法用于计算算子被分配的托管内存（Managed Memory）大小，这对于需要大量内存的算子（如 Hash Join 或 Sort）至关重要。
public abstract class TableStreamOperator<OUT> extends AbstractStreamOperator<OUT> {

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    // 当前水位线时间戳。 存储最近接收到的水位线的时间戳。
    // 由于非 Keyed 算子没有内置的 InternalTimerService 来跟踪时间，所以算子必须手动监听并存储它。初始化为 Long.MIN_VALUE。
    protected long currentWatermark = Long.MIN_VALUE;
    // 上下文实现对象。
    // 在 open() 方法中实例化，它封装了算子在处理元素时所需的时间上下文信息（如当前记录的时间戳、当前处理时间、当前水位线）。
    protected transient ContextImpl ctx;

    public TableStreamOperator() {
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.ctx = new ContextImpl(getProcessingTimeService());
    }
    // 表明该算子使用的是可拆分的计时器。
    // 这对于 非 Keyed 算子 来说是默认且必要的，因为它不依赖于键上下文。
    @Override
    public boolean useSplittableTimers() {
        return true;
    }

    /** Compute memory size from memory faction. */
    // 计算托管内存大小。 通过访问运行时环境 (Environment) 和内存管理器 (MemoryManager)，
    // 根据 Job 配置中为该算子配置的托管内存比例 (ManagedMemoryUseCase.OPERATOR)，计算出算子被分配的实际内存字节数。
    public long computeMemorySize() {
        final Environment environment = getContainingTask().getEnvironment();
        return environment
                .getMemoryManager()
                .computeMemorySize(
                        getOperatorConfig()
                                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        environment.getJobConfiguration(),
                                        environment.getTaskManagerInfo().getConfiguration(),
                                        environment.getUserCodeClassLoader().asClassLoader()));
    }
    // 处理水位线事件。 在调用父类的 processWatermark() 之前，
    // 先将传入的水位线时间戳存储到本地属性 currentWatermark 中，以便 ContextImpl 可以查询到最新的水位线。
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        currentWatermark = mark.getTimestamp();
        super.processWatermark(mark);
    }

    /** Information available in an invocation of processElement. */
    protected class ContextImpl implements TimerService {

        protected final ProcessingTimeService timerService;

        public StreamRecord<?> element;

        ContextImpl(ProcessingTimeService timerService) {
            this.timerService = checkNotNull(timerService);
        }

        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
        }

        @Override
        public long currentProcessingTime() {
            return timerService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Setting timers is only supported on a keyed streams.");
        }

        @Override
        public void registerEventTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Setting timers is only supported on a keyed streams.");
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Delete timers is only supported on a keyed streams.");
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Delete timers is only supported on a keyed streams.");
        }

        public TimerService timerService() {
            return this;
        }
    }
}
