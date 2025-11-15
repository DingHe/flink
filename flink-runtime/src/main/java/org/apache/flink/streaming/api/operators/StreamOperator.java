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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of {@link
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator} or {@link
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators that process
 * elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} offers
 * default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
// 所有算子（Operator） 的基本接口。
// 算子是 Flink 流处理程序的核心执行单元，它们定义了对数据流进行转换、处理、聚合等操作的逻辑
@PublicEvolving
public interface StreamOperator<OUT> extends CheckpointListener, KeyContext, Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic.
     *
     * @implSpec In case of recovery, this method needs to ensure that all recovered data is
     *     processed before passing back control, so that the order of elements is ensured during
     *     the recovery of an operator chain (operators are opened from the tail operator to the
     *     head operator).
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    // 算子启动时的初始化方法
    void open() throws Exception;

    /**
     * This method is called at the end of data processing.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions during this
     * flushing of buffered data should be propagated, in order to cause the operation to be
     * recognized as failed, because the last data items are not processed properly.
     *
     * <p><b>After this method is called, no more records can be produced for the downstream
     * operators.</b>
     *
     * <p><b>WARNING:</b> It is not safe to use this method to commit any transactions or other side
     * effects! You can use this method to flush any buffered data that can later on be committed
     * e.g. in a {@link StreamOperator#notifyCheckpointComplete(long)}.
     *
     * <p><b>NOTE:</b>This method does not need to close any resources. You should release external
     * resources in the {@link #close()} method.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    //算子处理完所有数据后的收尾方法
    void finish() throws Exception;

    /**
     * This method is called at the very end of the operator's life, both in the case of a
     * successful completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources that the
     * operator has acquired.
     *
     * <p><b>NOTE:</b>It can not emit any records! If you need to emit records at the end of
     * processing, do so in the {@link #finish()} method.
     */
    // 算子生命周期结束时的资源释放方法
    // 在作业成功完成或失败取消时，都会调用此方法。
    // 其目的是彻底释放算子所持有的所有外部资源，例如关闭数据库连接、文件句柄等。此方法中不能再发送任何记录到下游
    void close() throws Exception;

    // ------------------------------------------------------------------------
    //  state snapshots
    // ------------------------------------------------------------------------

    /**
     * This method is called when the operator should do a snapshot, before it emits its own
     * checkpoint barrier.
     *
     * <p>This method is intended not for any actual state persistence, but only for emitting some
     * data before emitting the checkpoint barrier. Operators that maintain some small transient
     * state that is inefficient to checkpoint (especially when it would need to be checkpointed in
     * a re-scalable way) but can simply be sent downstream before the checkpoint. An example are
     * opportunistic pre-aggregation operators, which have small the pre-aggregation state that is
     * frequently flushed downstream.
     *
     * <p><b>Important:</b> This method should not be used for any actual state snapshot logic,
     * because it will inherently be within the synchronous part of the operator's checkpoint. If
     * heavy work is done within this method, it will affect latency and downstream checkpoint
     * alignments.
     *
     * @param checkpointId The ID of the checkpoint.
     * @throws Exception Throwing an exception here causes the operator to fail and go into
     *     recovery.
     */
    // 在算子发出检查点屏障（Checkpoint Barrier）之前，准备进行快照的方法
    // 此方法在同步快照阶段被调用，但其主要目的不是持久化状态，而是处理一些瞬态数据。
    // 例如，一个预聚合算子可能将少量累积的状态刷新到下游，以便在快照中不需要包含这些状态，从而提高效率。
    // 它确保了在正式快照之前，算子已经处理并发送了所有必要的数据
    void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Called to draw a state snapshot from the operator.
     *
     * @return a runnable future to the state handle that points to the snapshotted state. For
     *     synchronous implementations, the runnable might already be finished.
     * @throws Exception exception that happened during snapshotting.
     */
    // 创建状态快照的核心方法
    // 当检查点屏障到达时，此方法被调用，负责将算子的所有状态（keyed state 和 operator state）序列化并异步地写入到持久化存储中。
    // 它返回一个 OperatorSnapshotFutures 对象，其中包含指向快照状态句柄的可运行Future，允许 Flink 异步地处理快照写入，避免阻塞数据流
    OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception;

    /** Provides a context to initialize all state in the operator. */
    //初始化或恢复算子状态的方法
    //此方法在 open() 方法之前被调用，用于根据 Flink 运行时提供的 StreamTaskStateInitializer 来初始化算子的状态。
    // 在首次启动时，它会初始化空状态；在从检查点或保存点恢复时，它会从已保存的状态中恢复数据
    void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception;

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------
    // 设置键上下文的方法
    // 这些方法用于为键控操作（如 keyBy 后续的操作）设置当前的键上下文。
    // 当一个元素被处理时，Flink 运行时会调用这些方法，将该元素的键信息传递给算子。这使得算子能够访问和修改与该键绑定的状态
    void setKeyContextElement1(StreamRecord<?> record) throws Exception;

    void setKeyContextElement2(StreamRecord<?> record) throws Exception;
    //获取度量组的方法
    OperatorMetricGroup getMetricGroup();
    //获取算子 ID 的方法
    OperatorID getOperatorID();

    /**
     * Called to get the OperatorAttributes of the operator. If there is no defined attribute, a
     * default OperatorAttributes is built.
     *
     * @return OperatorAttributes of the operator.
     */
    // 获取算子属性的方法
    @Experimental
    default OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().build();
    }
}
