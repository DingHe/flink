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

package org.apache.flink.table.runtime.operators.bundle.trigger;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * A {@link CoBundleTrigger} is similar with {@link BundleTrigger}, and the only differences is
 * {@link CoBundleTrigger} can handle two inputs.
 *
 * @param <IN1> The first input element type.
 * @param <IN2> The second input element type.
 */
// CoBundleTrigger 接口是 Flink Table API/SQL 中用于双输入算子（Two-Input Operator）的批处理触发器。
// 核心作用在于控制 Flink 算子中**微批处理（Mini-Batching）或捆绑处理（Bundling）**的执行时机。
// 在流处理中，为了提高吞吐量，算子不会对每条记录立即进行计算，而是将一定数量或满足一定条件的记录收集成一个“捆绑（Bundle）”，然后批量处理这个捆绑。
// 处理双输入： 它可以同时接收和响应来自第一个输入流 (IN1) 和第二个输入流 (IN2) 的元素。
// 决定批处理时机： 它根据预设的策略（例如：元素数量达到上限、时间窗口到达等），判断当前收集的捆绑是否已满，并决定何时触发对已收集记录的批量计算 (finishBundle)
// IN1 表示第一个输入流中元素的类型
// IN2 表示第二个输入流中元素的类型。
@Internal
public interface CoBundleTrigger<IN1, IN2> extends Serializable {

    /** Register a callback which will be called once this trigger decides to finish this bundle. */
    // 注册回调函数。
    // 将一个 BundleTriggerCallback 对象注册到触发器中。
    // 当触发器决定要结束当前的捆绑（即达到触发条件）时，它会调用这个回调对象上的 finishBundle() 方法来通知外部算子执行批量计算。
    void registerCallback(BundleTriggerCallback callback);

    /**
     * Called for every element that gets added to the bundle from the first input. If the trigger
     * decides to start evaluate, {@link BundleTriggerCallback#finishBundle()} should be invoked.
     *
     * @param element The element that arrived from the first input.
     */
    // 处理输入 1 元素。
    // 当一个元素从第一个输入流到达并被添加到当前捆绑时，会调用此方法。
    // 该方法的实现逻辑会根据新到达的元素来评估是否满足触发条件。
    // 如果满足，必须调用已注册的 callback.finishBundle() 方法。
    void onElement1(final IN1 element) throws Exception;

    /**
     * Called for every element that gets added to the bundle from the second input. If the trigger
     * decides to start evaluate, {@link BundleTriggerCallback#finishBundle()} should be invoked.
     *
     * @param element The element that arrived from the second input.
     */
    // 处理输入 2 元素。
    // 当一个元素从第二个输入流到达并被添加到当前捆绑时，会调用此方法。
    // 与 onElement1 类似，用于评估来自第二个流的元素是否触发批量计算。
    void onElement2(final IN2 element) throws Exception;

    /** Reset the trigger to its initiate status. */
    // 重置触发器状态。
    // 在每次成功触发 finishBundle 并完成批量计算后，算子会调用此方法，
    void reset();
    // 提供说明。
    // 返回一个字符串，用于描述当前触发器的具体实现和配置（例如，是“每 1000 条记录触发”还是“每 1 秒触发”），主要用于日志或监控。
    String explain();
}
