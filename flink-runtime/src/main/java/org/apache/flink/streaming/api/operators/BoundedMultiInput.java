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

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for multi-input operators that need to be notified about the logical/semantical end of
 * input.
 *
 * <p><b>NOTE:</b> Classes should not implement both {@link BoundedOneInput} and {@link
 * BoundedMultiInput} at the same time!
 *
 * @see BoundedOneInput
 */
// BoundedMultiInput 接口的主要作用是允许具有多个输入的 Flink 运算符感知到单个输入流的逻辑结束。
// 在处理有界流或将流作业转换为批处理模式（如 Flink 的 FLIP-27 批处理模式）时，知道何时不再有数据从某个特定的输入通道到达是非常重要的。
// 逻辑结束通知： 它提供了一个钩子 (endInput 方法)，当运行时系统确认某个特定输入（例如第一个输入或第二个输入）的数据已全部处理完毕时，会调用此方法来通知运算符。
// 资源清理与数据刷新： 运算符可以利用这个通知来进行针对该输入流的最终操作，例如：
// 刷新为该输入缓冲的任何数据。
// 在自定义的 InputSelectable 调度逻辑中，将该输入标记为永久关闭。
@PublicEvolving
public interface BoundedMultiInput {

    /**
     * It is notified that no more data will arrive from the input identified by the {@code
     * inputId}. The {@code inputId} is numbered starting from 1, and `1` indicates the first input.
     *
     * <p><b>WARNING:</b> It is not safe to use this method to commit any transactions or other side
     * effects! You can use this method to e.g. flush data buffered for the given input or implement
     * an ordered reading from multiple inputs via {@link InputSelectable}.
     */
    // 通知运算符一个输入流的数据已结束。
    void endInput(int inputId) throws Exception;
}
