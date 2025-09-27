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

package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@code Window} is a grouping of elements into finite buckets. Windows have a maximum timestamp
 * which means that, at some point, all elements that go into one window will have arrived.
 *
 * <p>Subclasses should implement {@code equals()} and {@code hashCode()} so that logically same
 * windows are treated the same.
 */
//Window 类是 Apache Flink 中窗口概念的抽象基类
//在流处理中，数据流是无限的，为了对数据进行聚合、统计等操作，我们需要将无限的数据流切分成有限的、有界的“桶”，这些“桶”就是窗口
//是一个元素的集合，并且有一个最大时间戳。这个最大时间戳（maxTimestamp）是窗口的一个核心属性，它代表了在该时间戳之前到达的所有元素都应该属于这个窗口
@PublicEvolving
public abstract class Window {

    /**
     * Gets the largest timestamp that still belongs to this window.
     *
     * @return The largest timestamp that still belongs to this window.
     */
    //用于获取窗口所能包含的最大时间戳
    //是窗口的“结束”时间，但要注意，它并不是指窗口的结束边界本身，而是窗口内元素可能拥有的最大时间戳
    public abstract long maxTimestamp();
}
