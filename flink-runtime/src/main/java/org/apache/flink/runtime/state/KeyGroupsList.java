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

package org.apache.flink.runtime.state;

/** This interface offers ordered random read access to multiple key group ids. */
// 抽象地表示和管理一个 Flink 任务实例（通常是 TaskManager 上的一个子任务）被分配到的所有键组（Key Group）ID 集合
// 在 Flink 的键控状态（Keyed State）管理中，为了实现状态的负载均衡和弹性伸缩（Rescaling），整个作业的键空间被划分为固定数量的 Key Group。每个并行任务（Subtask）会被分配到这些 Key Group ID 的一个子集。
public interface KeyGroupsList extends Iterable<Integer> {

    /** Returns the number of key group ids in the list. */
    // 获取键组数量。
    // 返回这个列表（即当前任务实例）被分配到的 Key Group ID 的总个数。
    int getNumberOfKeyGroups();

    /**
     * Returns the id of the keygroup at the given index, where index in interval [0, {@link
     * #getNumberOfKeyGroups()}[.
     *
     * @param idx the index into the list
     * @return key group id at the given index
     */
    // 按索引获取键组 ID。
    // 提供对列表中 Key Group ID 的随机访问。
    // idx 是列表中的索引（范围从 0 到 getNumberOfKeyGroups() - 1）。
    // 该方法返回位于该索引处的 Key Group ID。由于列表是有序的，可以通过索引快速定位。
    int getKeyGroupId(int idx);

    /** Returns true, if the given key group id is contained in the list, otherwise false. */
    // 检查是否包含键组 ID
    boolean contains(int keyGroupId);
}
