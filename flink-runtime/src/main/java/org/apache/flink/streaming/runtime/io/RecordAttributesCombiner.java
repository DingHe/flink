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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/** RecordAttributesValve combine RecordAttributes from different input channels. */
// 于处理和协调来自多个输入通道的记录属性（RecordAttributes）
// 主要作用是聚合（Combine）来自 Flink 任务所有输入通道的 RecordAttributes，并基于这些聚合属性，向下游任务统一发出一个代表整体状态的 RecordAttributes
// 在 Flink 3.0 中引入 RecordAttributes 主要是为了支持动态分区（Dynamic Partitioning）和背压信息传递等场景。其中一个关键属性是 isBacklog，它指示上游是否有积压数据（Backlog）正在处理。
// 木桶原理（Backlog 模式）： 如果任何一个输入通道报告自己正在处理积压数据（isBacklog = true），那么整个 Task 的输出 RecordAttributes 就必须报告正在处理积压数据（isBacklog = true）
// 简而言之，它是一个阀门或协调器，用于在多输入流场景下，以统一的、保守的策略，将控制层面的记录属性（尤其是积压状态）传递给下游。

public class RecordAttributesCombiner {

    private static final Logger LOG = LoggerFactory.getLogger(RecordAttributesCombiner.class);
    // 所有通道的记录属性数组。
    // 数组大小等于输入通道数。存储每个输入通道最新收到的 RecordAttributes。初始化时元素为 null。
    private final RecordAttributes[] allChannelRecordAttributes;
    // 正在处理积压数据的通道计数
    private int backlogChannelCnt = 0;
    // 积压状态未定义（未收到属性）的通道计数。 记录尚未收到任何 RecordAttributes 的输入通道数量
    private int backlogUndefinedChannelCnt;
    // 上次输出的聚合属性。 存储上次成功向下游发射的聚合 RecordAttributes
    private RecordAttributes lastOutputAttributes = null;

    public RecordAttributesCombiner(int numInputChannels) {
        this.backlogUndefinedChannelCnt = numInputChannels;
        this.allChannelRecordAttributes = new RecordAttributes[numInputChannels];
    }

    public void inputRecordAttributes(
            RecordAttributes recordAttributes, int channelIdx, DataOutput<?> output)
            throws Exception {
        LOG.debug("RecordAttributes: {} from channel idx: {}", recordAttributes, channelIdx);
        RecordAttributes lastChannelRecordAttributes = allChannelRecordAttributes[channelIdx];
        allChannelRecordAttributes[channelIdx] = recordAttributes;

        // skip if the input RecordAttributes of the input channel is the same as the last.
        if (recordAttributes.equals(lastChannelRecordAttributes)) {
            return;
        }

        final RecordAttributesBuilder builder =
                new RecordAttributesBuilder(Collections.emptyList());

        Boolean isBacklog = combineIsBacklog(lastChannelRecordAttributes, recordAttributes);
        if (isBacklog == null) {
            if (lastOutputAttributes == null) {
                return;
            } else {
                isBacklog = lastOutputAttributes.isBacklog();
            }
        }
        builder.setBacklog(isBacklog);

        final RecordAttributes outputAttribute = builder.build();
        if (!outputAttribute.equals(lastOutputAttributes)) {
            output.emitRecordAttributes(outputAttribute);
            lastOutputAttributes = outputAttribute;
        }
    }

    /**
     * If any of the input channels is backlog, the combined RecordAttributes is backlog. Return
     * null if the isBacklog cannot be determined, i.e. when none of the input channel is processing
     * backlog and some input channels are undefined.
     */
    private Boolean combineIsBacklog(
            RecordAttributes lastRecordAttributes, RecordAttributes recordAttributes) {

        if (lastRecordAttributes == null) {
            backlogUndefinedChannelCnt--;
            if (recordAttributes.isBacklog()) {
                backlogChannelCnt++;
            }
        } else if (lastRecordAttributes.isBacklog() != recordAttributes.isBacklog()) {
            if (recordAttributes.isBacklog()) {
                backlogChannelCnt++;
            } else {
                backlogChannelCnt--;
            }
        }

        // The input is processing backlog if any channel is processing backlog
        if (backlogChannelCnt > 0) {
            return true;
        }

        // None of the input channel is processing backlog and some are undefined
        if (backlogUndefinedChannelCnt > 0) {
            return null;
        }

        // All the input channels are defined and not processing backlog
        return false;
    }
}
