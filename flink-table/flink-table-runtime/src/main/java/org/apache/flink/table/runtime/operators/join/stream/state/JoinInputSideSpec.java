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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link JoinInputSideSpec} is ap specification which describes input side information of a
 * Join.
 */
// JoinInputSideSpec 类是 Apache Flink Table API/SQL 流式 Join 算子（特别是涉及到状态维护的 Join，如双流 Join）的输入侧规范描述。
// 核心作用是承载关于 Join 操作某个输入流（左侧或右侧）的唯一性（Unique Key）信息。
// 在流式 Join 中，尤其是当 Join 的结果需要支持撤回（Retraction）和更新语义时，准确知道一个输入流是否存在唯一键以及该唯一键是否包含在 Join 键中至关重要。
// 利用唯一键信息，Flink 运行时可以：
// 优化状态存储： 如果输入侧存在唯一键，则可以确保 Join 状态中不会出现重复的 Row，从而简化状态管理。
// 处理数据更新： 当接收到一条新的数据时，如果知道它拥有唯一键，并且 Join 键也包含这个唯一键，算子可以高效地识别和更新 Join 状态中对应的旧记录，
// 实现高效的更新（Update）和删除（Delete）操作，这对于处理变更日志流（Changelog Stream）至关重要。
public class JoinInputSideSpec implements Serializable {
    private static final long serialVersionUID = 3178408082297179959L;
    // 输入侧是否有唯一键。
    // 如果该输入流的 Schema 中定义了唯一键，则为 true。
    // 它是在构造函数中根据 uniqueKeyType 和 uniqueKeySelector 是否为 null 计算得出的。
    private final boolean inputSideHasUniqueKey;
    // Join 键是否包含唯一键。
    // 如果用于 Join 的键（Join Key）是该输入流唯一键的超集或相等，则为 true。
    // 这对于识别更新非常重要。
    private final boolean joinKeyContainsUniqueKey;
    // 唯一键的类型信息。
    // 如果输入侧存在唯一键，则存储其 Flink 内部类型信息 (InternalTypeInfo)；否则为 null
    @Nullable private final InternalTypeInfo<RowData> uniqueKeyType;
    // 唯一键提取器。
    // 一个 KeySelector 函数，用于从输入记录 (RowData) 中提取出唯一键。
    // 如果不存在唯一键，则为 null。
    @Nullable private final KeySelector<RowData, RowData> uniqueKeySelector;

    private JoinInputSideSpec(
            boolean joinKeyContainsUniqueKey,
            @Nullable InternalTypeInfo<RowData> uniqueKeyType,
            @Nullable KeySelector<RowData, RowData> uniqueKeySelector) {
        this.inputSideHasUniqueKey = uniqueKeyType != null && uniqueKeySelector != null;
        this.joinKeyContainsUniqueKey = joinKeyContainsUniqueKey;
        this.uniqueKeyType = uniqueKeyType;
        this.uniqueKeySelector = uniqueKeySelector;
    }

    /** Returns true if the input has unique key, otherwise false. */
    public boolean hasUniqueKey() {
        return inputSideHasUniqueKey;
    }

    /** Returns true if the join key contains the unique key of the input. */
    public boolean joinKeyContainsUniqueKey() {
        return joinKeyContainsUniqueKey;
    }

    /**
     * Returns the {@link TypeInformation} of the unique key. Returns null if the input hasn't
     * unique key.
     */
    @Nullable
    public InternalTypeInfo<RowData> getUniqueKeyType() {
        return uniqueKeyType;
    }

    /**
     * Returns the {@link KeySelector} to extract unique key from the input row. Returns null if the
     * input hasn't unique key.
     */
    @Nullable
    public KeySelector<RowData, RowData> getUniqueKeySelector() {
        return uniqueKeySelector;
    }

    /**
     * Creates a {@link JoinInputSideSpec} that the input has an unique key.
     *
     * @param uniqueKeyType type information of the unique key
     * @param uniqueKeySelector key selector to extract unique key from the input row
     */
    public static JoinInputSideSpec withUniqueKey(
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector) {
        checkNotNull(uniqueKeyType);
        checkNotNull(uniqueKeySelector);
        return new JoinInputSideSpec(false, uniqueKeyType, uniqueKeySelector);
    }

    /**
     * Creates a {@link JoinInputSideSpec} that input has an unique key and the unique key is
     * contained by the join key.
     *
     * @param uniqueKeyType type information of the unique key
     * @param uniqueKeySelector key selector to extract unique key from the input row
     */
    public static JoinInputSideSpec withUniqueKeyContainedByJoinKey(
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector) {
        checkNotNull(uniqueKeyType);
        checkNotNull(uniqueKeySelector);
        return new JoinInputSideSpec(true, uniqueKeyType, uniqueKeySelector);
    }

    /** Creates a {@link JoinInputSideSpec} that input hasn't any unique keys. */
    public static JoinInputSideSpec withoutUniqueKey() {
        return new JoinInputSideSpec(false, null, null);
    }

    @Override
    public String toString() {
        if (inputSideHasUniqueKey) {
            if (joinKeyContainsUniqueKey) {
                return "JoinKeyContainsUniqueKey";
            } else {
                return "HasUniqueKey";
            }
        } else {
            return "NoUniqueKey";
        }
    }
}
