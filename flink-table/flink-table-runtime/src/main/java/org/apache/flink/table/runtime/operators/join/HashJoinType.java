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

package org.apache.flink.table.runtime.operators.join;

/** Join type for hash table. */
// HashJoinType 是 Flink Table/SQL 运行时内部使用的 枚举（Enum） 类，用于定义基于 哈希表（Hash Table） 实现的连接（Hash Join）操作的具体类型和行为。
// 在执行 Hash Join 时，通常涉及两个阶段：
// 构建（Build）阶段： 将其中一个输入流（Build Side）的数据存储到一个哈希表中。
// 探测（Probe）阶段： 遍历另一个输入流（Probe Side）的数据，并在哈希表中查找匹配项
public enum HashJoinType {
    INNER, // 内连接	只输出匹配的记录。
    BUILD_OUTER, // 构建端外部连接，如果外部连接的外部表恰好是 Hash Join 的构建表，则使用此类型。用于确保未匹配的构建端记录也能输出。
    PROBE_OUTER, // 探测端外部连接， 如果外部连接的外部表恰好是 Hash Join 的探测表，则使用此类型。用于确保未匹配的探测端记录也能输出。
    FULL_OUTER, // 全外部连接	输出所有匹配和不匹配的记录。
    SEMI, // 半连接（左半连接），输出左表（Probe Side）中有匹配的记录，且每条记录最多输出一次。
    ANTI, // 反连接（左反连接）	输出左表（Probe Side）中没有匹配的记录。
    BUILD_LEFT_SEMI, // 构建端左半连接	仅在构建表是左表（且是半连接）时使用。输出构建表（左表）中有匹配的记录。
    BUILD_LEFT_ANTI; // 构建端左反连接	仅在构建表是左表（且是反连接）时使用。输出构建表（左表）中没有匹配的记录。

    public boolean isBuildOuter() {
        return this.equals(BUILD_OUTER) || this.equals(FULL_OUTER);
    }

    public boolean isProbeOuter() {
        return this.equals(PROBE_OUTER) || this.equals(FULL_OUTER);
    }

    public boolean leftSemiOrAnti() {
        return this.equals(SEMI) || this.equals(ANTI);
    }

    public boolean buildLeftSemiOrAnti() {
        return this.equals(BUILD_LEFT_SEMI) || this.equals(BUILD_LEFT_ANTI);
    }

    public boolean needSetProbed() {
        return isBuildOuter() || buildLeftSemiOrAnti();
    }

    public static HashJoinType of(boolean leftIsBuild, boolean leftOuter, boolean rightOuter) {
        if (leftOuter && rightOuter) {
            return FULL_OUTER;
        } else if (leftOuter) {
            return leftIsBuild ? BUILD_OUTER : PROBE_OUTER;
        } else if (rightOuter) {
            return leftIsBuild ? PROBE_OUTER : BUILD_OUTER;
        } else {
            return INNER;
        }
    }

    public static HashJoinType of(
            boolean leftIsBuild,
            boolean leftOuter,
            boolean rightOuter,
            boolean isSemi,
            boolean isAnti) {
        if (leftOuter && rightOuter) {
            return FULL_OUTER;
        } else if (leftOuter) {
            return leftIsBuild ? BUILD_OUTER : PROBE_OUTER;
        } else if (rightOuter) {
            return leftIsBuild ? PROBE_OUTER : BUILD_OUTER;
        } else if (isSemi) {
            return leftIsBuild ? BUILD_LEFT_SEMI : SEMI;
        } else if (isAnti) {
            return leftIsBuild ? BUILD_LEFT_ANTI : ANTI;
        } else {
            return INNER;
        }
    }
}
