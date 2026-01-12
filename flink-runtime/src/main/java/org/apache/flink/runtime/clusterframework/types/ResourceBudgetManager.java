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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Budget manager for {@link ResourceProfile}.
 *
 * <p>For a given total resource budget, this class handles reserving and releasing resources from
 * the budget, and rejects reservations if they cannot be satisfied by the remaining budget.
 *
 * <p>Both the total budget and the reservations are in the form of {@link ResourceProfile}.
 */
// ResourceBudgetManager 是一个非常精简但核心的资源计数器。如果说 TaskSlotTable 是账本，那么 ResourceBudgetManager 就是账本上的余额计算器
// 主要作用是对 TaskManager 的总资源进行预演和额度管控。
// 在 Flink 1.10+ 版本引入细粒度资源管理（Fine-grained Resource Management）后，Slot 的大小不再是固定统一的，而是可以根据 ResourceProfile（包含 CPU、内存、GPU 等多个维度）动态申请。
// 额度校验：在真正分配 Slot 之前，判断剩余的 CPU 和内存是否还能满足本次申请。
// 原子更新：通过减法（预留）和加法（释放）实时维护可用资源的快照。
// 防止超卖：确保 TaskManager 内部的所有 Slot 资源总和永远不会超过配置的最大物理限额。
public class ResourceBudgetManager {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceBudgetManager.class);
    // 总预算资源。
    // 代表该 TaskManager 节点能够提供给所有 Slot 使用的资源总上限。它在构造时确定，之后不可更改。
    private final ResourceProfile totalBudget;
    // 当前可用资源。
    // 每当有一个 Slot 被成功分配时，它会减少；当 Slot 被释放时，它会增加。它是决策“能否再开一个 Slot”的直接依据。
    private ResourceProfile availableBudget;

    public ResourceBudgetManager(final ResourceProfile totalBudget) {
        checkResourceProfileNotNullOrUnknown(totalBudget);
        this.totalBudget = totalBudget;
        this.availableBudget = totalBudget;
    }

    public ResourceProfile getTotalBudget() {
        return totalBudget;
    }

    public ResourceProfile getAvailableBudget() {
        return availableBudget;
    }
    // 预留/扣减资源。
    public boolean reserve(final ResourceProfile reservation) {
        checkResourceProfileNotNullOrUnknown(reservation);
        // 逐个字段检查（CPU 是否够？内存是否够？网络资源是否够？）
        // 如果任一维度不足：返回 false，表示资源不足，拒绝分配。
        if (!availableBudget.allFieldsNoLessThan(reservation)) {
            return false;
        }
        // 如果资源充足：执行减法 availableBudget.subtract(reservation)，更新余额，返回 true。
        availableBudget = availableBudget.subtract(reservation);
        LOG.debug("Resource budget reduced to {}.", availableBudget);
        return true;
    }
    // 释放/归还资源。
    public boolean release(final ResourceProfile reservation) {
        // 使用 merge(reservation) 将归还的资源加回到可用余额中。
        checkResourceProfileNotNullOrUnknown(reservation);
        ResourceProfile newAvailableBudget = availableBudget.merge(reservation);
        // 检查加回后的余额是否超过了 totalBudget。如果超过了，说明归还的逻辑存在 Bug（即归还了比总数还多的资源），此时会返回 false。
        if (!totalBudget.allFieldsNoLessThan(newAvailableBudget)) {
            return false;
        }

        availableBudget = newAvailableBudget;
        LOG.debug("Resource budget increased to {}.", availableBudget);
        return true;
    }
    // 防御性编程校验。
    private static void checkResourceProfileNotNullOrUnknown(
            final ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkArgument(!resourceProfile.equals(ResourceProfile.UNKNOWN));
    }
}
