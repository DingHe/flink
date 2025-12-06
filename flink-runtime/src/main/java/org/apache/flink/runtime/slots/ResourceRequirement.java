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

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** Represents the number of required resources for a specific {@link ResourceProfile}. */
// 核心作用是将特定的资源配置 (ResourceProfile) 与所需槽位的数量 (numberOfRequiredSlots) 关联起来。
// 资源请求的载体： 在 Flink 的声明式资源管理中，JobMaster 通过 ResourceRequirement 列表向 ResourceManager 传达：“我需要 $N$ 个具有 $X$ 配置（CPU/内存等）的槽位。”
// 不可变性： 它是一个不可变的（Immutable）值对象，一旦创建，其资源配置和所需数量就不能更改。
public class ResourceRequirement implements Serializable {

    private static final long serialVersionUID = 1L;
    // 资源配置概要。
    // 定义了所需要的槽位的资源规格（例如：1.0 CPU 核，4GB 内存，GPU 数量等）。
    private final ResourceProfile resourceProfile;
    // 所需槽位数量。
    // 定义了 JobMaster 需要多少个具有上述 resourceProfile 规格的槽位。在构造时必须大于 0。
    private final int numberOfRequiredSlots;

    private ResourceRequirement(ResourceProfile resourceProfile, int numberOfRequiredSlots) {
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkArgument(numberOfRequiredSlots > 0);

        this.resourceProfile = resourceProfile;
        this.numberOfRequiredSlots = numberOfRequiredSlots;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public int getNumberOfRequiredSlots() {
        return numberOfRequiredSlots;
    }

    public static ResourceRequirement create(
            ResourceProfile resourceProfile, int numberOfRequiredSlots) {
        return new ResourceRequirement(resourceProfile, numberOfRequiredSlots);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceRequirement that = (ResourceRequirement) o;
        return numberOfRequiredSlots == that.numberOfRequiredSlots
                && Objects.equals(resourceProfile, that.resourceProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceProfile, numberOfRequiredSlots);
    }

    @Override
    public String toString() {
        return "ResourceRequirement{"
                + "resourceProfile="
                + resourceProfile
                + ", numberOfRequiredSlots="
                + numberOfRequiredSlots
                + '}';
    }
}
