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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverhead;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;

/**
 * Describe the specifics of different resource dimensions of the JobManager process.
 *
 * <p>A JobManager's memory consists of the following components:
 *
 * <ul>
 *   <li>JVM Heap Memory
 *   <li>Off-heap Memory
 *   <li>JVM Metaspace
 *   <li>JVM Overhead
 * </ul>
 *
 * We use Total Process Memory to refer to all the memory components, while Total Flink Memory
 * refering to all the components except JVM Metaspace and JVM Overhead.
 *
 * <p>The relationships of JobManager memory components are shown below.
 *
 * <pre>
 *               ┌ ─ ─ Total Process Memory  ─ ─ ┐
 *                ┌ ─ ─ Total Flink Memory  ─ ─ ┐
 *               │ ┌───────────────────────────┐ │
 *  On-Heap ----- ││      JVM Heap Memory      ││
 *               │ └───────────────────────────┘ │
 *               │ ┌───────────────────────────┐ │
 *            ┌─  ││       Off-heap Memory     ││
 *            │  │ └───────────────────────────┘ │
 *            │   └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *            │  │┌─────────────────────────────┐│
 *  Off-Heap ─|   │        JVM Metaspace        │
 *            │  │└─────────────────────────────┘│
 *            │   ┌─────────────────────────────┐
 *            └─ ││        JVM Overhead         ││
 *                └─────────────────────────────┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
// JobManagerProcessSpec 是 Flink 内存模型中专门为 JobManager (JM) 进程定义的内存规格描述类。
// 简单来说，它就是 JobManager 内存配置的最终成品清单。
// 主要作用是描述和持有 JobManager 进程在各个资源维度上的具体内存数值。
// $$Total\ Process\ Memory = JVM\ Heap + Off\text{-}heap + Metaspace + Overhead$$
public class JobManagerProcessSpec extends CommonProcessMemorySpec<JobManagerFlinkMemory> {
    private static final long serialVersionUID = 1L;

    JobManagerProcessSpec(
            JobManagerFlinkMemory flinkMemory, JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
        super(flinkMemory, jvmMetaspaceAndOverhead);
    }

    @VisibleForTesting
    public JobManagerProcessSpec(
            MemorySize jvmHeapSize,
            MemorySize offHeapSize,
            MemorySize jvmMetaspaceSize,
            MemorySize jvmOverheadSize) {
        this(
                new JobManagerFlinkMemory(jvmHeapSize, offHeapSize),
                new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize));
    }

    @Override
    public String toString() {
        return "JobManagerProcessSpec {"
                + "jvmHeapSize="
                + getJvmHeapMemorySize().toHumanReadableString()
                + ", "
                + "offHeapSize="
                + getJvmDirectMemorySize().toHumanReadableString()
                + ", "
                + "jvmMetaspaceSize="
                + getJvmMetaspaceSize().toHumanReadableString()
                + ", "
                + "jvmOverheadSize="
                + getJvmOverheadSize().toHumanReadableString()
                + '}';
    }
}
