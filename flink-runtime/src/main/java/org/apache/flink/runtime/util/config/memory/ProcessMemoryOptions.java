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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Common Flink's options to describe its JVM process memory configuration for JM or TM. */
// 在 Flink 复杂的内存计算逻辑中，为了能够通用地处理 JobManager (JM) 和 TaskManager (TM) 的内存推导，Flink 需要知道哪些配置项（ConfigOptions）是用来定义内存的。
// 这个类并不存储具体的内存数值（如 1024MB），而是存储配置键的定义。
// ProcessMemoryOptions 的核心作用是参数化内存计算模型。
public class ProcessMemoryOptions {
    // 存储必须定义的细粒度内存配置项列表
    private final List<ConfigOption<MemorySize>> requiredFineGrainedOptions;
    // 指向“Flink 总内存”的配置项 Key
    private final ConfigOption<MemorySize> totalFlinkMemoryOption;
    // 指向“进程总内存”的配置项 Key
    private final ConfigOption<MemorySize> totalProcessMemoryOption;
    // 封装了 JVM 元空间（Metaspace）和执行开销（Overhead）相关的配置项
    private final JvmMetaspaceAndOverheadOptions jvmOptions;

    public ProcessMemoryOptions(
            List<ConfigOption<MemorySize>> requiredFineGrainedOptions,
            ConfigOption<MemorySize> totalFlinkMemoryOption,
            ConfigOption<MemorySize> totalProcessMemoryOption,
            JvmMetaspaceAndOverheadOptions jvmOptions) {
        this.requiredFineGrainedOptions = new ArrayList<>(checkNotNull(requiredFineGrainedOptions));
        this.totalFlinkMemoryOption = checkNotNull(totalFlinkMemoryOption);
        this.totalProcessMemoryOption = checkNotNull(totalProcessMemoryOption);
        this.jvmOptions = checkNotNull(jvmOptions);
    }

    List<ConfigOption<MemorySize>> getRequiredFineGrainedOptions() {
        return Collections.unmodifiableList(requiredFineGrainedOptions);
    }

    ConfigOption<MemorySize> getTotalFlinkMemoryOption() {
        return totalFlinkMemoryOption;
    }

    ConfigOption<MemorySize> getTotalProcessMemoryOption() {
        return totalProcessMemoryOption;
    }

    JvmMetaspaceAndOverheadOptions getJvmOptions() {
        return jvmOptions;
    }
}
