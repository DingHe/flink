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

package org.apache.flink.runtime.util.bash;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Utility class for using java utilities in bash scripts. */
// BashJavaUtils 是一个非常特殊的工具类。
// 它不是为了在 Java 运行时被其他类调用，而是专门为 Flink 的 Shell 脚本（如 config.sh, jobmanager.sh, taskmanager.sh）提供计算支持的。
// 核心作用是作为 Bash 脚本与 Java 逻辑之间的桥梁。
// 由于 Flink 的内存模型非常复杂（涉及堆内、堆外、直接内存、托管内存、元空间等），单纯靠 Shell 脚本很难准确计算出 JVM 启动参数（如 -Xmx, -Xms, -XX:MaxDirectMemorySize）。
// 因此，Flink 采取了以下策略：
// 脚本调用 Java：Shell 脚本启动一个临时的 JVM 进程运行 BashJavaUtils。
// 计算参数：Java 代码利用 Flink 内部精确的内存计算逻辑（TaskExecutorProcessUtils 等）计算出结果。
// 结果回传：BashJavaUtils 将计算好的参数打印到控制台，Shell 脚本通过管道捕获这些带有特殊前缀（BASH_JAVA_UTILS_EXEC_RESULT:）的行，并将其用于启动真正的 JobManager 或 TaskManager 进程。


public class BashJavaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BashJavaUtils.class);
    // 标识符前缀。
    // Bash 脚本会过滤标准输出，只有带有这个前缀的行才会被解析为计算结果，其他的（如 Logback 输出的日志）会被忽略
    @VisibleForTesting public static final String EXECUTION_PREFIX = "BASH_JAVA_UTILS_EXEC_RESULT:";

    private BashJavaUtils() {}
    // 程序的入口
    public static void main(String[] args) throws FlinkException {
        checkArgument(args.length > 0, "Command not specified.");

        Command command = Command.valueOf(args[0]);
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
        List<String> output = runCommand(command, commandArgs);
        for (String outputLine : output) {
            System.out.println(EXECUTION_PREFIX + outputLine);
        }
    }
    // 根据传入的命令类型分发任务
    private static List<String> runCommand(Command command, String[] commandArgs)
            throws FlinkException {
        switch (command) {
            // 计算 TaskManager 的内存参数
            case GET_TM_RESOURCE_PARAMS:
                return getTmResourceParams(FlinkConfigLoader.loadConfiguration(commandArgs));
            // 计算 JobManager 的内存参数
            case GET_JM_RESOURCE_PARAMS:
                return getJmResourceParams(FlinkConfigLoader.loadConfiguration(commandArgs));
            // 加载并修改配置
            case UPDATE_AND_GET_FLINK_CONFIGURATION:
                return FlinkConfigLoader.loadAndModifyConfiguration(commandArgs);
            // 处理旧版配置文件的迁移。
            case MIGRATE_LEGACY_FLINK_CONFIGURATION_TO_STANDARD_YAML:
                return FlinkConfigLoader.migrateLegacyConfigurationToStandardYaml(commandArgs);
            default:
                // unexpected, Command#valueOf should fail if a unknown command is passed in
                throw new RuntimeException("Unexpected, something is wrong.");
        }
    }

    /**
     * Generate and print JVM parameters and dynamic configs of task executor resources. The last
     * two lines of the output should be JVM parameters and dynamic configs respectively.
     */
    private static List<String> getTmResourceParams(Configuration configuration) {
        Configuration configurationWithFallback =
                TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                        configuration, TaskManagerOptions.TOTAL_FLINK_MEMORY);
        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(configurationWithFallback);

        logTaskExecutorConfiguration(taskExecutorProcessSpec);

        return Arrays.asList(
                ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec),
                TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec));
    }

    /** Generate and print JVM parameters of Flink Master resources as one line. */
    @VisibleForTesting
    static List<String> getJmResourceParams(Configuration configuration) {
        JobManagerProcessSpec jobManagerProcessSpec =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                        configuration, JobManagerOptions.JVM_HEAP_MEMORY);

        logMasterConfiguration(jobManagerProcessSpec);

        return Arrays.asList(
                JobManagerProcessUtils.generateJvmParametersStr(
                        jobManagerProcessSpec, configuration),
                JobManagerProcessUtils.generateDynamicConfigsStr(jobManagerProcessSpec));
    }

    private static void logMasterConfiguration(JobManagerProcessSpec spec) {
        JobManagerFlinkMemory flinkMemory = spec.getFlinkMemory();
        LOG.info("Final Master Memory configuration:");
        LOG.info(
                "  Total Process Memory: {}",
                spec.getTotalProcessMemorySize().toHumanReadableString());
        LOG.info(
                "    Total Flink Memory: {}",
                flinkMemory.getTotalFlinkMemorySize().toHumanReadableString());
        LOG.info(
                "      JVM Heap:         {}",
                flinkMemory.getJvmHeapMemorySize().toHumanReadableString());
        LOG.info(
                "      Off-heap:         {}",
                flinkMemory.getJvmDirectMemorySize().toHumanReadableString());
        LOG.info("    JVM Metaspace:      {}", spec.getJvmMetaspaceSize().toHumanReadableString());
        LOG.info("    JVM Overhead:       {}", spec.getJvmOverheadSize().toHumanReadableString());
    }

    private static void logTaskExecutorConfiguration(TaskExecutorProcessSpec spec) {
        TaskExecutorFlinkMemory flinkMemory = spec.getFlinkMemory();
        MemorySize totalOffHeapMemory =
                flinkMemory.getManaged().add(flinkMemory.getJvmDirectMemorySize());
        LOG.info("Final TaskExecutor Memory configuration:");
        LOG.info(
                "  Total Process Memory:          {}",
                spec.getTotalProcessMemorySize().toHumanReadableString());
        LOG.info(
                "    Total Flink Memory:          {}",
                flinkMemory.getTotalFlinkMemorySize().toHumanReadableString());
        LOG.info(
                "      Total JVM Heap Memory:     {}",
                flinkMemory.getJvmHeapMemorySize().toHumanReadableString());
        LOG.info(
                "        Framework:               {}",
                flinkMemory.getFrameworkHeap().toHumanReadableString());
        LOG.info(
                "        Task:                    {}",
                flinkMemory.getTaskHeap().toHumanReadableString());
        LOG.info("      Total Off-heap Memory:     {}", totalOffHeapMemory.toHumanReadableString());
        LOG.info(
                "        Managed:                 {}",
                flinkMemory.getManaged().toHumanReadableString());
        LOG.info(
                "        Total JVM Direct Memory: {}",
                flinkMemory.getJvmDirectMemorySize().toHumanReadableString());
        LOG.info(
                "          Framework:             {}",
                flinkMemory.getFrameworkOffHeap().toHumanReadableString());
        LOG.info(
                "          Task:                  {}",
                flinkMemory.getTaskOffHeap().toHumanReadableString());
        LOG.info(
                "          Network:               {}",
                flinkMemory.getNetwork().toHumanReadableString());
        LOG.info(
                "    JVM Metaspace:               {}",
                spec.getJvmMetaspaceSize().toHumanReadableString());
        LOG.info(
                "    JVM Overhead:                {}",
                spec.getJvmOverheadSize().toHumanReadableString());
    }

    /** Commands that BashJavaUtils supports. */
    public enum Command {
        /** Get JVM parameters and dynamic configs of task executor resources. */
        // 计算 TaskManager 的内存参数
        GET_TM_RESOURCE_PARAMS,

        /** Get JVM parameters and dynamic configs of job manager resources. */
        // 计算 JobManager 的内存参数
        GET_JM_RESOURCE_PARAMS,

        /** Update and get configuration from conf file and dynamic configs of the FLINK cluster. */
        // 加载并修改配置
        UPDATE_AND_GET_FLINK_CONFIGURATION,

        /** Load configuration from legacy conf file and return standard yaml config file. */
        // 处理旧版配置文件的迁移。
        MIGRATE_LEGACY_FLINK_CONFIGURATION_TO_STANDARD_YAML
    }
}
