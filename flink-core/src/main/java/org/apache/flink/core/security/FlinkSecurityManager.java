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

package org.apache.flink.core.security;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Permission;

/**
 * {@code FlinkSecurityManager} to control certain behaviors that can be captured by Java system
 * security manager. It can be used to control unexpected user behaviors that potentially impact
 * cluster availability, for example, it can warn or prevent user code from terminating JVM by
 * System.exit or halt by logging or throwing an exception. This does not necessarily prevent
 * malicious users who try to tweak security manager on their own, but more for being dependable
 * against user mistakes by gracefully handling them informing users rather than causing silent
 * unavailability.
 */
// FlinkSecurityManager 是一个专门定制的安全管理器，它继承自 Java 标准的 java.lang.SecurityManager。
// 其核心设计目标不是为了防止黑客攻击，而是为了增加集群的健壮性（Dependability），防止用户代码中的非预期行为（如误用 System.exit）导致整个 TaskManager 或集群节点意外下线。
// 主要作用可以概括为：拦截与管控 JVM 退出行为
// 防止用户误操作：在分布式环境中，如果用户在定义的 UDF（用户自定义函数）中写了一句 System.exit(0)，这会导致承载该任务的整个 TaskManager 进程直接退出，从而导致该节点上所有其他任务被迫中断。FlinkSecurityManager 可以拦截此类调用，改为报错或记录日志
// 优雅与强制退出的切换：它可以根据配置决定在 JVM 退出时是执行正常的钩子函数（System.exit），还是立即强制关停（Runtime.halt）
// 兼容现有的安全机制：它支持“链式调用”，即在执行 Flink 特有的安全检查后，依然会触发系统中原有的安全管理器检查。

public class FlinkSecurityManager extends SecurityManager {

    static final Logger LOG = LoggerFactory.getLogger(FlinkSecurityManager.class);

    /**
     * Security manager reference lastly set to system's security manager by public API. As system
     * security manager can be reset with another but still chain-called into this manager properly,
     * this reference may not be referenced by System.getSecurityManager, but we still need to
     * control runtime check behaviors such as monitoring exit from user code.
     */
    // 保存当前系统正在使用的 Flink 安全管理器实例。
    // 由于 JVM 只有一个 System.getSecurityManager()，这个引用方便在静态方法中直接操作管理器的状态
    private static FlinkSecurityManager flinkSecurityManager;
    // 在 Flink 设置自己的管理器之前，如果系统已经有一个管理器，Flink 会把它存起来，并在检查权限时委托给它，确保不破坏原有的安全策略
    private final SecurityManager originalSecurityManager;
    // 标记当前线程（以及其子线程）是否处于“被监控用户代码执行”的状态。
    // 只有当这个值为 true 时，调用 System.exit 才会被拦截。
    private final ThreadLocal<Boolean> monitorUserSystemExit = new InheritableThreadLocal<>();
    // 拦截模式。
    // 取值来自配置，如 DISABLED（不拦截）、LOG（打印警告日志但允许退出）、THROW（抛出异常禁止退出）
    private final ClusterOptions.UserSystemExitMode userSystemExitMode;
    // 布尔标志。如果为 true，当需要关停 JVM 时，会调用 Runtime.halt()（不执行关闭钩子，最快速度退出），否则调用正常的退出流程。
    private final boolean haltOnSystemExit;

    @VisibleForTesting
    FlinkSecurityManager(
            ClusterOptions.UserSystemExitMode userSystemExitMode, boolean haltOnSystemExit) {
        this(userSystemExitMode, haltOnSystemExit, System.getSecurityManager());
    }

    @VisibleForTesting
    FlinkSecurityManager(
            ClusterOptions.UserSystemExitMode userSystemExitMode,
            boolean haltOnSystemExit,
            SecurityManager originalSecurityManager) {
        this.userSystemExitMode = Preconditions.checkNotNull(userSystemExitMode);
        this.haltOnSystemExit = haltOnSystemExit;
        this.originalSecurityManager = originalSecurityManager;
    }

    /**
     * Instantiate FlinkUserSecurityManager from configuration. Return null if no security manager
     * check is needed, so that a caller can skip setting security manager avoiding runtime check
     * cost, if there is no security check set up already. Use {@link #setFromConfiguration} helper,
     * which handles disabled case.
     *
     * @param configuration to instantiate the security manager from
     * @return FlinkUserSecurityManager instantiated based on configuration. Return null if
     *     disabled.
     */
    // 根据 ClusterOptions（如 cluster.intercept-user-system-exit）判断是否需要启用安全管理器。
    // 如果配置为禁用且不需要强制 halt，则返回 null 以节省运行开销。
    @VisibleForTesting
    static FlinkSecurityManager fromConfiguration(Configuration configuration) {
        final ClusterOptions.UserSystemExitMode userSystemExitMode =
                configuration.get(ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT);

        boolean haltOnSystemExit = configuration.get(ClusterOptions.HALT_ON_FATAL_ERROR);

        // If no check is needed, return null so that caller can avoid setting security manager not
        // to incur any runtime cost.
        if (userSystemExitMode == ClusterOptions.UserSystemExitMode.DISABLED && !haltOnSystemExit) {
            return null;
        }
        LOG.info(
                "FlinkSecurityManager is created with {} user system exit mode and {} exit",
                userSystemExitMode,
                haltOnSystemExit ? "forceful" : "graceful");
        // Add more configuration parameters that need user security manager (currently only for
        // system exit).
        return new FlinkSecurityManager(userSystemExitMode, haltOnSystemExit);
    }
    // 读取配置并将其正式注册到 JVM 中（通过 System.setSecurityManager）
    public static void setFromConfiguration(Configuration configuration) {
        final FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        if (flinkSecurityManager != null) {
            try {
                System.setSecurityManager(flinkSecurityManager);
            } catch (Exception e) {
                throw new IllegalConfigurationException(
                        String.format(
                                "Could not register security manager due to no permission to "
                                        + "set a SecurityManager. Either update your existing "
                                        + "SecurityManager to allow the permission or do not use "
                                        + "security manager features (e.g., '%s: %s', '%s: %s')",
                                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT.key(),
                                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT.defaultValue(),
                                ClusterOptions.HALT_ON_FATAL_ERROR.key(),
                                ClusterOptions.HALT_ON_FATAL_ERROR.defaultValue()),
                        e);
            }
        }
        FlinkSecurityManager.flinkSecurityManager = flinkSecurityManager;
    }
    // 作用：开启当前线程的退出监控。Flink 会在执行用户代码（如 Map、Filter 算子）之前调用此方法。
    public static void monitorUserSystemExitForCurrentThread() {
        if (flinkSecurityManager != null) {
            flinkSecurityManager.monitorUserSystemExit();
        }
    }
    // 作用：关闭监控。在用户代码执行完毕，回到 Flink 框架代码后调用。
    public static void unmonitorUserSystemExitForCurrentThread() {
        if (flinkSecurityManager != null) {
            flinkSecurityManager.unmonitorUserSystemExit();
        }
    }
    // 逻辑：完全委托给 originalSecurityManager。Flink 本身不定义新的文件或网络权限，只做代理。
    @Override
    public void checkPermission(Permission perm) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(perm);
        }
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(perm, context);
        }
    }
    // 逻辑核心
    @Override
    public void checkExit(int status) {
        if (userSystemExitMonitored()) {
            switch (userSystemExitMode) {
                case DISABLED:
                    break;
                case LOG:
                    // Add exception trace log to help users to debug where exit came from.
                    LOG.warn(
                            "Exiting JVM with status {} is monitored: The system will exit due to this call.",
                            status,
                            new UserSystemExitException());
                    break;
                case THROW:
                    throw new UserSystemExitException();
                default:
                    // Must not happen if exhaustively handling all modes above. Logging as being
                    // already at exit path.
                    LOG.warn("No valid check exit mode configured: {}", userSystemExitMode);
            }
        }
        // As this security manager is current at outer most of the chain and it has exit guard
        // option, invoke inner security manager here after passing guard checking above, if any.
        if (originalSecurityManager != null) {
            originalSecurityManager.checkExit(status);
        }
        // At this point, exit is determined. Halt if defined, otherwise check ended, JVM will call
        // System.exit
        if (haltOnSystemExit) {
            Runtime.getRuntime().halt(status);
        }
    }

    @VisibleForTesting
    void monitorUserSystemExit() {
        monitorUserSystemExit.set(true);
    }

    @VisibleForTesting
    void unmonitorUserSystemExit() {
        monitorUserSystemExit.set(false);
    }

    @VisibleForTesting
    boolean userSystemExitMonitored() {
        return Boolean.TRUE.equals(monitorUserSystemExit.get());
    }

    /**
     * Use this method to circumvent the configured {@link FlinkSecurityManager} behavior, ensuring
     * that the current JVM process will always stop via System.exit() or
     * Runtime.getRuntime().halt().
     */
    public static void forceProcessExit(int exitCode) {
        // Unset ourselves to allow exiting in any case.
        System.setSecurityManager(null);
        if (flinkSecurityManager != null && flinkSecurityManager.haltOnSystemExit) {
            Runtime.getRuntime().halt(exitCode);
        } else {
            System.exit(exitCode);
        }
    }
}
