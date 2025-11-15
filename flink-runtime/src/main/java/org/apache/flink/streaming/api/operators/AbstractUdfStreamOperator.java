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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import static java.util.Objects.requireNonNull;

/**
 * This is used as the base class for operators that have a user-defined function. This class
 * handles the opening and closing of the user-defined functions, as part of the operator life
 * cycle.
 *
 * @param <OUT> The output type of the operator
 * @param <F> The type of the user function，UserFunctionProvider接口可以获取用户定义的函数，OutputTypeConfigurable设置输出类型
 */
// 核心作用是为包含用户自定义函数（UDF，User-Defined Function） 的算子提供一个通用的模板和基础设施
// 管理 UDF 的生命周期：它将 Function 接口的 open() 和 close() 方法与算子的生命周期 (AbstractStreamOperator 的 open() 和 close()) 关联起来，
// 确保用户函数在算子启动时被初始化，在算子结束时被正确关闭
// 集成 UDF 的状态管理：它负责将用户函数中实现的状态接口（如 CheckpointedFunction 或 ListCheckpointed）与 Flink 的底层状态管理和检查点机制集成起来，
// 确保用户函数的状态能够在检查点时被正确保存和恢复
// 提供运行时上下文：它将 StreamingRuntimeContext 传递给用户函数，让用户函数能够访问任务配置、度量、状态后端等运行时信息
@PublicEvolving
public abstract class AbstractUdfStreamOperator<OUT, F extends Function>
        extends AbstractStreamOperator<OUT>
        implements OutputTypeConfigurable<OUT>, UserFunctionProvider<F> {

    private static final long serialVersionUID = 1L;

    /** The user function. */
    //存储用户自定义函数的实例
    protected final F userFunction;

    public AbstractUdfStreamOperator(F userFunction) {
        this.userFunction = requireNonNull(userFunction);
        checkUdfCheckpointingPreconditions();
    }

    /**
     * Gets the user function executed in this operator.
     *
     * @return The user function of this operator.
     */
    public F getUserFunction() {
        return userFunction;
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());
    }
    // checkpoint barriers 会调用（异步）snapshotState() 方法触发 checkpoint
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), userFunction);
    }
    //initializeState() 既包含在初始化过程中算子状态的初始化逻辑（比如注册 keyed 状态），又包含异常后从 checkpoint 中恢复原有状态的逻辑
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        StreamingFunctionUtils.restoreFunctionState(context, userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(userFunction, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (userFunction instanceof SinkFunction) {
            ((SinkFunction<?>) userFunction).finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(userFunction);
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    // ------------------------------------------------------------------------
    //  Output type configuration
    // ------------------------------------------------------------------------

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
        StreamingFunctionUtils.setOutputType(userFunction, outTypeInfo, executionConfig);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Since the streaming API does not implement any parametrization of functions via a
     * configuration, the config returned here is actually empty.
     *
     * @return The user function parameters (currently empty)
     */
    public Configuration getUserFunctionParameters() {
        return new Configuration();
    }

    private void checkUdfCheckpointingPreconditions() {

        if (userFunction instanceof CheckpointedFunction
                && userFunction instanceof ListCheckpointed) {

            throw new IllegalStateException(
                    "User functions are not allowed to implement "
                            + "CheckpointedFunction AND ListCheckpointed.");
        }
    }
}
