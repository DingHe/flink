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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * An abstract stub implementation for rich user-defined functions. Rich functions have additional
 * methods for initialization ({@link #open(OpenContext)}) and teardown ({@link #close()}), as well
 * as access to their runtime execution context via {@link #getRuntimeContext()}.
 */
//主要作用是提供一个默认实现和样板代码，简化了开发者编写 Rich UDF 的过程
//实现了 RichFunction 接口，并提供了 open()、close()、getRuntimeContext() 等方法的默认实现。这意味着如果你继承这个抽象类，你就不必自己实现这些方法的全部逻辑
@Public
public abstract class AbstractRichFunction implements RichFunction, Serializable {

    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------
    //  Runtime context access
    // --------------------------------------------------------------------------------------------

    private transient RuntimeContext runtimeContext;

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        this.runtimeContext = t;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        if (this.runtimeContext == null) {
            throw new IllegalStateException("The runtime context has not been initialized.");
        } else if (this.runtimeContext instanceof IterationRuntimeContext) {
            return (IterationRuntimeContext) this.runtimeContext;
        } else {
            throw new IllegalStateException("This stub is not part of an iteration step function.");
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Default life cycle methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void open(Configuration parameters) throws Exception {}

    @Override
    public void close() throws Exception {}
}
