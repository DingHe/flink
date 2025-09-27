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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;

/** The builder class for {@link OperatorAttributes}. */
@Experimental
public class OperatorAttributesBuilder {
    private boolean outputOnlyAfterEndOfStream = false; //一个布尔值，表示运算符是否仅在所有输入流结束之后才会发出记录。默认值为 false
    private boolean internalSorterSupported = false; //一个布尔值，表示运算符是否支持内部排序。默认值为 false

    /**
     * Set to true if and only if the operator only emits records after all its inputs have ended.
     * If it is not set, the default value false is used.
     */
    public OperatorAttributesBuilder setOutputOnlyAfterEndOfStream(
            boolean outputOnlyAfterEndOfStream) {
        this.outputOnlyAfterEndOfStream = outputOnlyAfterEndOfStream;
        return this;
    }

    public OperatorAttributesBuilder setInternalSorterSupported(boolean internalSorterSupported) {
        this.internalSorterSupported = internalSorterSupported;
        return this;
    }

    public OperatorAttributes build() {
        return new OperatorAttributes(outputOnlyAfterEndOfStream, internalSorterSupported);
    }
}
