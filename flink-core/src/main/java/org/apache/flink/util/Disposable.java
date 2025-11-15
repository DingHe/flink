/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

/**
 * Interface for classes that can be disposed, i.e. that have a dedicated lifecycle step to
 * "destroy" the object. On reason for this is for example to release native resources. From this
 * point, the interface fulfills a similar purpose as the {@link java.io.Closeable} interface, but
 * sometimes both should be represented as isolated, independent lifecycle steps.
 */
// 为 Flink 需要显式执行销毁操作的类定义一个标准的生命周期步骤
public interface Disposable {

    /**
     * Disposes the object and releases all resources. After calling this method, calling any
     * methods on the object may result in undefined behavior.
     *
     * @throws Exception if something goes wrong during disposal.
     */
    // 销毁对象并释放所有资源。
    void dispose() throws Exception;
}
