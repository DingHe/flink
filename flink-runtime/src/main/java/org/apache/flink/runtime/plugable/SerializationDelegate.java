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

package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * The serialization delegate exposes an arbitrary element as a {@link IOReadableWritable} for
 * serialization, with the help of a type serializer.
 *
 * @param <T> The type to be represented as an IOReadableWritable.
 */
// SerializationDelegate<T> 的核心作用是将任意类型的 Flink 数据对象 <T> 包装起来，使其能够被视为一个实现了 IOReadableWritable 接口的对象。
// 在 Flink 的 I/O 子系统（例如网络传输、磁盘溢写）中，数据通常需要以字节流的形式读写，并且许多底层组件（如 RecordWriter、RecordDeserializer）需要处理的对象都要求实现 IOReadableWritable 接口。
// SerializationDelegate 的作用就是作为 T 类型对象和底层 I/O 系统的“中介”：
// 适配器（Adapter）: 它实现了 IOReadableWritable 接口，但将实际的序列化/反序列化工作委托给专门的 TypeSerializer<T>。
public class SerializationDelegate<T> implements IOReadableWritable {

    private T instance;

    private final TypeSerializer<T> serializer;

    public SerializationDelegate(TypeSerializer<T> serializer) {
        this.serializer = serializer;
    }

    public void setInstance(T instance) {
        this.instance = instance;
    }

    public T getInstance() {
        return this.instance;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        this.serializer.serialize(this.instance, out);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new IllegalStateException("Deserialization method called on SerializationDelegate.");
    }
}
