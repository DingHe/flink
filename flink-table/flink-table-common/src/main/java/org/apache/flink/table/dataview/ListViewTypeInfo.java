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

package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.table.api.dataview.ListView;

/**
 * Type information for {@link ListView}.
 *
 * @param <T> element type
 */
//Flink 中遗留的 TypeInformation 实现，专门用于描述 ListView 类型。
// 它的主要作用是在 Flink 的旧版类型系统中为 ListView 提供必要的类型元信息，
// 包括如何创建序列化器、如何进行类型比较以及如何处理泛型等，以确保 ListView 能够在 Flink 运行时中被正确地处理
@Internal
@Deprecated
public class ListViewTypeInfo<T> extends TypeInformation<ListView<T>> {

    private static final long serialVersionUID = 6468505781419989441L;
    //存储 ListView 中元素的 TypeInformation
    private final TypeInformation<T> elementType;
    //用于指示是否应该使用一个特殊的“空序列化器”（NullSerializer）
    //当设置为 true 时，createSerializer 方法会返回一个 NullSerializer，这通常意味着 ListView 对象本身不需要被序列化，它的状态将由 Flink 的状态后端直接管理
    private boolean nullSerializer;

    public ListViewTypeInfo(TypeInformation<T> elementType, boolean nullSerializer) {
        this.elementType = elementType;
        this.nullSerializer = nullSerializer;
    }

    public ListViewTypeInfo(TypeInformation<T> elementType) {
        this(elementType, false);
    }

    public TypeInformation<T> getElementType() {
        return elementType;
    }

    public boolean isNullSerializer() {
        return nullSerializer;
    }

    public void setNullSerializer(boolean nullSerializer) {
        this.nullSerializer = nullSerializer;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<ListView<T>> getTypeClass() {
        return (Class<ListView<T>>) (Class<?>) ListView.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeSerializer<ListView<T>> createSerializer(SerializerConfig config) {
        if (nullSerializer) {
            return (TypeSerializer<ListView<T>>) (TypeSerializer<?>) NullSerializer.INSTANCE;
        } else {
            TypeSerializer<T> elementSerializer = elementType.createSerializer(config);
            return new ListViewSerializer<>(new ListSerializer<>(elementSerializer));
        }
    }

    @Override
    public TypeSerializer<ListView<T>> createSerializer(ExecutionConfig config) {
        return createSerializer(config.getSerializerConfig());
    }

    @Override
    public String toString() {
        return "ListView<" + elementType + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ListViewTypeInfo) {
            //noinspection unchecked
            ListViewTypeInfo<T> other = (ListViewTypeInfo<T>) obj;
            return elementType.equals(other.elementType) && nullSerializer == other.nullSerializer;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31 * elementType.hashCode() + Boolean.hashCode(nullSerializer);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }
}
