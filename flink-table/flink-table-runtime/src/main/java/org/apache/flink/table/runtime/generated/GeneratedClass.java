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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.codesplit.JavaCodeSplitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper for generated class, defines a {@link #newInstance(ClassLoader)} method to get an
 * instance by reference objects easily.
 */
// GeneratedClass<T> 是 Flink Table/SQL 模块中用于封装动态生成的 Java 源代码及其运行时依赖的抽象基类。
// Flink Table/SQL 优化器为了提高执行效率，会将许多操作（如表达式计算、连接条件、聚合逻辑等）动态生成为 Java 源代码。
// GeneratedClass 充当了这些生成的代码和实际可执行类之间的桥梁和容器。
// 代码和依赖的封装： 存储生成的类的名称、源代码、代码分割后的源代码，以及运行时需要传入的引用对象 (references)。
// 动态编译与缓存： 负责在运行时（通常是算子启动时）将存储的源代码动态编译成 Java Class，并将编译后的类缓存起来，避免重复编译。
// 实例创建： 提供方便的方法 (newInstance)，使用缓存的 Class 和封装的引用对象来创建该生成的类的新实例，供 Flink 算子使用。
public abstract class GeneratedClass<T> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratedClass.class);
    // 生成的类的完整名称（包含包名）。用于编译和实例化。
    private final String className;
    // 生成的 Java 源代码。这是原始、未分割的代码字符串。
    private final String code;
    // 分割后的 Java 源代码。
    // 当原始代码过长时，JavaCodeSplitter 会将其分割成多个部分，以适应 JVM 的方法/成员长度限制。
    private final String splitCode;
    // 引用的对象数组。
    // 存储生成的代码在运行时需要依赖的外部对象实例（如序列化器、配置、用户函数等）。
    // 这些对象在实例化时会传递给生成的类的构造函数。
    private final Object[] references;
    // 已编译的类对象缓存。
    // 这是一个 transient 字段，意味着它在序列化时不被传输。它用于存储动态编译后的 Class 对象，避免在运行时重复编译。
    private transient Class<T> compiledClass;

    protected GeneratedClass(
            String className, String code, Object[] references, ReadableConfig config) {
        checkNotNull(className, "name must not be null");
        checkNotNull(code, "code must not be null");
        checkNotNull(references, "references must not be null");
        checkNotNull(config, "config must not be null");
        this.className = className;
        this.code = code;
        this.splitCode =
                code.isEmpty()
                        ? code
                        : JavaCodeSplitter.split(
                                code,
                                config.get(TableConfigOptions.MAX_LENGTH_GENERATED_CODE),
                                config.get(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE));
        this.references = references;
    }

    /** Create a new instance of this generated class. */
    public T newInstance(ClassLoader classLoader) {
        try {
            return compile(classLoader)
                    .getConstructor(Object[].class)
                    // Because Constructor.newInstance(Object... initargs), we need to load
                    // references into a new Object[], otherwise it cannot be compiled.
                    .newInstance(new Object[] {references});
        } catch (Throwable e) {
            throw new RuntimeException(
                    "Could not instantiate generated class '" + className + "'", e);
        }
    }

    @SuppressWarnings("unchecked")
    public T newInstance(ClassLoader classLoader, Object... args) {
        try {
            return (T) compile(classLoader).getConstructors()[0].newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not instantiate generated class '" + className + "'", e);
        }
    }

    /**
     * Compiles the generated code, the compiled class will be cached in the {@link GeneratedClass}.
     */
    public Class<T> compile(ClassLoader classLoader) {
        if (compiledClass == null) {
            // cache the compiled class
            try {
                // first try to compile the split code
                compiledClass = CompileUtils.compile(classLoader, className, splitCode);
            } catch (Throwable t) {
                // compile the original code as fallback
                LOG.warn("Failed to compile split code, falling back to original code", t);
                compiledClass = CompileUtils.compile(classLoader, className, code);
            }
        }
        return compiledClass;
    }

    public String getClassName() {
        return className;
    }

    public String getCode() {
        return code;
    }

    public Object[] getReferences() {
        return references;
    }

    public Class<T> getClass(ClassLoader classLoader) {
        return compile(classLoader);
    }
}
