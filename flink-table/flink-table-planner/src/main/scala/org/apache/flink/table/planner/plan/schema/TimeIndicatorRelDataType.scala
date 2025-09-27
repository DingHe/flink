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
package org.apache.flink.table.planner.plan.schema

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.sql.`type`.BasicSqlType
import org.apache.calcite.sql.`type`.SqlTypeName.TIMESTAMP

import java.lang

/** 表示时间指示符：它标识时间戳字段是事件时间（ROWTIME）还是处理时间（PROCTIME），作为 BasicSqlType 的子类，它可以与 SQL 类型系统进行交互，允许 Flink 在内部进行时间类型的处理和优化
 * Creates a time indicator type for event-time or processing-time, but with similar properties as a
 * basic SQL type.  支持类型的序列化和比较：通过重写 toString() 和 hashCode() 方法，该类确保时间指示符类型在 Flink 的查询计划中有独特的标识，并支持类型的比较与序列化
 */
class TimeIndicatorRelDataType(
    val typeSystemField: RelDataTypeSystem, //该字段表示所使用的类型系统，通常是 Calcite SQL 类型系统
    val originalType: BasicSqlType, //该字段表示原始的 SQL 类型，用于从基础的 SQL 类型（如 TIMESTAMP）初始化该类型
    val nullable: Boolean,
    val isEventTime: Boolean) //该字段指示当前时间指示符是否为事件时间。如果为 true，表示是事件时间（ROWTIME），否则为处理时间（PROCTIME）
  extends BasicSqlType(typeSystemField, originalType.getSqlTypeName, originalType.getPrecision) {

  this.isNullable = nullable
  computeDigest()

  override def hashCode(): Int = {
    super.hashCode() + 42 // we change the hash code to differentiate from regular timestamps
  }

  override def toString: String = {
    // Calcite caches type instance by the type string representation in
    // org.apache.calcite.rel.type.RelDataTypeFactoryImpl, thus we use
    // unique name for each TimeIndicatorRelDataType
    s"${if (typeName == TIMESTAMP) "TIMESTAMP(3)" else "TIMESTAMP_LTZ(3)"}" +
      s" ${if (isEventTime) "*ROWTIME*" else "*PROCTIME*"}"
  }

  override def generateTypeString(sb: lang.StringBuilder, withDetail: Boolean): Unit = {
    sb.append(toString)
  }
}
