/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.PinotDataType;


public class FunctionUtils {
  private FunctionUtils() {
  }

  // Types allowed as the function parameter (in the function signature) for type conversion
  private static final Map<Class<?>, PinotDataType> PARAMETER_TYPE_MAP = new HashMap<>() {{
    put(int.class, PinotDataType.INTEGER);
    put(Integer.class, PinotDataType.INTEGER);
    put(long.class, PinotDataType.LONG);
    put(Long.class, PinotDataType.LONG);
    put(float.class, PinotDataType.FLOAT);
    put(Float.class, PinotDataType.FLOAT);
    put(double.class, PinotDataType.DOUBLE);
    put(Double.class, PinotDataType.DOUBLE);
    put(BigDecimal.class, PinotDataType.BIG_DECIMAL);
    put(boolean.class, PinotDataType.BOOLEAN);
    put(Boolean.class, PinotDataType.BOOLEAN);
    put(Timestamp.class, PinotDataType.TIMESTAMP);
    put(String.class, PinotDataType.STRING);
    put(byte[].class, PinotDataType.BYTES);
    put(int[].class, PinotDataType.PRIMITIVE_INT_ARRAY);
    put(long[].class, PinotDataType.PRIMITIVE_LONG_ARRAY);
    put(float[].class, PinotDataType.PRIMITIVE_FLOAT_ARRAY);
    put(double[].class, PinotDataType.PRIMITIVE_DOUBLE_ARRAY);
    put(BigDecimal[].class, PinotDataType.BIG_DECIMAL_ARRAY);
    put(boolean[].class, PinotDataType.PRIMITIVE_BOOLEAN_ARRAY);
    put(Timestamp[].class, PinotDataType.TIMESTAMP_ARRAY);
    put(String[].class, PinotDataType.STRING_ARRAY);
    put(byte[][].class, PinotDataType.BYTES_ARRAY);
    put(Map.class, PinotDataType.MAP);
    put(Object.class, PinotDataType.OBJECT);
  }};

  private static final Map<Class<?>, ColumnDataType> COLUMN_DATA_TYPE_MAP = new HashMap<>() {{
    put(int.class, ColumnDataType.INT);
    put(Integer.class, ColumnDataType.INT);
    put(long.class, ColumnDataType.LONG);
    put(Long.class, ColumnDataType.LONG);
    put(float.class, ColumnDataType.FLOAT);
    put(Float.class, ColumnDataType.FLOAT);
    put(double.class, ColumnDataType.DOUBLE);
    put(Double.class, ColumnDataType.DOUBLE);
    put(BigDecimal.class, ColumnDataType.BIG_DECIMAL);
    put(boolean.class, ColumnDataType.BOOLEAN);
    put(Boolean.class, ColumnDataType.BOOLEAN);
    put(Timestamp.class, ColumnDataType.TIMESTAMP);
    put(String.class, ColumnDataType.STRING);
    put(byte[].class, ColumnDataType.BYTES);
    put(int[].class, ColumnDataType.INT_ARRAY);
    put(long[].class, ColumnDataType.LONG_ARRAY);
    put(float[].class, ColumnDataType.FLOAT_ARRAY);
    put(double[].class, ColumnDataType.DOUBLE_ARRAY);
    put(BigDecimal[].class, ColumnDataType.BIG_DECIMAL_ARRAY);
    put(boolean[].class, ColumnDataType.BOOLEAN_ARRAY);
    put(Timestamp[].class, ColumnDataType.TIMESTAMP_ARRAY);
    put(String[].class, ColumnDataType.STRING_ARRAY);
    put(byte[][].class, ColumnDataType.BYTES_ARRAY);
    put(Object.class, ColumnDataType.OBJECT);
  }};

  /**
   * Returns the corresponding PinotDataType for the given parameter class, or {@code null} if there is no one matching.
   */
  @Nullable
  public static PinotDataType getParameterType(Class<?> clazz) {
    return PARAMETER_TYPE_MAP.get(clazz);
  }

  /// Returns the corresponding [PinotDataType] for the given argument value (the actual value passed into
  /// the function). Returns [PinotDataType#OBJECT] / [PinotDataType#OBJECT_ARRAY] for unrecognized types,
  /// matching [PinotDataType#getSingleValueType]'s best-effort fallback. Subclasses of non-final types
  /// (e.g. vendor `Timestamp` subclasses returned by JDBC drivers) are matched by their parent type.
  ///
  /// Dispatch (single-value first since it's the dominant case for function arguments):
  /// - Single values → delegated to [PinotDataType#getSingleValueType] (covers all scalar types
  ///   including `byte[]` → [PinotDataType#BYTES]).
  /// - Reference arrays (`Object[]` and subtypes including `byte[][]`) → first non-null element is
  ///   sampled and [PinotDataType#getMultiValueType] is consulted. Empty / all-null reference arrays
  ///   fall back to [PinotDataType#OBJECT_ARRAY] since the element type is undeterminable.
  /// - Primitive arrays (`int[]` / `long[]` / `float[]` / `double[]` / `boolean[]`) → handled here, since
  ///   they can't be element-sampled into a boxed type.
  /// - [PinotDataType#COLLECTION] for any [Collection]; otherwise falls back to [PinotDataType#OBJECT].
  public static PinotDataType getArgumentType(Object value) {
    PinotDataType singleValueType = PinotDataType.getSingleValueType(value);
    if (singleValueType != PinotDataType.OBJECT) {
      return singleValueType;
    }
    if (value instanceof Object[]) {
      Object[] array = (Object[]) value;
      for (Object element : array) {
        if (element == null) {
          continue;
        }
        return PinotDataType.getMultiValueType(element);
      }
      // Empty or all-null reference array — element type undeterminable.
      return PinotDataType.OBJECT_ARRAY;
    }
    if (value instanceof int[]) {
      return PinotDataType.PRIMITIVE_INT_ARRAY;
    }
    if (value instanceof long[]) {
      return PinotDataType.PRIMITIVE_LONG_ARRAY;
    }
    if (value instanceof float[]) {
      return PinotDataType.PRIMITIVE_FLOAT_ARRAY;
    }
    if (value instanceof double[]) {
      return PinotDataType.PRIMITIVE_DOUBLE_ARRAY;
    }
    if (value instanceof boolean[]) {
      return PinotDataType.PRIMITIVE_BOOLEAN_ARRAY;
    }
    if (value instanceof Collection) {
      return PinotDataType.COLLECTION;
    }
    return PinotDataType.OBJECT;
  }

  /**
   * Returns the corresponding ColumnDataType for the given class, or {@code null} if there is no one matching.
   */
  @Nullable
  public static ColumnDataType getColumnDataType(Class<?> clazz) {
    return COLUMN_DATA_TYPE_MAP.get(clazz);
  }

  /**
   * Returns the corresponding RelDataType for the given class, or OTHER if there is no one matching.
   */
  public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, Class<?> clazz) {
    ColumnDataType columnDataType = getColumnDataType(clazz);
    if (columnDataType == null) {
      return typeFactory.createSqlType(SqlTypeName.OTHER);
    }
    switch (columnDataType) {
      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case BIG_DECIMAL:
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case STRING:
      case JSON:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case BYTES:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case INT_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      case LONG_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1);
      case FLOAT_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), -1);
      case DOUBLE_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
      case BIG_DECIMAL_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DECIMAL), -1);
      case BOOLEAN_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), -1);
      case TIMESTAMP_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), -1);
      case STRING_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
      case BYTES_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARBINARY), -1);
      default:
        return typeFactory.createSqlType(SqlTypeName.OTHER);
    }
  }

  public static boolean isAssertEnabled() {
    boolean assertEnabled = false;
    //CHECKSTYLE:OFF
    assert assertEnabled = true;
    //CHECKSTYLE:ON
    return assertEnabled;
  }
}
