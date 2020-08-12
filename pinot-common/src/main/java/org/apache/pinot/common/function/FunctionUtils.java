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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class FunctionUtils {
  private FunctionUtils() {
  }

  // Types allowed as the function parameter (in the function signature) for type conversion
  private static final Map<Class<?>, PinotDataType> PARAMETER_TYPE_MAP = new HashMap<Class<?>, PinotDataType>() {{
    put(int.class, PinotDataType.INTEGER);
    put(Integer.class, PinotDataType.INTEGER);
    put(long.class, PinotDataType.LONG);
    put(Long.class, PinotDataType.LONG);
    put(float.class, PinotDataType.FLOAT);
    put(Float.class, PinotDataType.FLOAT);
    put(double.class, PinotDataType.DOUBLE);
    put(Double.class, PinotDataType.DOUBLE);
    put(String.class, PinotDataType.STRING);
    put(byte[].class, PinotDataType.BYTES);
  }};

  // Types allowed as the function argument (actual value passed into the function) for type conversion
  private static final Map<Class<?>, PinotDataType> ARGUMENT_TYPE_MAP = new HashMap<Class<?>, PinotDataType>() {{
    put(Byte.class, PinotDataType.BYTE);
    put(Boolean.class, PinotDataType.BOOLEAN);
    put(Character.class, PinotDataType.CHARACTER);
    put(Short.class, PinotDataType.SHORT);
    put(Integer.class, PinotDataType.INTEGER);
    put(Long.class, PinotDataType.LONG);
    put(Float.class, PinotDataType.FLOAT);
    put(Double.class, PinotDataType.DOUBLE);
    put(String.class, PinotDataType.STRING);
    put(byte[].class, PinotDataType.BYTES);
  }};

  private static final Map<Class<?>, DataType> DATA_TYPE_MAP = new HashMap<Class<?>, DataType>() {{
    put(int.class, DataType.INT);
    put(Integer.class, DataType.INT);
    put(long.class, DataType.LONG);
    put(Long.class, DataType.LONG);
    put(float.class, DataType.FLOAT);
    put(Float.class, DataType.FLOAT);
    put(double.class, DataType.DOUBLE);
    put(Double.class, DataType.DOUBLE);
    put(String.class, DataType.STRING);
    put(byte[].class, DataType.BYTES);
  }};

  private static final Map<Class<?>, ColumnDataType> COLUMN_DATA_TYPE_MAP = new HashMap<Class<?>, ColumnDataType>() {{
    put(int.class, ColumnDataType.INT);
    put(Integer.class, ColumnDataType.INT);
    put(long.class, ColumnDataType.LONG);
    put(Long.class, ColumnDataType.LONG);
    put(float.class, ColumnDataType.FLOAT);
    put(Float.class, ColumnDataType.FLOAT);
    put(double.class, ColumnDataType.DOUBLE);
    put(Double.class, ColumnDataType.DOUBLE);
    put(String.class, ColumnDataType.STRING);
    put(byte[].class, ColumnDataType.BYTES);
  }};

  /**
   * Returns the corresponding PinotDataType for the given parameter class, or {@code null} if there is no one matching.
   */
  @Nullable
  public static PinotDataType getParameterType(Class<?> clazz) {
    return PARAMETER_TYPE_MAP.get(clazz);
  }

  /**
   * Returns the corresponding PinotDataType for the given argument class, or {@code null} if there is no one matching.
   */
  @Nullable
  public static PinotDataType getArgumentType(Class<?> clazz) {
    return ARGUMENT_TYPE_MAP.get(clazz);
  }

  /**
   * Returns the corresponding DataType for the given class, or {@code null} if there is no one matching.
   */
  @Nullable
  public static DataType getDataType(Class<?> clazz) {
    return DATA_TYPE_MAP.get(clazz);
  }

  /**
   * Returns the corresponding ColumnDataType for the given class, or {@code null} if there is no one matching.
   */
  @Nullable
  public static ColumnDataType getColumnDataType(Class<?> clazz) {
    return COLUMN_DATA_TYPE_MAP.get(clazz);
  }
}
