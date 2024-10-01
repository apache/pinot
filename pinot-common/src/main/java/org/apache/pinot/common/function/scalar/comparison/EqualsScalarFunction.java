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
package org.apache.pinot.common.function.scalar.comparison;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;

/**
 * Polymorphic equals (=) scalar function implementation
 */
@ScalarFunction
public class EqualsScalarFunction extends PolymorphicComparisonScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);
  private static final FunctionInfo DOUBLE_EQUALS_WITH_TOLERANCE;

  static {
    try {
       DOUBLE_EQUALS_WITH_TOLERANCE = new FunctionInfo(
           EqualsScalarFunction.class.getMethod("doubleEqualsWithTolerance", double.class, double.class),
           EqualsScalarFunction.class, false);

      // Set nullable parameters to false for each function because the return value should be null if any argument
      // is null
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("intEquals", int.class, int.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("longEquals", long.class, long.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("floatEquals", float.class, float.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("doubleEquals", double.class, double.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("bigDecimalEquals", BigDecimal.class, BigDecimal.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("stringEquals", String.class, String.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BYTES, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("bytesEquals", byte[].class, byte[].class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.OBJECT, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("objectEquals", Object.class, Object.class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT_ARRAY, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("intArrayEquals", int[].class, int[].class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG_ARRAY, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("longArrayEquals", long[].class, long[].class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT_ARRAY, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("floatArrayEquals", float[].class, float[].class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE_ARRAY, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("doubleArrayEquals", double[].class, double[].class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING_ARRAY, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("stringArrayEquals", String[].class, String[].class),
          EqualsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BYTES_ARRAY, new FunctionInfo(
          EqualsScalarFunction.class.getMethod("bytesArrayEquals", byte[][].class, byte[][].class),
          EqualsScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FunctionInfo functionInfoForType(ColumnDataType argumentType) {
    return TYPE_FUNCTION_INFO_MAP.get(argumentType);
  }

  @Override
  protected FunctionInfo defaultFunctionInfo() {
    return DOUBLE_EQUALS_WITH_TOLERANCE;
  }

  @Override
  public String getName() {
    return "equals";
  }

  public static boolean intEquals(int a, int b) {
    return a == b;
  }

  public static boolean longEquals(long a, long b) {
    return a == b;
  }

  public static boolean floatEquals(float a, float b) {
    return a == b;
  }

  public static boolean doubleEquals(double a, double b) {
    return a == b;
  }

  public static boolean doubleEqualsWithTolerance(double a, double b) {
    // To avoid approximation errors
    return Math.abs(a - b) < DOUBLE_COMPARISON_TOLERANCE;
  }

  public static boolean bigDecimalEquals(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) == 0;
  }

  public static boolean stringEquals(String a, String b) {
    return a.equals(b);
  }

  public static boolean bytesEquals(byte[] a, byte[] b) {
    return Arrays.equals(a, b);
  }

  public static boolean objectEquals(Object a, Object b) {
    return Objects.equals(a, b);
  }

  public static boolean intArrayEquals(int[] a, int[] b) {
    return Arrays.equals(a, b);
  }

  public static boolean longArrayEquals(long[] a, long[] b) {
    return Arrays.equals(a, b);
  }

  public static boolean floatArrayEquals(float[] a, float[] b) {
    return Arrays.equals(a, b);
  }

  public static boolean doubleArrayEquals(double[] a, double[] b) {
    return Arrays.equals(a, b);
  }

  public static boolean stringArrayEquals(String[] a, String[] b) {
    return Arrays.equals(a, b);
  }

  public static boolean bytesArrayEquals(byte[][] a, byte[][]b) {
    return Arrays.deepEquals(a, b);
  }
}
