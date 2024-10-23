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
import java.util.EnumMap;
import java.util.Map;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Polymorphic greaterThanOrEqual (>=) scalar function implementation
 */
@ScalarFunction
public class GreaterThanOrEqualScalarFunction extends PolymorphicComparisonScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      // Set nullable parameters to false for each function because the return value should be null if any argument
      // is null
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          GreaterThanOrEqualScalarFunction.class.getMethod("intGreaterThanOrEqual", int.class, int.class),
          GreaterThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          GreaterThanOrEqualScalarFunction.class.getMethod("longGreaterThanOrEqual", long.class, long.class),
          GreaterThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          GreaterThanOrEqualScalarFunction.class.getMethod("floatGreaterThanOrEqual", float.class, float.class),
          GreaterThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          GreaterThanOrEqualScalarFunction.class.getMethod("doubleGreaterThanOrEqual", double.class, double.class),
          GreaterThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          GreaterThanOrEqualScalarFunction.class.getMethod("bigDecimalGreaterThanOrEqual", BigDecimal.class,
              BigDecimal.class), GreaterThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING, new FunctionInfo(
          GreaterThanOrEqualScalarFunction.class.getMethod("stringGreaterThanOrEqual", String.class, String.class),
          GreaterThanOrEqualScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FunctionInfo functionInfoForType(ColumnDataType argumentType) {
    return TYPE_FUNCTION_INFO_MAP.get(argumentType);
  }

  @Override
  public String getName() {
    return "greaterThanOrEqual";
  }

  public static boolean intGreaterThanOrEqual(int a, int b) {
    return a >= b;
  }

  public static boolean longGreaterThanOrEqual(long a, long b) {
    return a >= b;
  }

  public static boolean floatGreaterThanOrEqual(float a, float b) {
    return a >= b;
  }

  public static boolean doubleGreaterThanOrEqual(double a, double b) {
    return a >= b;
  }

  public static boolean bigDecimalGreaterThanOrEqual(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) >= 0;
  }

  public static boolean stringGreaterThanOrEqual(String a, String b) {
    return a.compareTo(b) >= 0;
  }
}
