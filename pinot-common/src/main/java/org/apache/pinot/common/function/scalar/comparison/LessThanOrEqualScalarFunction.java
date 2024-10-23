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
 * Polymorphic lessThanOrEqual (<=) scalar function implementation
 */
@ScalarFunction
public class LessThanOrEqualScalarFunction extends PolymorphicComparisonScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      // Set nullable parameters to false for each function because the return value should be null if any argument
      // is null
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          LessThanOrEqualScalarFunction.class.getMethod("intLessThanOrEqual", int.class, int.class),
          LessThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          LessThanOrEqualScalarFunction.class.getMethod("longLessThanOrEqual", long.class, long.class),
          LessThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          LessThanOrEqualScalarFunction.class.getMethod("floatLessThanOrEqual", float.class, float.class),
          LessThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          LessThanOrEqualScalarFunction.class.getMethod("doubleLessThanOrEqual", double.class, double.class),
          LessThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          LessThanOrEqualScalarFunction.class.getMethod("bigDecimalLessThanOrEqual",
              BigDecimal.class, BigDecimal.class),
          LessThanOrEqualScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING, new FunctionInfo(
          LessThanOrEqualScalarFunction.class.getMethod("stringLessThanOrEqual", String.class, String.class),
          LessThanOrEqualScalarFunction.class, false));
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
    return "lessThanOrEqual";
  }

  public static boolean intLessThanOrEqual(int a, int b) {
    return a <= b;
  }

  public static boolean longLessThanOrEqual(long a, long b) {
    return a <= b;
  }

  public static boolean floatLessThanOrEqual(float a, float b) {
    return a <= b;
  }

  public static boolean doubleLessThanOrEqual(double a, double b) {
    return a <= b;
  }

  public static boolean bigDecimalLessThanOrEqual(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) <= 0;
  }

  public static boolean stringLessThanOrEqual(String a, String b) {
    return a.compareTo(b) <= 0;
  }
}
