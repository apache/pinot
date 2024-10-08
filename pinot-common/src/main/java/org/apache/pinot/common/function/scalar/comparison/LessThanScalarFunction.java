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
 * Polymorphic lessThan (<) scalar function implementation
 */
@ScalarFunction
public class LessThanScalarFunction extends PolymorphicComparisonScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      // Set nullable parameters to false for each function because the return value should be null if any argument
      // is null
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          LessThanScalarFunction.class.getMethod("intLessThan", int.class, int.class),
          LessThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          LessThanScalarFunction.class.getMethod("longLessThan", long.class, long.class),
          LessThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          LessThanScalarFunction.class.getMethod("floatLessThan", float.class, float.class),
          LessThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          LessThanScalarFunction.class.getMethod("doubleLessThan", double.class, double.class),
          LessThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          LessThanScalarFunction.class.getMethod("bigDecimalLessThan", BigDecimal.class, BigDecimal.class),
          LessThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING, new FunctionInfo(
          LessThanScalarFunction.class.getMethod("stringLessThan", String.class, String.class),
          LessThanScalarFunction.class, false));
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
    return "lessThan";
  }

  public static boolean intLessThan(int a, int b) {
    return a < b;
  }

  public static boolean longLessThan(long a, long b) {
    return a < b;
  }

  public static boolean floatLessThan(float a, float b) {
    return a < b;
  }

  public static boolean doubleLessThan(double a, double b) {
    return a < b;
  }

  public static boolean bigDecimalLessThan(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) < 0;
  }

  public static boolean stringLessThan(String a, String b) {
    return a.compareTo(b) < 0;
  }
}
