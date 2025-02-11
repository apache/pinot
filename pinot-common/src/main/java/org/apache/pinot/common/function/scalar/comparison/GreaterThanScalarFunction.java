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
 * Polymorphic greaterThan (>) scalar function implementation
 */
@ScalarFunction
public class GreaterThanScalarFunction extends PolymorphicComparisonScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      // Set nullable parameters to false for each function because the return value should be null if any argument
      // is null
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          GreaterThanScalarFunction.class.getMethod("intGreaterThan", int.class, int.class),
          GreaterThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          GreaterThanScalarFunction.class.getMethod("longGreaterThan", long.class, long.class),
          GreaterThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          GreaterThanScalarFunction.class.getMethod("floatGreaterThan", float.class, float.class),
          GreaterThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          GreaterThanScalarFunction.class.getMethod("doubleGreaterThan", double.class, double.class),
          GreaterThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          GreaterThanScalarFunction.class.getMethod("bigDecimalGreaterThan", BigDecimal.class, BigDecimal.class),
          GreaterThanScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING, new FunctionInfo(
          GreaterThanScalarFunction.class.getMethod("stringGreaterThan", String.class, String.class),
          GreaterThanScalarFunction.class, false));
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
    return "greaterThan";
  }

  public static boolean intGreaterThan(int a, int b) {
    return a > b;
  }

  public static boolean longGreaterThan(long a, long b) {
    return a > b;
  }

  public static boolean floatGreaterThan(float a, float b) {
    return a > b;
  }

  public static boolean doubleGreaterThan(double a, double b) {
    return a > b;
  }

  public static boolean bigDecimalGreaterThan(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) > 0;
  }

  public static boolean stringGreaterThan(String a, String b) {
    return a.compareTo(b) > 0;
  }
}
