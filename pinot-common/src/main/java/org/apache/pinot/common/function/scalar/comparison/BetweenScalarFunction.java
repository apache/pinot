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
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Polymorphic 'between' scalar function implementation.
 */
@ScalarFunction
public class BetweenScalarFunction implements PinotScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT,
          new FunctionInfo(BetweenScalarFunction.class.getMethod("intBetween", int.class, int.class, int.class),
              BetweenScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(BetweenScalarFunction.class.getMethod("longBetween", long.class, long.class, long.class),
              BetweenScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT,
          new FunctionInfo(BetweenScalarFunction.class.getMethod("floatBetween", float.class, float.class, float.class),
              BetweenScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          BetweenScalarFunction.class.getMethod("doubleBetween", double.class, double.class, double.class),
          BetweenScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          BetweenScalarFunction.class.getMethod("bigDecimalBetween", BigDecimal.class, BigDecimal.class,
              BigDecimal.class), BetweenScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING, new FunctionInfo(
          BetweenScalarFunction.class.getMethod("stringBetween", String.class, String.class, String.class),
          BetweenScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "between";
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 3) {
      return null;
    }

    if (argumentTypes[0].getStoredType() != argumentTypes[1].getStoredType()
        || argumentTypes[0].getStoredType() != argumentTypes[2].getStoredType()) {
      // In case of heterogeneous argument types, fall back to double based comparison and allow FunctionInvoker to
      // convert argument types for v1 engine support.
      return TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
    }

    return TYPE_FUNCTION_INFO_MAP.get(argumentTypes[0].getStoredType());
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments != 3) {
      return null;
    }

    // Fall back to double based comparison by default for backward compatibility
    return TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
  }

  public static boolean intBetween(int val, int lower, int upper) {
    return val >= lower && val <= upper;
  }

  public static boolean longBetween(long val, long lower, long upper) {
    return val >= lower && val <= upper;
  }

  public static boolean floatBetween(float val, float lower, float upper) {
    return val >= lower && val <= upper;
  }

  public static boolean doubleBetween(double val, double lower, double upper) {
    return val >= lower && val <= upper;
  }

  public static boolean bigDecimalBetween(BigDecimal val, BigDecimal lower, BigDecimal upper) {
    return val.compareTo(lower) >= 0 && val.compareTo(upper) <= 0;
  }

  public static boolean stringBetween(String val, String lower, String upper) {
    return val.compareTo(lower) >= 0 && val.compareTo(upper) <= 0;
  }
}
