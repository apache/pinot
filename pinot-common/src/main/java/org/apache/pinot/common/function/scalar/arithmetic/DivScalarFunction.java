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
package org.apache.pinot.common.function.scalar.arithmetic;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"div", "divide"})
public class DivScalarFunction implements PinotScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TWO_ARG_TYPE_FUNCTION_INFO_MAP = new HashMap<>();
  private static final Map<ColumnDataType, FunctionInfo> THREE_ARG_TYPE_FUNCTION_INFO_MAP = new HashMap<>();

  static {
    try {
      TWO_ARG_TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(DivScalarFunction.class.getMethod("longDiv", long.class, long.class),
              DivScalarFunction.class, false));
      TWO_ARG_TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(DivScalarFunction.class.getMethod("doubleDiv", double.class, double.class),
              DivScalarFunction.class, false));
      THREE_ARG_TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(DivScalarFunction.class.getMethod("longDivWithDefault", long.class, long.class, long.class),
              DivScalarFunction.class, false));
      THREE_ARG_TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          DivScalarFunction.class.getMethod("doubleDivWithDefault", double.class, double.class, double.class),
          DivScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    // Use double based comparison for backward compatibility
    if (numArguments == 2) {
      return TWO_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
    } else if (numArguments == 3) {
      return THREE_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length == 2) {
      if (argumentTypes[0].getStoredType() == ColumnDataType.DOUBLE
          || argumentTypes[0].getStoredType() == ColumnDataType.FLOAT
          || argumentTypes[1].getStoredType() == ColumnDataType.DOUBLE
          || argumentTypes[1].getStoredType() == ColumnDataType.FLOAT) {
        return TWO_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
      } else if ((argumentTypes[0].getStoredType() == ColumnDataType.LONG
          || argumentTypes[0].getStoredType() == ColumnDataType.INT) && (
          argumentTypes[1].getStoredType() == ColumnDataType.LONG
              || argumentTypes[1].getStoredType() == ColumnDataType.INT)) {
        return TWO_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.LONG);
      } else {
        return TWO_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
      }
    } else if (argumentTypes.length == 3) {
      if (argumentTypes[0].getStoredType() == ColumnDataType.DOUBLE
          || argumentTypes[0].getStoredType() == ColumnDataType.FLOAT
          || argumentTypes[1].getStoredType() == ColumnDataType.DOUBLE
          || argumentTypes[1].getStoredType() == ColumnDataType.FLOAT
          || argumentTypes[2].getStoredType() == ColumnDataType.DOUBLE
          || argumentTypes[2].getStoredType() == ColumnDataType.FLOAT) {
        return THREE_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
      } else if ((argumentTypes[0].getStoredType() == ColumnDataType.LONG
          || argumentTypes[0].getStoredType() == ColumnDataType.INT) && (
          argumentTypes[1].getStoredType() == ColumnDataType.LONG
              || argumentTypes[1].getStoredType() == ColumnDataType.INT) && (
          argumentTypes[2].getStoredType() == ColumnDataType.LONG
              || argumentTypes[2].getStoredType() == ColumnDataType.INT)) {
        return THREE_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.LONG);
      } else {
        return THREE_ARG_TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
      }
    } else {
      return null;
    }
  }

  @Override
  public String getName() {
    return "div";
  }

  public static long longDiv(long a, long b) {
    return a / b;
  }

  public static long longDivWithDefault(long a, long b, long defaultValue) {
    return (b == 0) ? defaultValue : a / b;
  }

  public static double doubleDiv(double a, double b) {
    return a / b;
  }

  public static double doubleDivWithDefault(double a, double b, double defaultValue) {
    return (b == 0) ? defaultValue : a / b;
  }
}
