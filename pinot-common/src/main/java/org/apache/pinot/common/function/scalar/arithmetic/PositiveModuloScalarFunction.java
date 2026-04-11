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

import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.Map;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Polymorphic positive-modulo scalar function implementation.
 *
 * <p>Instances are immutable and thread-safe.
 */
@ScalarFunction
public class PositiveModuloScalarFunction extends BaseBinaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          PositiveModuloScalarFunction.class.getMethod("intPositiveModulo", int.class, int.class),
          PositiveModuloScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          PositiveModuloScalarFunction.class.getMethod("longPositiveModulo", long.class, long.class),
          PositiveModuloScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          PositiveModuloScalarFunction.class.getMethod("floatPositiveModulo", float.class, float.class),
          PositiveModuloScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          PositiveModuloScalarFunction.class.getMethod("doublePositiveModulo", double.class, double.class),
          PositiveModuloScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          PositiveModuloScalarFunction.class.getMethod("bigDecimalPositiveModulo", BigDecimal.class, BigDecimal.class),
          PositiveModuloScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FunctionInfo functionInfoForType(ColumnDataType argumentType) {
    FunctionInfo functionInfo = TYPE_FUNCTION_INFO_MAP.get(argumentType);
    return functionInfo != null ? functionInfo : defaultFunctionInfo();
  }

  @Override
  public String getName() {
    return "positiveModulo";
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return ArithmeticFunctionUtils.binaryArithmeticSqlFunction(getName());
  }

  public static int intPositiveModulo(int a, int b) {
    int result = a % b;
    if (result >= 0) {
      return result;
    }
    if (b == Integer.MIN_VALUE) {
      return Integer.MAX_VALUE + result + 1;
    }
    return result + Math.abs(b);
  }

  public static long longPositiveModulo(long a, long b) {
    long result = a % b;
    if (result >= 0) {
      return result;
    }
    if (b == Long.MIN_VALUE) {
      return Long.MAX_VALUE + result + 1;
    }
    return result + Math.abs(b);
  }

  public static float floatPositiveModulo(float a, float b) {
    float result = a % b;
    return result >= 0 ? result : result + Math.abs(b);
  }

  public static double doublePositiveModulo(double a, double b) {
    double result = a % b;
    return result >= 0 ? result : result + Math.abs(b);
  }

  public static BigDecimal bigDecimalPositiveModulo(BigDecimal a, BigDecimal b) {
    BigDecimal result = a.remainder(b);
    return result.signum() >= 0 ? result : result.add(b.abs());
  }
}
