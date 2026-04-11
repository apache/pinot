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
 * Polymorphic greatest scalar function implementation.
 *
 * <p>Instances are immutable and thread-safe.
 */
@ScalarFunction
public class GreatestScalarFunction extends BaseBinaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT,
          new FunctionInfo(GreatestScalarFunction.class.getMethod("intGreatest", int.class, int.class),
              GreatestScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(GreatestScalarFunction.class.getMethod("longGreatest", long.class, long.class),
              GreatestScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT,
          new FunctionInfo(GreatestScalarFunction.class.getMethod("floatGreatest", float.class, float.class),
              GreatestScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(GreatestScalarFunction.class.getMethod("doubleGreatest", double.class, double.class),
              GreatestScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          GreatestScalarFunction.class.getMethod("bigDecimalGreatest", BigDecimal.class, BigDecimal.class),
          GreatestScalarFunction.class, false));
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
    return "greatest";
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return ArithmeticFunctionUtils.binaryArithmeticSqlFunction(getName());
  }

  public static int intGreatest(int a, int b) {
    return Math.max(a, b);
  }

  public static long longGreatest(long a, long b) {
    return Math.max(a, b);
  }

  public static float floatGreatest(float a, float b) {
    return Math.max(a, b);
  }

  public static double doubleGreatest(double a, double b) {
    return Double.max(a, b);
  }

  public static BigDecimal bigDecimalGreatest(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) >= 0 ? a : b;
  }
}
