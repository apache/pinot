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
 * Polymorphic least scalar function implementation.
 *
 * <p>Instances are immutable and thread-safe.
 */
@ScalarFunction
public class LeastScalarFunction extends BaseBinaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT,
          new FunctionInfo(LeastScalarFunction.class.getMethod("intLeast", int.class, int.class),
              LeastScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(LeastScalarFunction.class.getMethod("longLeast", long.class, long.class),
              LeastScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT,
          new FunctionInfo(LeastScalarFunction.class.getMethod("floatLeast", float.class, float.class),
              LeastScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(LeastScalarFunction.class.getMethod("doubleLeast", double.class, double.class),
              LeastScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL,
          new FunctionInfo(LeastScalarFunction.class.getMethod("bigDecimalLeast", BigDecimal.class, BigDecimal.class),
              LeastScalarFunction.class, false));
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
    return "least";
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return ArithmeticFunctionUtils.binaryArithmeticSqlFunction(getName());
  }

  public static int intLeast(int a, int b) {
    return Math.min(a, b);
  }

  public static long longLeast(long a, long b) {
    return Math.min(a, b);
  }

  public static float floatLeast(float a, float b) {
    return Math.min(a, b);
  }

  public static double doubleLeast(double a, double b) {
    return Double.min(a, b);
  }

  public static BigDecimal bigDecimalLeast(BigDecimal a, BigDecimal b) {
    return a.compareTo(b) <= 0 ? a : b;
  }
}
