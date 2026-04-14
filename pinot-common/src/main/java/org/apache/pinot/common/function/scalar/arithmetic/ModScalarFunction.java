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
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Polymorphic modulo scalar function implementation.
 *
 * <p>Instances are immutable and thread-safe.
 */
@ScalarFunction
public class ModScalarFunction extends BaseBinaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT,
          new FunctionInfo(ModScalarFunction.class.getMethod("intMod", int.class, int.class), ModScalarFunction.class,
              false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(ModScalarFunction.class.getMethod("longMod", long.class, long.class),
              ModScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT,
          new FunctionInfo(ModScalarFunction.class.getMethod("floatMod", float.class, float.class),
              ModScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(ModScalarFunction.class.getMethod("doubleMod", double.class, double.class),
              ModScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL,
          new FunctionInfo(ModScalarFunction.class.getMethod("bigDecimalMod", BigDecimal.class, BigDecimal.class),
              ModScalarFunction.class, false));
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
    return "mod";
  }

  public static int intMod(int a, int b) {
    return a % b;
  }

  public static long longMod(long a, long b) {
    return a % b;
  }

  public static float floatMod(float a, float b) {
    return a % b;
  }

  public static double doubleMod(double a, double b) {
    return a % b;
  }

  public static BigDecimal bigDecimalMod(BigDecimal a, BigDecimal b) {
    return a.remainder(b);
  }
}
