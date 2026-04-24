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
 * Polymorphic absolute-value scalar function implementation.
 *
 * <p>Instances are immutable and thread-safe.
 */
@ScalarFunction
public class AbsScalarFunction extends BaseUnaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT,
          new FunctionInfo(AbsScalarFunction.class.getMethod("intAbs", int.class), AbsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(AbsScalarFunction.class.getMethod("longAbs", long.class), AbsScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT,
          new FunctionInfo(AbsScalarFunction.class.getMethod("floatAbs", float.class), AbsScalarFunction.class,
              false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(AbsScalarFunction.class.getMethod("doubleAbs", double.class), AbsScalarFunction.class,
              false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL,
          new FunctionInfo(AbsScalarFunction.class.getMethod("bigDecimalAbs", BigDecimal.class),
              AbsScalarFunction.class, false));
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
    return "abs";
  }

  public static int intAbs(int value) {
    if (value == Integer.MIN_VALUE) {
      throw new ArithmeticException("integer overflow");
    }
    return Math.abs(value);
  }

  public static long longAbs(long value) {
    if (value == Long.MIN_VALUE) {
      throw new ArithmeticException("long overflow");
    }
    return Math.abs(value);
  }

  public static float floatAbs(float value) {
    return Math.abs(value);
  }

  public static double doubleAbs(double value) {
    return Math.abs(value);
  }

  public static BigDecimal bigDecimalAbs(BigDecimal value) {
    return value.abs();
  }
}
