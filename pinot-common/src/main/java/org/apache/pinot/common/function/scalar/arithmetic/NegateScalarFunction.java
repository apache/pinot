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
 * Polymorphic negation scalar function implementation.
 *
 * <p>Instances are immutable and thread-safe.
 */
@ScalarFunction
public class NegateScalarFunction extends BaseUnaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT, new FunctionInfo(
          NegateScalarFunction.class.getMethod("intNegate", int.class), NegateScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG, new FunctionInfo(
          NegateScalarFunction.class.getMethod("longNegate", long.class), NegateScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT, new FunctionInfo(
          NegateScalarFunction.class.getMethod("floatNegate", float.class), NegateScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE, new FunctionInfo(
          NegateScalarFunction.class.getMethod("doubleNegate", double.class), NegateScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BIG_DECIMAL, new FunctionInfo(
          NegateScalarFunction.class.getMethod("bigDecimalNegate", BigDecimal.class), NegateScalarFunction.class,
          false));
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
    return "negate";
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return ArithmeticFunctionUtils.unaryArithmeticSqlFunction(getName());
  }

  public static int intNegate(int value) {
    return Math.negateExact(value);
  }

  public static long longNegate(long value) {
    return Math.negateExact(value);
  }

  public static float floatNegate(float value) {
    return -value;
  }

  public static double doubleNegate(double value) {
    return -value;
  }

  public static BigDecimal bigDecimalNegate(BigDecimal value) {
    return value.negate();
  }
}
