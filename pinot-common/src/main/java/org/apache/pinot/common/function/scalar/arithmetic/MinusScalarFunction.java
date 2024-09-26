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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"sub", "minus"})
public class MinusScalarFunction extends PolymorphicBinaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new HashMap<>();

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(MinusScalarFunction.class.getMethod("longMinus", long.class, long.class),
              MinusScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(MinusScalarFunction.class.getMethod("doubleMinus", double.class, double.class),
              MinusScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @Nullable
  public PinotSqlFunction toPinotSqlFunction() {
    return new PinotSqlFunction(getName(), ReturnTypes.NULLABLE_SUM, OperandTypes.NUMERIC_NUMERIC);
  }

  @Override
  protected FunctionInfo functionInfoForType(ColumnDataType argumentType) {
    FunctionInfo functionInfo = TYPE_FUNCTION_INFO_MAP.get(argumentType);

    // Fall back to double based comparison by default
    return functionInfo != null ? functionInfo : TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
  }

  @Override
  public String getName() {
    return "minus";
  }

  public static long longMinus(long a, long b) {
    return a - b;
  }

  public static double doubleMinus(double a, double b) {
    return a - b;
  }
}
