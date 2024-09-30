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

import java.util.EnumMap;
import java.util.Map;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"mult", "times"})
public class MultScalarFunction extends PolymorphicBinaryArithmeticScalarFunction {

  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP = new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG,
          new FunctionInfo(MultScalarFunction.class.getMethod("longMult", long.class, long.class),
              MultScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE,
          new FunctionInfo(MultScalarFunction.class.getMethod("doubleMult", double.class, double.class),
              MultScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FunctionInfo functionInfoForType(ColumnDataType argumentType) {
    FunctionInfo functionInfo = TYPE_FUNCTION_INFO_MAP.get(argumentType);

    // Fall back to double based comparison by default
    return functionInfo != null ? functionInfo : TYPE_FUNCTION_INFO_MAP.get(ColumnDataType.DOUBLE);
  }

  @Override
  public String getName() {
    return "mult";
  }

  public static long longMult(long a, long b) {
    return a * b;
  }

  public static double doubleMult(double a, double b) {
    return a * b;
  }
}
