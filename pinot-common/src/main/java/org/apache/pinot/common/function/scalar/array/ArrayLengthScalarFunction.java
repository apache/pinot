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
package org.apache.pinot.common.function.scalar.array;

import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"ARRAYLENGTH", "CARDINALITY"})
public class ArrayLengthScalarFunction implements PinotScalarFunction {

  private static final Map<DataSchema.ColumnDataType, FunctionInfo>
      TYPE_FUNCTION_INFO_MAP = new EnumMap<>(DataSchema.ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.INT_ARRAY,
          new FunctionInfo(ArrayLengthScalarFunction.class.getMethod("arrayLength", int[].class),
              ArrayLengthScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.LONG_ARRAY,
          new FunctionInfo(ArrayLengthScalarFunction.class.getMethod("arrayLength", long[].class),
              ArrayLengthScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.FLOAT_ARRAY,
          new FunctionInfo(ArrayLengthScalarFunction.class.getMethod("arrayLength", float[].class),
              ArrayLengthScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.DOUBLE_ARRAY,
          new FunctionInfo(ArrayLengthScalarFunction.class.getMethod("arrayLength", double[].class),
              ArrayLengthScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.STRING_ARRAY,
          new FunctionInfo(ArrayLengthScalarFunction.class.getMethod("arrayLength", String[].class),
              ArrayLengthScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "ARRAYLENGTH";
  }

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    // Should already be registered in PinotOperatorTable by the transform function implementation
    return null;
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(DataSchema.ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 1) {
      return null;
    }
    return TYPE_FUNCTION_INFO_MAP.get(argumentTypes[0]);
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments != 1) {
      return null;
    }
    // Fall back to string
    return getFunctionInfo(new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING_ARRAY});
  }

  public static int arrayLength(int[] array) {
    return array.length;
  }

  public static int arrayLength(long[] array) {
    return array.length;
  }

  public static int arrayLength(float[] array) {
    return array.length;
  }

  public static int arrayLength(double[] array) {
    return array.length;
  }

  public static int arrayLength(String[] array) {
    return array.length;
  }
}
