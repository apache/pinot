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
package org.apache.pinot.common.function.scalar.bitwise;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Polymorphic bit extraction scalar function.
 *
 * <p>This implementation is stateless and thread-safe.
 */
@ScalarFunction(names = {"bitExtract", "extractBit"})
public class BitExtractScalarFunction implements PinotScalarFunction {
  private static final FunctionInfo INT_FUNCTION_INFO;
  private static final FunctionInfo LONG_FUNCTION_INFO;

  static {
    try {
      INT_FUNCTION_INFO =
          new FunctionInfo(BitExtractScalarFunction.class.getMethod("intBitExtract", int.class, long.class),
              BitExtractScalarFunction.class, false);
      LONG_FUNCTION_INFO =
          new FunctionInfo(BitExtractScalarFunction.class.getMethod("longBitExtract", long.class, long.class),
              BitExtractScalarFunction.class, false);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "bitExtract";
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return BitFunctionUtils.extractSqlFunction(getName());
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }
    ColumnDataType valueType = argumentTypes[0].getStoredType();
    ColumnDataType bitType = argumentTypes[1].getStoredType();
    if (!BitFunctionUtils.isIntegral(valueType) || !BitFunctionUtils.isIntegral(bitType)) {
      return null;
    }
    return valueType == ColumnDataType.INT ? INT_FUNCTION_INFO : LONG_FUNCTION_INFO;
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    // LONG overload is a safe fallback — INT inputs widen to LONG losslessly via convertTypes.
    return numArguments == 2 ? LONG_FUNCTION_INFO : null;
  }

  public static int intBitExtract(int value, long bit) {
    if (!BitFunctionUtils.isBitIndexInRange(bit, Integer.SIZE)) {
      return 0;
    }
    return (value >>> (int) bit) & 1;
  }

  public static int longBitExtract(long value, long bit) {
    if (!BitFunctionUtils.isBitIndexInRange(bit, Long.SIZE)) {
      return 0;
    }
    return (int) ((value >>> (int) bit) & 1L);
  }
}
