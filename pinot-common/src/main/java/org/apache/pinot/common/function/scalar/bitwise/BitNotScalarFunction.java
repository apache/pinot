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

import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Polymorphic bitwise NOT scalar function.
 *
 * <p>This implementation is stateless and thread-safe.
 */
@ScalarFunction
public class BitNotScalarFunction extends BaseUnaryIntegralScalarFunction {
  private static final FunctionInfo INT_FUNCTION_INFO;
  private static final FunctionInfo LONG_FUNCTION_INFO;

  static {
    try {
      INT_FUNCTION_INFO =
          new FunctionInfo(BitNotScalarFunction.class.getMethod("intBitNot", int.class), BitNotScalarFunction.class,
              false);
      LONG_FUNCTION_INFO =
          new FunctionInfo(BitNotScalarFunction.class.getMethod("longBitNot", long.class), BitNotScalarFunction.class,
              false);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "bitNot";
  }

  @Override
  protected FunctionInfo intFunctionInfo() {
    return INT_FUNCTION_INFO;
  }

  @Override
  protected FunctionInfo longFunctionInfo() {
    return LONG_FUNCTION_INFO;
  }

  public static int intBitNot(int value) {
    return ~value;
  }

  public static long longBitNot(long value) {
    return ~value;
  }
}
