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

import java.util.List;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;


/**
 * Tests for arithmetic scalar function edge cases.
 */
public class ArithmeticScalarFunctionTest {

  @Test
  public void testAbsIntegralOverflowFailsFast() {
    assertThrows(ArithmeticException.class, () -> AbsScalarFunction.intAbs(Integer.MIN_VALUE));
    assertThrows(ArithmeticException.class, () -> AbsScalarFunction.longAbs(Long.MIN_VALUE));
  }

  @Test
  public void testNegateIntegralOverflowFailsFast() {
    assertThrows(ArithmeticException.class, () -> NegateScalarFunction.intNegate(Integer.MIN_VALUE));
    assertThrows(ArithmeticException.class, () -> NegateScalarFunction.longNegate(Long.MIN_VALUE));
  }

  /**
   * Regression test: additive/multiplicative operators expose only LONG and DOUBLE overloads, so an INT x INT pair
   * must widen to the LONG overload. Otherwise the equal-argument-types branch in
   * {@link BaseBinaryArithmeticScalarFunction} resolves the (absent) INT overload and silently falls back to DOUBLE,
   * flipping whole-number arithmetic such as {@code COUNT(DISTINCT ...) * 60} from LONG to DOUBLE.
   */
  @Test
  public void testWholeNumberArithmeticWidensToLong() {
    ColumnDataType[] intOperands = {ColumnDataType.INT, ColumnDataType.INT};
    List<BaseBinaryArithmeticScalarFunction> functions =
        List.of(new PlusScalarFunction(), new MinusScalarFunction(), new MultScalarFunction());
    for (BaseBinaryArithmeticScalarFunction function : functions) {
      FunctionInfo functionInfo = function.getFunctionInfo(intOperands);
      assertNotNull(functionInfo, function.getName() + " should resolve an overload for (INT, INT)");
      assertEquals(functionInfo.getMethod().getReturnType(), long.class,
          function.getName() + "(INT, INT) must widen to LONG");
    }
  }

  /**
   * Companion invariant to {@link #testWholeNumberArithmeticWidensToLong()}: operators that expose a dedicated INT
   * overload (and cannot overflow for equal-width operands) must keep INT x INT as INT. This asymmetry is why the
   * widening fix lives in the individual functions rather than the shared {@link BaseBinaryArithmeticScalarFunction}
   * dispatch - a blanket dispatch change would force these to LONG as well.
   */
  @Test
  public void testIntPreservingArithmeticStaysInt() {
    ColumnDataType[] intOperands = {ColumnDataType.INT, ColumnDataType.INT};
    List<BaseBinaryArithmeticScalarFunction> functions =
        List.of(new ModScalarFunction(), new GreatestScalarFunction(), new LeastScalarFunction());
    for (BaseBinaryArithmeticScalarFunction function : functions) {
      FunctionInfo functionInfo = function.getFunctionInfo(intOperands);
      assertNotNull(functionInfo, function.getName() + " should resolve an overload for (INT, INT)");
      assertEquals(functionInfo.getMethod().getReturnType(), int.class,
          function.getName() + "(INT, INT) should stay INT");
    }
  }
}
