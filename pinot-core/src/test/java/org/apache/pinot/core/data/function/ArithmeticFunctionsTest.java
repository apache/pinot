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
package org.apache.pinot.core.data.function;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.function.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the arithmetic scalar transform functions
 */
public class ArithmeticFunctionsTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "arithmeticFunctionsDataProvider")
  public void testArithmeticFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "arithmeticFunctionsDataProvider")
  public Object[][] arithmeticFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();
    // test add
    {
      GenericRow row = new GenericRow();
      row.putValue("a", (byte) 1);
      row.putValue("b", (char) 2);
      inputs.add(new Object[]{"a + b", Lists.newArrayList("a", "b"), row, 3.0});
      inputs.add(new Object[]{"add(a, b)", Lists.newArrayList("a", "b"), row, 3.0});
      inputs.add(new Object[]{"plus(a, b)", Lists.newArrayList("a", "b"), row, 3.0});
    }
    // test subtract
    {
      GenericRow row = new GenericRow();
      row.putValue("a", (short) 3);
      row.putValue("b", 4);
      inputs.add(new Object[]{"a - b", Lists.newArrayList("a", "b"), row, -1.0});
    }
    // test multiply
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 5);
      row.putValue("b", 6);
      inputs.add(new Object[]{"a * b", Lists.newArrayList("a", "b"), row, 30.0});
      inputs.add(new Object[]{"mult(a, b)", Lists.newArrayList("a", "b"), row, 30.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 5L);
      row.putValue("b", 6f);
      inputs.add(new Object[]{"a * b", Lists.newArrayList("a", "b"), row, 30.0});
      inputs.add(new Object[]{"mult(a, b)", Lists.newArrayList("a", "b"), row, 30.0});
    }
    // test divide
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 7.0);
      row.putValue("b", 8);
      inputs.add(new Object[]{"a / b", Lists.newArrayList("a", "b"), row, 0.875});
      inputs.add(new Object[]{"div(a, b)", Lists.newArrayList("a", "b"), row, 0.875});
      inputs.add(new Object[]{"divide(a, b)", Lists.newArrayList("a", "b"), row, 0.875});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 7.0);
      row.putValue("b", "8");
      inputs.add(new Object[]{"a / b", Lists.newArrayList("a", "b"), row, 0.875});
      inputs.add(new Object[]{"div(a, b)", Lists.newArrayList("a", "b"), row, 0.875});
      inputs.add(new Object[]{"divide(a, b)", Lists.newArrayList("a", "b"), row, 0.875});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 1.0);
      row.putValue("b", "0.0001");
      inputs.add(new Object[]{"intdiv(a, b)", Lists.newArrayList("a", "b"), row, 10000L});
      inputs.add(new Object[]{"intDivOrZero(a, b)", Lists.newArrayList("a", "b"), row, 10000L});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 1.0);
      row.putValue("b", "0");
      inputs.add(new Object[]{"divide(a, b, 0)", Lists.newArrayList("a", "b"), row, 0.0});
      inputs.add(new Object[]{"intDivOrZero(a, b)", Lists.newArrayList("a", "b"), row, 0L});
    }
    // test isFinite
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 1.0);
      inputs.add(new Object[]{"isFinite(a)", Lists.newArrayList("a"), row, 1});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.POSITIVE_INFINITY);
      inputs.add(new Object[]{"isFinite(a)", Lists.newArrayList("a"), row, 0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NEGATIVE_INFINITY);
      inputs.add(new Object[]{"isFinite(a)", Lists.newArrayList("a"), row, 0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NaN);
      inputs.add(new Object[]{"isFinite(a)", Lists.newArrayList("a"), row, 0});
    }
    // test isInfinite
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 1.0);
      inputs.add(new Object[]{"isInfinite(a)", Lists.newArrayList("a"), row, 0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.POSITIVE_INFINITY);
      inputs.add(new Object[]{"isInfinite(a)", Lists.newArrayList("a"), row, 1});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NEGATIVE_INFINITY);
      inputs.add(new Object[]{"isInfinite(a)", Lists.newArrayList("a"), row, 1});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NaN);
      inputs.add(new Object[]{"isInfinite(a)", Lists.newArrayList("a"), row, 0});
    }
    // test ifNotFinite
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 1.0);
      inputs.add(new Object[]{"ifNotFinite(a, 2.0)", Lists.newArrayList("a"), row, 1.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.POSITIVE_INFINITY);
      inputs.add(new Object[]{"ifNotFinite(a, 2.0)", Lists.newArrayList("a"), row, 2.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NEGATIVE_INFINITY);
      inputs.add(new Object[]{"ifNotFinite(a, 2.0)", Lists.newArrayList("a"), row, 2.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NaN);
      inputs.add(new Object[]{"ifNotFinite(a, 2.0)", Lists.newArrayList("a"), row, 2.0});
    }
    // test isNaN
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 1.0);
      inputs.add(new Object[]{"isNaN(a)", Lists.newArrayList("a"), row, 0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.POSITIVE_INFINITY);
      inputs.add(new Object[]{"isNaN(a)", Lists.newArrayList("a"), row, 0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NEGATIVE_INFINITY);
      inputs.add(new Object[]{"isNaN(a)", Lists.newArrayList("a"), row, 0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", Double.NaN);
      inputs.add(new Object[]{"isNaN(a)", Lists.newArrayList("a"), row, 1});
    }
    // test mod
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9);
      row.putValue("b", 5);
      inputs.add(new Object[]{"a % b", Lists.newArrayList("a", "b"), row, 4.0});
      inputs.add(new Object[]{"mod(a, b)", Lists.newArrayList("a", "b"), row, 4.0});
      inputs.add(new Object[]{"moduloOrZero(a, b)", Lists.newArrayList("a", "b"), row, 4.0});
    }
    // test moduloOrZero
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9);
      row.putValue("b", 0);
      inputs.add(new Object[]{"moduloOrZero(a, b)", Lists.newArrayList("a", "b"), row, 0.0});
    }
    // test positiveModulo
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9);
      row.putValue("b", 5);
      inputs.add(new Object[]{"positiveModulo(a, b)", Lists.newArrayList("a", "b"), row, 4.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9);
      row.putValue("b", -5);
      inputs.add(new Object[]{"positiveModulo(a, b)", Lists.newArrayList("a", "b"), row, 4.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", -9);
      row.putValue("b", 5);
      inputs.add(new Object[]{"positiveModulo(a, b)", Lists.newArrayList("a", "b"), row, 1.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", -9);
      row.putValue("b", -5);
      inputs.add(new Object[]{"positiveModulo(a, b)", Lists.newArrayList("a", "b"), row, 1.0});
    }
    // test negate
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9);
      inputs.add(new Object[]{"negate(a)", Lists.newArrayList("a"), row, -9.0});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", -9);
      inputs.add(new Object[]{"negate(a)", Lists.newArrayList("a"), row, 9.0});
    }

    // test least/greatest
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9);
      row.putValue("b", 5);
      inputs.add(new Object[]{"least(a, b)", Lists.newArrayList("a", "b"), row, 5.0});
      inputs.add(new Object[]{"greatest(a, b)", Lists.newArrayList("a", "b"), row, 9.0});
    }

    // test abs, sign, floor, ceil, exp, sqrt, ln, log10, log2, power
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.5);
      row.putValue("b", -9.5);
      inputs.add(new Object[]{"abs(a)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"abs(b)", Lists.newArrayList("b"), row, 9.5});
      inputs.add(new Object[]{"sign(a)", Lists.newArrayList("a"), row, 1.0});
      inputs.add(new Object[]{"sign(b)", Lists.newArrayList("b"), row, -1.0});
      inputs.add(new Object[]{"floor(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"ceil(a)", Lists.newArrayList("a"), row, 10.0});
      inputs.add(new Object[]{"exp(a)", Lists.newArrayList("a"), row, Math.exp(9.5)});
      inputs.add(new Object[]{"sqrt(a)", Lists.newArrayList("a"), row, Math.sqrt(9.5)});
      inputs.add(new Object[]{"ln(a)", Lists.newArrayList("a"), row, Math.log(9.5)});
      inputs.add(new Object[]{"log10(a)", Lists.newArrayList("a"), row, Math.log10(9.5)});
      inputs.add(new Object[]{"log2(a)", Lists.newArrayList("a"), row, Math.log(9.5) / Math.log(2.0)});
      inputs.add(new Object[]{"power(a, 2)", Lists.newArrayList("a"), row, 9.5 * 9.5});
    }
    // test roundDecimal
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.5);
      inputs.add(new Object[]{"roundDecimal(a)", Lists.newArrayList("a"), row, 10.0});
      inputs.add(new Object[]{"roundDecimal(a, 0)", Lists.newArrayList("a"), row, 10.0});
      inputs.add(new Object[]{"roundDecimal(a, 1)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"roundDecimal(a, 2)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"roundDecimal(a, 3)", Lists.newArrayList("a"), row, 9.5});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.4);
      inputs.add(new Object[]{"roundDecimal(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"roundDecimal(a, 0)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"roundDecimal(a, 1)", Lists.newArrayList("a"), row, 9.4});
      inputs.add(new Object[]{"roundDecimal(a, 2)", Lists.newArrayList("a"), row, 9.4});
      inputs.add(new Object[]{"roundDecimal(a, 3)", Lists.newArrayList("a"), row, 9.4});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.6);
      inputs.add(new Object[]{"roundDecimal(a)", Lists.newArrayList("a"), row, 10.0});
      inputs.add(new Object[]{"roundDecimal(a, 0)", Lists.newArrayList("a"), row, 10.0});
      inputs.add(new Object[]{"roundDecimal(a, 1)", Lists.newArrayList("a"), row, 9.6});
      inputs.add(new Object[]{"roundDecimal(a, 2)", Lists.newArrayList("a"), row, 9.6});
      inputs.add(new Object[]{"roundDecimal(a, 3)", Lists.newArrayList("a"), row, 9.6});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.45);
      inputs.add(new Object[]{"roundDecimal(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"roundDecimal(a, 1)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"roundDecimal(a, 2)", Lists.newArrayList("a"), row, 9.45});
      inputs.add(new Object[]{"roundDecimal(a, 3)", Lists.newArrayList("a"), row, 9.45});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.46);
      inputs.add(new Object[]{"roundDecimal(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"roundDecimal(a, 1)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"roundDecimal(a, 2)", Lists.newArrayList("a"), row, 9.46});
      inputs.add(new Object[]{"roundDecimal(a, 3)", Lists.newArrayList("a"), row, 9.46});
    }
    // test truncate
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.5);
      inputs.add(new Object[]{"truncate(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 0)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 1)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"truncate(a, 2)", Lists.newArrayList("a"), row, 9.5});
      inputs.add(new Object[]{"truncate(a, 3)", Lists.newArrayList("a"), row, 9.5});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.4);
      inputs.add(new Object[]{"truncate(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 0)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 1)", Lists.newArrayList("a"), row, 9.4});
      inputs.add(new Object[]{"truncate(a, 2)", Lists.newArrayList("a"), row, 9.4});
      inputs.add(new Object[]{"truncate(a, 3)", Lists.newArrayList("a"), row, 9.4});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.6);
      inputs.add(new Object[]{"truncate(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 0)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 1)", Lists.newArrayList("a"), row, 9.6});
      inputs.add(new Object[]{"truncate(a, 2)", Lists.newArrayList("a"), row, 9.6});
      inputs.add(new Object[]{"truncate(a, 3)", Lists.newArrayList("a"), row, 9.6});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9.45);
      inputs.add(new Object[]{"truncate(a)", Lists.newArrayList("a"), row, 9.0});
      inputs.add(new Object[]{"truncate(a, 1)", Lists.newArrayList("a"), row, 9.4});
      inputs.add(new Object[]{"truncate(a, 2)", Lists.newArrayList("a"), row, 9.45});
      inputs.add(new Object[]{"truncate(a, 3)", Lists.newArrayList("a"), row, 9.45});
    }
    // test gcd, lcm
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9L);
      row.putValue("b", 6L);
      inputs.add(new Object[]{"gcd(a, b)", Lists.newArrayList("a", "b"), row, 3L});
      inputs.add(new Object[]{"lcm(a, b)", Lists.newArrayList("a", "b"), row, 18L});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 9L);
      row.putValue("b", 0L);
      inputs.add(new Object[]{"gcd(a, b)", Lists.newArrayList("a", "b"), row, 9L});
      inputs.add(new Object[]{"lcm(a, b)", Lists.newArrayList("a", "b"), row, 0L});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 0L);
      row.putValue("b", 9L);
      inputs.add(new Object[]{"gcd(a, b)", Lists.newArrayList("a", "b"), row, 9L});
      inputs.add(new Object[]{"lcm(a, b)", Lists.newArrayList("a", "b"), row, 0L});
    }
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 0L);
      row.putValue("b", 0L);
      inputs.add(new Object[]{"gcd(a, b)", Lists.newArrayList("a", "b"), row, 0L});
      inputs.add(new Object[]{"lcm(a, b)", Lists.newArrayList("a", "b"), row, 0L});
    }
    // test hypot
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 3.0);
      row.putValue("b", 4.0);
      inputs.add(new Object[]{"hypot(a, b)", Lists.newArrayList("a", "b"), row, 5.0});
    }
    // test byteswapInt
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 0x12345678);
      inputs.add(new Object[]{"byteswapInt(a)", Lists.newArrayList("a"), row, 0x78563412});
    }
    // test byteswapLong
    {
      GenericRow row = new GenericRow();
      row.putValue("a", 0x1234567890abcdefL);
      inputs.add(new Object[]{"byteswapLong(a)", Lists.newArrayList("a"), row, 0xefcdab9078563412L});
    }
    return inputs.toArray(new Object[0][]);
  }
}
