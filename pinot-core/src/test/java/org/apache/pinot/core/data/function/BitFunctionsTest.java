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
 * Tests the bitwise scalar transform functions.
 */
public class BitFunctionsTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "bitFunctionsDataProvider")
  public void testBitFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "bitFunctionsDataProvider")
  public Object[][] bitFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    GenericRow bitwiseRow = new GenericRow();
    bitwiseRow.putValue("lhs", 6);
    bitwiseRow.putValue("rhs", 10L);
    inputs.add(new Object[]{"bitAnd(lhs, rhs)", Lists.newArrayList("lhs", "rhs"), bitwiseRow, 2L});
    inputs.add(new Object[]{"bitOr(lhs, rhs)", Lists.newArrayList("lhs", "rhs"), bitwiseRow, 14L});
    inputs.add(new Object[]{"bitXor(lhs, rhs)", Lists.newArrayList("lhs", "rhs"), bitwiseRow, 12L});

    GenericRow unaryRow = new GenericRow();
    unaryRow.putValue("value", 6);
    inputs.add(new Object[]{"bitNot(value)", Lists.newArrayList("value"), unaryRow, -7L});
    inputs.add(new Object[]{"bitMask(value)", Lists.newArrayList("value"), unaryRow, 64L});

    GenericRow shiftRow = new GenericRow();
    shiftRow.putValue("value", -8L);
    shiftRow.putValue("shift", 2);
    inputs.add(new Object[]{"bitShiftLeft(value, shift)", Lists.newArrayList("value", "shift"), shiftRow, -32L});
    inputs.add(new Object[]{"bitShiftRight(value, shift)", Lists.newArrayList("value", "shift"), shiftRow, -2L});
    inputs.add(new Object[]{"bitShiftRightUnsigned(value, shift)", Lists.newArrayList("value", "shift"), shiftRow,
        4611686018427387902L});
    inputs.add(new Object[]{"bitShiftRightLogical(value, shift)", Lists.newArrayList("value", "shift"), shiftRow,
        4611686018427387902L});
    inputs.add(new Object[]{"bitExtract(value, shift)", Lists.newArrayList("value", "shift"), shiftRow, 0});
    inputs.add(new Object[]{"extractBit(value, shift)", Lists.newArrayList("value", "shift"), shiftRow, 0});

    return inputs.toArray(new Object[0][]);
  }
}
