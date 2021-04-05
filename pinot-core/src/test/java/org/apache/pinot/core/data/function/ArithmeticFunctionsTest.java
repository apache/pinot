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

    GenericRow row0 = new GenericRow();
    row0.putValue("a", (byte) 1);
    row0.putValue("b", (char) 2);
    inputs.add(new Object[]{"plus(a, b)", Lists.newArrayList("a", "b"), row0, 3.0});

    GenericRow row1 = new GenericRow();
    row1.putValue("a", (short) 3);
    row1.putValue("b", 4);
    inputs.add(new Object[]{"minus(a, b)", Lists.newArrayList("a", "b"), row1, -1.0});

    GenericRow row2 = new GenericRow();
    row2.putValue("a", 5L);
    row2.putValue("b", 6f);
    inputs.add(new Object[]{"times(a, b)", Lists.newArrayList("a", "b"), row2, 30.0});

    GenericRow row3 = new GenericRow();
    row3.putValue("a", 7.0);
    row3.putValue("b", "8");
    inputs.add(new Object[]{"divide(a, b)", Lists.newArrayList("a", "b"), row3, 0.875});

    return inputs.toArray(new Object[0][]);
  }
}
