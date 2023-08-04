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
 * Tests the vector scalar functions
 */
public class VectorFunctionsTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "vectorFunctionsDataProvider")
  public void testVectorFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "vectorFunctionsDataProvider")
  public Object[][] vectorFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    GenericRow row = new GenericRow();
    row.putValue("vector1", new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f});
    row.putValue("vector2", new float[]{0.6f, 0.7f, 0.8f, 0.9f, 1.0f});
    inputs.add(new Object[]{
        "cosineDistance(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 0.03504950750101454
    });
    inputs.add(new Object[]{
        "innerProduct(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 1.2999999970197678
    });
    inputs.add(new Object[]{
        "l2Distance(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 1.1180339754218913
    });
    inputs.add(new Object[]{
        "l1Distance(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 2.4999999701976776
    });
    inputs.add(new Object[]{"vectorDims(vector1)", Lists.newArrayList("vector1"), row, 5});
    inputs.add(new Object[]{"vectorDims(vector2)", Lists.newArrayList("vector2"), row, 5});
    inputs.add(new Object[]{"vectorNorm(vector1)", Lists.newArrayList("vector1"), row, 0.741619857751291});
    inputs.add(new Object[]{"vectorNorm(vector2)", Lists.newArrayList("vector2"), row, 1.8165902091773676});
    return inputs.toArray(new Object[0][]);
  }

  @Test(dataProvider = "vectorFunctionsZeroDataProvider")
  public void testVectorFunctionsWithZeroVector(String functionExpression, List<String> expectedArguments,
      GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "vectorFunctionsZeroDataProvider")
  public Object[][] vectorFunctionsZeroDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    GenericRow row = new GenericRow();
    row.putValue("vector1", new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f});
    row.putValue("vector2", new float[]{0f, 0f, 0f, 0f, 0f});
    inputs.add(new Object[]{
        "cosineDistance(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, Double.NaN
    });
    inputs.add(new Object[]{
        "cosineDistance(vector1, vector2, 0.0)", Lists.newArrayList("vector1", "vector2"), row, 0.0
    });
    inputs.add(new Object[]{
        "cosineDistance(vector1, vector2, 1.0)", Lists.newArrayList("vector1", "vector2"), row, 1.0
    });
    inputs.add(new Object[]{
        "innerProduct(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 0.0
    });
    inputs.add(new Object[]{
        "l2Distance(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 0.741619857751291
    });
    inputs.add(new Object[]{
        "l1Distance(vector1, vector2)", Lists.newArrayList("vector1", "vector2"), row, 1.5000000223517418
    });
    inputs.add(new Object[]{"vectorDims(vector1)", Lists.newArrayList("vector1"), row, 5});
    inputs.add(new Object[]{"vectorDims(vector2)", Lists.newArrayList("vector2"), row, 5});
    inputs.add(new Object[]{"vectorNorm(vector1)", Lists.newArrayList("vector1"), row, 0.741619857751291});
    inputs.add(new Object[]{"vectorNorm(vector2)", Lists.newArrayList("vector2"), row, 0.0});
    return inputs.toArray(new Object[0][]);
  }
}
