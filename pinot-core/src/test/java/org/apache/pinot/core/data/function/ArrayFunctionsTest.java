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
import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.local.function.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the array scalar functions
 */
public class ArrayFunctionsTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "arrayFunctionsDataProvider")
  public void testArrayFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "arrayFunctionsDataProvider")
  public Object[][] arrayFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    GenericRow row = new GenericRow();
    row.putValue("intArray", new int[]{3, 2, 10, 6, 1, 12});
    row.putValue("integerArray", new Integer[]{3, 2, 10, 6, 1, 12});
    row.putValue("stringArray", new String[]{"3", "2", "10", "6", "1", "12"});

    inputs.add(new Object[]{"array_reverse_int(intArray)", Collections
        .singletonList("intArray"), row, new int[]{12, 1, 6, 10, 2, 3}});
    inputs.add(new Object[]{"array_reverse_int(integerArray)", Collections
        .singletonList("integerArray"), row, new int[]{12, 1, 6, 10, 2, 3}});
    inputs.add(new Object[]{"array_reverse_int(stringArray)", Collections
        .singletonList("stringArray"), row, new int[]{12, 1, 6, 10, 2, 3}});

    inputs.add(new Object[]{"array_reverse_string(intArray)", Collections
        .singletonList("intArray"), row, new String[]{"12", "1", "6", "10", "2", "3"}});
    inputs.add(new Object[]{"array_reverse_string(integerArray)", Collections
        .singletonList("integerArray"), row, new String[]{"12", "1", "6", "10", "2", "3"}});
    inputs.add(new Object[]{"array_reverse_string(stringArray)", Collections
        .singletonList("stringArray"), row, new String[]{"12", "1", "6", "10", "2", "3"}});

    inputs.add(new Object[]{"array_sort_int(intArray)", Collections
        .singletonList("intArray"), row, new int[]{1, 2, 3, 6, 10, 12}});
    inputs.add(new Object[]{"array_sort_int(integerArray)", Collections
        .singletonList("integerArray"), row, new int[]{1, 2, 3, 6, 10, 12}});
    inputs.add(new Object[]{"array_sort_int(stringArray)", Collections
        .singletonList("stringArray"), row, new int[]{1, 2, 3, 6, 10, 12}});

    inputs.add(new Object[]{"array_sort_string(intArray)", Collections
        .singletonList("intArray"), row, new String[]{"1", "10", "12", "2", "3", "6"}});
    inputs.add(new Object[]{"array_sort_string(integerArray)", Collections
        .singletonList("integerArray"), row, new String[]{"1", "10", "12", "2", "3", "6"}});
    inputs.add(new Object[]{"array_sort_string(stringArray)", Collections
        .singletonList("stringArray"), row, new String[]{"1", "10", "12", "2", "3", "6"}});

    inputs.add(new Object[]{"array_index_of_int(intArray, 2)", Collections.singletonList("intArray"), row, 1});
    inputs.add(new Object[]{"array_index_of_int(integerArray, 2)", Collections.singletonList("integerArray"), row, 1});
    inputs.add(new Object[]{"array_index_of_int(stringArray, 2)", Collections.singletonList("stringArray"), row, 1});

    inputs.add(new Object[]{"array_index_of_string(intArray, '2')", Collections.singletonList("intArray"), row, 1});
    inputs.add(
        new Object[]{"array_index_of_string(integerArray, '2')", Collections.singletonList("integerArray"), row, 1});
    inputs
        .add(new Object[]{"array_index_of_string(stringArray, '2')", Collections.singletonList("stringArray"), row, 1});

    inputs.add(new Object[]{"array_contains_int(intArray, 2)", Collections.singletonList("intArray"), row, true});
    inputs
        .add(new Object[]{"array_contains_int(integerArray, 2)", Collections.singletonList("integerArray"), row, true});
    inputs.add(new Object[]{"array_contains_int(stringArray, 2)", Collections.singletonList("stringArray"), row, true});

    inputs.add(new Object[]{"array_contains_string(intArray, '2')", Collections.singletonList("intArray"), row, true});
    inputs.add(
        new Object[]{"array_contains_string(integerArray, '2')", Collections.singletonList("integerArray"), row, true});
    inputs.add(
        new Object[]{"array_contains_string(stringArray, '2')", Collections.singletonList("stringArray"), row, true});

    inputs
        .add(new Object[]{"array_slice_int(intArray, 1, 2)", Collections.singletonList("intArray"), row, new int[]{2}});
    inputs.add(new Object[]{"array_slice_int(integerArray, 1, 2)", Collections
        .singletonList("integerArray"), row, new int[]{2}});
    inputs.add(new Object[]{"array_slice_string(stringArray, 1, 2)", Collections
        .singletonList("stringArray"), row, new String[]{"2"}});

    inputs.add(new Object[]{"array_distinct_int(intArray)", Collections
        .singletonList("intArray"), row, new int[]{3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_distinct_int(integerArray)", Collections
        .singletonList("integerArray"), row, new int[]{3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_distinct_string(stringArray)", Collections
        .singletonList("stringArray"), row, new String[]{"3", "2", "10", "6", "1", "12"}});

    inputs.add(new Object[]{"array_remove_int(intArray, 2)", Collections
        .singletonList("intArray"), row, new int[]{3, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_remove_int(integerArray, 2)", Collections
        .singletonList("integerArray"), row, new int[]{3, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_remove_string(stringArray, 2)", Collections
        .singletonList("stringArray"), row, new String[]{"3", "10", "6", "1", "12"}});

    inputs.add(new Object[]{"array_union_int(intArray, intArray)", Lists.newArrayList("intArray",
        "intArray"), row, new int[]{3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_union_int(integerArray, integerArray)", Lists.newArrayList("integerArray",
        "integerArray"), row, new int[]{3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_union_string(stringArray, stringArray)", Lists.newArrayList("stringArray",
        "stringArray"), row, new String[]{"3", "2", "10", "6", "1", "12"}});

    inputs.add(new Object[]{"array_concat_int(intArray, intArray)", Lists.newArrayList("intArray",
        "intArray"), row, new int[]{3, 2, 10, 6, 1, 12, 3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_concat_int(integerArray, integerArray)", Lists.newArrayList("integerArray",
        "integerArray"), row, new int[]{3, 2, 10, 6, 1, 12, 3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_concat_string(stringArray, stringArray)", Lists.newArrayList("stringArray",
        "stringArray"), row, new String[]{"3", "2", "10", "6", "1", "12", "3", "2", "10", "6", "1", "12"}});
    return inputs.toArray(new Object[0][]);
  }
}
