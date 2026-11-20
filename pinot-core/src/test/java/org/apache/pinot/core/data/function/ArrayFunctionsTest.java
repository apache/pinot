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
import org.apache.pinot.common.evaluator.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/// Tests the array scalar functions
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
    row.putValue("longArray", new long[]{3L, 2L, 10L, 6L, 1L, 12L});
    row.putValue("stringArray", new String[]{"3", "2", "10", "6", "1", "12"});
    row.putValue("stringArrayWithNulls", new String[]{"3", "2", "10", "6", "1", "12", "", null});
    row.putValue("nullIntArray", null);
    row.putValue("nullStringArray", null);
    row.putValue("nullInt", null);
    row.putValue("nullString", null);

    inputs.add(new Object[]{
        "array_reverse_int(intArray)", List.of("intArray"), row, new int[]{12, 1, 6, 10, 2, 3}
    });
    inputs.add(new Object[]{
        "array_reverse_int(integerArray)", List.of("integerArray"), row, new int[]{12, 1, 6, 10, 2, 3}
    });
    inputs.add(new Object[]{
        "array_reverse_int(stringArray)", List.of("stringArray"), row, new int[]{12, 1, 6, 10, 2, 3}
    });

    inputs.add(new Object[]{
        "array_reverse_string(intArray)", List.of("intArray"), row,
        new String[]{"12", "1", "6", "10", "2", "3"}
    });
    inputs.add(new Object[]{
        "array_reverse_string(integerArray)", List.of("integerArray"), row,
        new String[]{"12", "1", "6", "10", "2", "3"}
    });
    inputs.add(new Object[]{
        "array_reverse_string(stringArray)", List.of("stringArray"), row,
        new String[]{"12", "1", "6", "10", "2", "3"}
    });

    inputs.add(new Object[]{
        "array_sort_int(intArray)", List.of("intArray"), row, new int[]{1, 2, 3, 6, 10, 12}
    });
    inputs.add(new Object[]{
        "array_sort_int(integerArray)", List.of("integerArray"), row, new int[]{1, 2, 3, 6, 10, 12}
    });
    inputs.add(new Object[]{
        "array_sort_int(stringArray)", List.of("stringArray"), row, new int[]{1, 2, 3, 6, 10, 12}
    });

    inputs.add(new Object[]{
        "array_sort_string(intArray)", List.of("intArray"), row,
        new String[]{"1", "10", "12", "2", "3", "6"}
    });
    inputs.add(new Object[]{
        "array_sort_string(integerArray)", List.of("integerArray"), row,
        new String[]{"1", "10", "12", "2", "3", "6"}
    });
    inputs.add(new Object[]{
        "array_sort_string(stringArray)", List.of("stringArray"), row,
        new String[]{"1", "10", "12", "2", "3", "6"}
    });

    inputs.add(new Object[]{"array_index_of_int(intArray, 2)", List.of("intArray"), row, 1});
    inputs.add(new Object[]{"array_index_of_int(integerArray, 2)", List.of("integerArray"), row, 1});
    inputs.add(new Object[]{"array_index_of_int(stringArray, 2)", List.of("stringArray"), row, 1});

    inputs.add(new Object[]{"array_index_of_string(intArray, '2')", List.of("intArray"), row, 1});
    inputs.add(
        new Object[]{"array_index_of_string(integerArray, '2')", List.of("integerArray"), row, 1});
    inputs
        .add(new Object[]{"array_index_of_string(stringArray, '2')", List.of("stringArray"), row, 1});

    inputs.add(new Object[]{"array_contains_int(intArray, 2)", List.of("intArray"), row, true});
    inputs
        .add(new Object[]{"array_contains_int(integerArray, 2)", List.of("integerArray"), row, true});
    inputs.add(new Object[]{"array_contains_int(stringArray, 2)", List.of("stringArray"), row, true});

    inputs.add(new Object[]{"array_contains_string(intArray, '2')", List.of("intArray"), row, true});
    inputs.add(
        new Object[]{"array_contains_string(integerArray, '2')", List.of("integerArray"), row, true});
    inputs.add(
        new Object[]{"array_contains_string(stringArray, '2')", List.of("stringArray"), row, true});

    inputs
        .add(new Object[]{"array_slice_int(intArray, 1, 2)", List.of("intArray"), row, new int[]{2}});
    inputs.add(new Object[]{
        "array_slice_int(integerArray, 1, 2)", List.of("integerArray"), row, new int[]{2}
    });
    inputs.add(new Object[]{
        "array_slice_long(longArray, 1, 3)", List.of("longArray"), row, new long[]{2L, 10L}
    });
    inputs.add(new Object[]{
        "array_slice_string(stringArray, 1, 2)", List.of("stringArray"), row, new String[]{"2"}
    });

    inputs.add(new Object[]{
        "array_distinct_int(intArray)", List.of("intArray"), row, new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_distinct_int(integerArray)", List.of("integerArray"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_distinct_string(stringArray)", List.of("stringArray"), row,
        new String[]{"3", "2", "10", "6", "1", "12"}
    });

    inputs.add(new Object[]{
        "array_remove_int(intArray, 2)", List.of("intArray"), row, new int[]{3, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_remove_int(integerArray, 2)", List.of("integerArray"), row, new int[]{3, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_remove_string(stringArray, 2)", List.of("stringArray"), row,
        new String[]{"3", "10", "6", "1", "12"}
    });

    inputs.add(new Object[]{
        "array_union_int(intArray, intArray)", Lists.newArrayList("intArray", "intArray"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_union_int(integerArray, integerArray)", Lists.newArrayList("integerArray", "integerArray"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_union_string(stringArray, stringArray)", Lists.newArrayList("stringArray", "stringArray"), row,
        new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_union_int_ignore_nulls(array_concat_int(intArray, intArray), nullIntArray)",
        List.of("intArray", "intArray", "nullIntArray"), row, new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_union_int_ignore_nulls(nullIntArray, intArray)", List.of("nullIntArray", "intArray"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_union_int_ignore_nulls(nullIntArray, nullIntArray)", List.of("nullIntArray", "nullIntArray"), row, null
    });
    inputs.add(new Object[]{
        "array_union_string_ignore_nulls(array_concat_string(stringArray, stringArray), nullStringArray)",
        List.of("stringArray", "stringArray", "nullStringArray"), row, new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_union_string_ignore_nulls(nullStringArray, stringArray)", List.of("nullStringArray", "stringArray"),
        row, new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_union_string_ignore_nulls(nullStringArray, nullStringArray)",
        List.of("nullStringArray", "nullStringArray"), row, null
    });

    inputs.add(new Object[]{
        "array_concat_int(intArray, intArray)", Lists.newArrayList("intArray", "intArray"), row,
        new int[]{3, 2, 10, 6, 1, 12, 3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_concat_int(integerArray, integerArray)", Lists.newArrayList("integerArray", "integerArray"), row,
        new int[]{3, 2, 10, 6, 1, 12, 3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_concat_string(stringArray, stringArray)", Lists.newArrayList("stringArray", "stringArray"), row,
        new String[]{"3", "2", "10", "6", "1", "12", "3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_concat_int_ignore_nulls(intArray, nullIntArray)", List.of("intArray", "nullIntArray"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_concat_int_ignore_nulls(nullIntArray, intArray)", List.of("nullIntArray", "intArray"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_concat_int_ignore_nulls(nullIntArray, nullIntArray)", List.of("nullIntArray", "nullIntArray"), row, null
    });
    inputs.add(new Object[]{
        "array_concat_string_ignore_nulls(stringArray, nullStringArray)", List.of("stringArray", "nullStringArray"),
        row, new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_concat_string_ignore_nulls(nullStringArray, stringArray)", List.of("nullStringArray", "stringArray"),
        row, new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_concat_string_ignore_nulls(nullStringArray, nullStringArray)",
        List.of("nullStringArray", "nullStringArray"), row, null
    });
    inputs.add(new Object[]{
        "array_push_back_int(intArray, 7)", List.of("intArray"), row,
        new int[]{3, 2, 10, 6, 1, 12, 7}
    });
    inputs.add(new Object[]{
        "array_push_back_string(stringArray, 'x')", List.of("stringArray"), row,
        new String[]{"3", "2", "10", "6", "1", "12", "x"}
    });
    inputs.add(new Object[]{
        "array_push_back_int_ignore_nulls(intArray, nullInt)", List.of("intArray", "nullInt"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_push_back_int_ignore_nulls(nullIntArray, 7)", List.of("nullIntArray"), row, new int[]{7}
    });
    inputs.add(new Object[]{
        "array_push_back_int_ignore_nulls(nullIntArray, nullInt)", List.of("nullIntArray", "nullInt"), row, null
    });
    inputs.add(new Object[]{
        "array_push_back_string_ignore_nulls(stringArray, nullString)", List.of("stringArray", "nullString"), row,
        new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_push_back_string_ignore_nulls(nullStringArray, 'x')", List.of("nullStringArray"), row, new String[]{"x"}
    });
    inputs.add(new Object[]{
        "array_push_back_string_ignore_nulls(nullStringArray, nullString)", List.of("nullStringArray", "nullString"),
        row, null
    });
    inputs.add(new Object[]{
        "array_push_front_int(intArray, 7)", List.of("intArray"), row,
        new int[]{7, 3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_push_front_string(stringArray, 'x')", List.of("stringArray"), row,
        new String[]{"x", "3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_push_front_int_ignore_nulls(intArray, nullInt)", List.of("intArray", "nullInt"), row,
        new int[]{3, 2, 10, 6, 1, 12}
    });
    inputs.add(new Object[]{
        "array_push_front_int_ignore_nulls(nullIntArray, 7)", List.of("nullIntArray"), row, new int[]{7}
    });
    inputs.add(new Object[]{
        "array_push_front_int_ignore_nulls(nullIntArray, nullInt)", List.of("nullIntArray", "nullInt"), row, null
    });
    inputs.add(new Object[]{
        "array_push_front_string_ignore_nulls(stringArray, nullString)", List.of("stringArray", "nullString"), row,
        new String[]{"3", "2", "10", "6", "1", "12"}
    });
    inputs.add(new Object[]{
        "array_push_front_string_ignore_nulls(nullStringArray, 'x')", List.of("nullStringArray"), row, new String[]{"x"}
    });
    inputs.add(new Object[]{
        "array_push_front_string_ignore_nulls(nullStringArray, nullString)", List.of("nullStringArray", "nullString"),
        row, null
    });
    inputs.add(new Object[]{
        "array_to_string(stringArray, '::')", List.of("stringArray"), row,
        "3::2::10::6::1::12"
    });
    inputs.add(new Object[]{
        "array_to_string(stringArrayWithNulls, '::', '*')", List.of("stringArrayWithNulls"), row,
        "3::2::10::6::1::12::*::*"
    });
    return inputs.toArray(new Object[0][]);
  }
}
