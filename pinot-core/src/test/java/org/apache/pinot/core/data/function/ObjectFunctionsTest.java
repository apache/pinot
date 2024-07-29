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


public class ObjectFunctionsTest {
  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "objectFunctionsDataProvider")
  public void testObjectFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "objectFunctionsDataProvider")
  public Object[][] objectFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    // Both nulls
    GenericRow nullRows = new GenericRow();
    nullRows.putValue("null0", null);
    nullRows.putValue("null1", null);
    inputs.add(new Object[]{"isDistinctFrom(null0,null1)", Lists.newArrayList("null0", "null1"), nullRows, false});
    inputs.add(new Object[]{"isNotDistinctFrom(null0,null1)", Lists.newArrayList("null0", "null1"), nullRows, true});

    // Left null
    GenericRow leftNull = new GenericRow();
    leftNull.putValue("null0", null);
    leftNull.putValue("value", 1);
    inputs.add(new Object[]{"isDistinctFrom(null0,value)", Lists.newArrayList("null0", "value"), leftNull, true});
    inputs.add(new Object[]{"isNotDistinctFrom(null0,value)", Lists.newArrayList("null0", "value"), leftNull, false});

    // Right null
    GenericRow rightNull = new GenericRow();
    rightNull.putValue("value", 1);
    rightNull.putValue("null0", null);
    inputs.add(new Object[]{"isDistinctFrom(value,null0)", Lists.newArrayList("value", "null0"), rightNull, true});
    inputs.add(new Object[]{"isNotDistinctFrom(value,null0)", Lists.newArrayList("value", "null0"), rightNull, false});

    // Both not null and not equal
    GenericRow notEqual = new GenericRow();
    notEqual.putValue("value1", 1);
    notEqual.putValue("value2", 2);
    inputs.add(new Object[]{"isDistinctFrom(value1,value2)", Lists.newArrayList("value1", "value2"), notEqual, true});
    inputs.add(
        new Object[]{"isNotDistinctFrom(value1,value2)", Lists.newArrayList("value1", "value2"), notEqual, false});

    // Both not null and equal
    GenericRow equal = new GenericRow();
    equal.putValue("value1", 1);
    equal.putValue("value2", 1);
    inputs.add(new Object[]{"isDistinctFrom(value1,value2)", Lists.newArrayList("value1", "value2"), equal, false});
    inputs.add(new Object[]{"isNotDistinctFrom(value1,value2)", Lists.newArrayList("value1", "value2"), equal, true});

    // all nulls
    GenericRow nullRows2 = new GenericRow();
    nullRows2.putValue("null0", null);
    nullRows2.putValue("null1", null);
    inputs.add(new Object[]{"coalesce(null0,null1)", Lists.newArrayList("null0", "null1"), nullRows, null});

    // one not null
    GenericRow oneValue = new GenericRow();
    oneValue.putValue("null0", null);
    oneValue.putValue("null1", null);
    oneValue.putValue("null2", null);
    oneValue.putValue("value1", 1);
    oneValue.putValue("value2", null);

    inputs.add(new Object[]{
        "coalesce(null0,null1, null2, value1, value2)", Lists.newArrayList("null0", "null1", "null2", "value1",
        "value2"), oneValue, 1
    });

    // all not null
    GenericRow allValues = new GenericRow();
    allValues.putValue("value1", "1");
    allValues.putValue("value2", 2);
    allValues.putValue("value3", 3);
    allValues.putValue("value4", 4);
    allValues.putValue("value5", 5);

    inputs.add(new Object[]{
        "coalesce(value1,value2,value3,value4,value5)", Lists.newArrayList("value1", "value2", "value3", "value4",
        "value5"), allValues, "1"
    });

    // Adding a test for case when
    GenericRow caseWhenCaseValueOne = new GenericRow();
    caseWhenCaseValueOne.putValue("value1", 1);
    inputs.add(new Object[]{
        "CASEWHEN(value1 = 1, 'one', value1 = 2, 'two', 'other')", Lists.newArrayList("value1",
        "value1"), caseWhenCaseValueOne, "one"
    });

    GenericRow caseWhenCaseValueTwo = new GenericRow();
    caseWhenCaseValueTwo.putValue("value1", 2);
    inputs.add(new Object[]{
        "CASEWHEN(value1 = 1, 'one', value1 = 2, 'two', 'other')", Lists.newArrayList("value1",
        "value1"), caseWhenCaseValueTwo, "two"
    });

    GenericRow caseWhenCaseValueThree = new GenericRow();
    caseWhenCaseValueThree.putValue("value1", 3);
    inputs.add(new Object[]{
        "CASEWHEN(value1 = 1, 'one', value1 = 2, 'two', 'other')", Lists.newArrayList("value1",
        "value1"), caseWhenCaseValueThree, "other"
    });

    GenericRow caseWhenCaseMultipleExpression = new GenericRow();
    caseWhenCaseMultipleExpression.putValue("value1", 10);
    inputs.add(new Object[]{
        "CASEWHEN(value1 = 1, 'one', value1 = 2, 'two', value1 = 3, 'three', value1 = 4, 'four', value1 = 5, 'five', "
            + "value1 = 6, 'six', value1 = 7, 'seven', value1 = 8, 'eight', value1 = 9, 'nine', value1 = 10, 'ten', "
            + "'other')", Lists.newArrayList("value1", "value1", "value1", "value1", "value1", "value1", "value1",
        "value1", "value1", "value1"), caseWhenCaseMultipleExpression, "ten"
    });

    GenericRow caseWhenCaseMultipleExpression2 = new GenericRow();
    caseWhenCaseMultipleExpression2.putValue("value1", 15);
    inputs.add(new Object[]{
        "CASEWHEN(value1 = 1, 'one', value1 = 2, 'two', value1 = 3, 'three', value1 = 4, 'four', value1 = 5, 'five', "
            + "value1 = 6, 'six', value1 = 7, 'seven', value1 = 8, 'eight', value1 = 9, 'nine', value1 = 10, 'ten', "
            + "value1 = 11, 'eleven', value1 = 12, 'twelve', value1 = 13, 'thirteen', value1 = 14, 'fourteen', value1"
            + " = 15, 'fifteen'," + "'other')", Lists.newArrayList("value1", "value1", "value1", "value1", "value1",
        "value1", "value1", "value1", "value1", "value1", "value1", "value1", "value1", "value1",
        "value1"), caseWhenCaseMultipleExpression2, "fifteen"
    });

    return inputs.toArray(new Object[0][]);
  }
}
