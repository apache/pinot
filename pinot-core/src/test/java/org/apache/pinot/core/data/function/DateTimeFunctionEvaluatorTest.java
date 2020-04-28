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
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the Pinot inbuilt transform functions in {@link DateTimeFunctions} which perform date time conversion
 */
public class DateTimeFunctionEvaluatorTest {

  @Test(dataProvider = "dateTimeFunctionsTestDataProvider")
  public void testExpressionWithColumn(String transformFunction, List<String> arguments, GenericRow row, Object result)
      throws Exception {
    DefaultFunctionEvaluator evaluator = new DefaultFunctionEvaluator(transformFunction);
    Assert.assertEquals(evaluator.getArguments(), arguments);
    Assert.assertEquals(evaluator.evaluate(row), result);
  }

  @DataProvider(name = "dateTimeFunctionsTestDataProvider")
  public Object[][] dateTimeFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    // toEpochHours
    GenericRow row1 = new GenericRow();
    row1.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochHours(timestamp)", Lists.newArrayList("timestamp"), row1, 440479L});

    // toEpochMinutes w/ bucketing fixed
    GenericRow row2 = new GenericRow();
    row2.putValue("millis", 1585724400000L);
    inputs.add(new Object[]{"toEpochMinutes(millis, 5)", Lists.newArrayList("millis"), row2, 5285748L});

    return inputs.toArray(new Object[0][]);
  }
}
