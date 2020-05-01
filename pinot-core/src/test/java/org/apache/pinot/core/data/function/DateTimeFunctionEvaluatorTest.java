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

    // toEpochSeconds
    GenericRow row1 = new GenericRow();
    row1.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochSeconds(timestamp)", Lists.newArrayList("timestamp"), row1, 1585724400L});

    // toEpochSeconds w/ bucketing
    GenericRow row2 = new GenericRow();
    row2.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochSecondsBucket(timestamp, 10)", Lists.newArrayList("timestamp"), row2, 158572440L});

    // toEpochMinutes
    GenericRow row3 = new GenericRow();
    row3.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochMinutes(timestamp)", Lists.newArrayList("timestamp"), row3, 26428740L});

    // toEpochMinutes w/ bucketing
    GenericRow row4 = new GenericRow();
    row4.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochMinutesBucket(timestamp, 15)", Lists.newArrayList("timestamp"), row4, 1761916L});

    // toEpochHours
    GenericRow row5 = new GenericRow();
    row5.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochHours(timestamp)", Lists.newArrayList("timestamp"), row5, 440479L});

    // toEpochHours w/ bucketing
    GenericRow row6 = new GenericRow();
    row6.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochHoursBucket(timestamp, 2)", Lists.newArrayList("timestamp"), row6, 220239L});

    // toEpochDays
    GenericRow row7 = new GenericRow();
    row7.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochDays(timestamp)", Lists.newArrayList("timestamp"), row7, 18353L});

    // toEpochDays w/ bucketing
    GenericRow row8 = new GenericRow();
    row8.putValue("timestamp", 1585724400000L);
    inputs.add(new Object[]{"toEpochDaysBucket(timestamp, 7)", Lists.newArrayList("timestamp"), row8, 2621L});

    // fromEpochDays
    GenericRow row9 = new GenericRow();
    row9.putValue("daysSinceEpoch", 14000);
    inputs
        .add(new Object[]{"fromEpochDays(daysSinceEpoch)", Lists.newArrayList("daysSinceEpoch"), row9, 1209600000000L});

    // fromEpochDays w/ bucketing
    GenericRow row10 = new GenericRow();
    row10.putValue("sevenDaysSinceEpoch", 2000);
    inputs.add(new Object[]{"fromEpochDaysBucket(sevenDaysSinceEpoch, 7)", Lists.newArrayList(
        "sevenDaysSinceEpoch"), row10, 1209600000000L});

    // fromEpochHours
    GenericRow row11 = new GenericRow();
    row11.putValue("hoursSinceEpoch", 336000);
    inputs
        .add(new Object[]{"fromEpochHours(hoursSinceEpoch)", Lists.newArrayList("hoursSinceEpoch"), row11, 1209600000000L});

    // fromEpochHours w/ bucketing
    GenericRow row12 = new GenericRow();
    row12.putValue("twoHoursSinceEpoch", 168000);
    inputs.add(new Object[]{"fromEpochHoursBucket(twoHoursSinceEpoch, 2)", Lists.newArrayList(
        "twoHoursSinceEpoch"), row12, 1209600000000L});

    // fromEpochMinutes
    GenericRow row13 = new GenericRow();
    row13.putValue("minutesSinceEpoch", 20160000);
    inputs
        .add(new Object[]{"fromEpochMinutes(minutesSinceEpoch)", Lists.newArrayList("minutesSinceEpoch"), row13, 1209600000000L});

    // fromEpochMinutes w/ bucketing
    GenericRow row14 = new GenericRow();
    row14.putValue("fifteenMinutesSinceEpoch", 1344000);
    inputs.add(new Object[]{"fromEpochMinutesBucket(fifteenMinutesSinceEpoch, 15)", Lists.newArrayList(
        "fifteenMinutesSinceEpoch"), row14, 1209600000000L});

    // fromEpochSeconds
    GenericRow row15 = new GenericRow();
    row15.putValue("secondsSinceEpoch", 1209600000L);
    inputs
        .add(new Object[]{"fromEpochSeconds(secondsSinceEpoch)", Lists.newArrayList("secondsSinceEpoch"), row15, 1209600000000L});

    // fromEpochSeconds w/ bucketing
    GenericRow row16 = new GenericRow();
    row16.putValue("tenSecondsSinceEpoch", 120960000L);
    inputs.add(new Object[]{"fromEpochSecondsBucket(tenSecondsSinceEpoch, 10)", Lists.newArrayList(
        "tenSecondsSinceEpoch"), row16, 1209600000000L});

    // nested
    GenericRow row17 = new GenericRow();
    row17.putValue("hoursSinceEpoch", 336000);
    inputs.add(new Object[]{"toEpochDays(fromEpochHours(hoursSinceEpoch))", Lists.newArrayList(
        "hoursSinceEpoch"), row17, 14000L});

    GenericRow row18 = new GenericRow();
    row18.putValue("fifteenSecondsSinceEpoch", 80640000L);
    inputs.add(new Object[]{"toEpochMinutesBucket(fromEpochSecondsBucket(fifteenSecondsSinceEpoch, 15), 10)", Lists.newArrayList(
        "fifteenSecondsSinceEpoch"), row18, 2016000L});

    return inputs.toArray(new Object[0][]);
  }
}
