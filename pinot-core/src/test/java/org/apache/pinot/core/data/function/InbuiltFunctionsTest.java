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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the Pinot inbuilt transform functions
 */
public class InbuiltFunctionsTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "dateTimeFunctionsDataProvider")
  public void testDateTimeFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "dateTimeFunctionsDataProvider")
  public Object[][] dateTimeFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    // round epoch millis to nearest 15 minutes
    GenericRow row0_0 = new GenericRow();
    row0_0.putValue("timestamp", 1578685189000L);
    // round to 15 minutes, but keep in milliseconds: Fri Jan 10 2020 19:39:49 becomes Fri Jan 10 2020 19:30:00
    inputs.add(new Object[]{"round(timestamp, 900000)", Lists.newArrayList("timestamp"), row0_0, 1578684600000L});

    // toEpochSeconds (with type conversion)
    GenericRow row1_0 = new GenericRow();
    row1_0.putValue("timestamp", 1578685189000.0);
    inputs.add(new Object[]{"toEpochSeconds(timestamp)", Lists.newArrayList("timestamp"), row1_0, 1578685189L});

    // toEpochSeconds w/ rounding (with type conversion)
    GenericRow row1_1 = new GenericRow();
    row1_1.putValue("timestamp", "1578685189000");
    inputs.add(
        new Object[]{"toEpochSecondsRounded(timestamp, 10)", Lists.newArrayList("timestamp"), row1_1, 1578685180L});

    // toEpochSeconds w/ bucketing (with underscore in function name)
    GenericRow row1_2 = new GenericRow();
    row1_2.putValue("timestamp", 1578685189000L);
    inputs.add(
        new Object[]{"to_epoch_seconds_bucket(timestamp, 10)", Lists.newArrayList("timestamp"), row1_2, 157868518L});

    // toEpochMinutes
    GenericRow row2_0 = new GenericRow();
    row2_0.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochMinutes(timestamp)", Lists.newArrayList("timestamp"), row2_0, 26311419L});

    // toEpochMinutes w/ rounding
    GenericRow row2_1 = new GenericRow();
    row2_1.putValue("timestamp", 1578685189000L);
    inputs
        .add(new Object[]{"toEpochMinutesRounded(timestamp, 15)", Lists.newArrayList("timestamp"), row2_1, 26311410L});

    // toEpochMinutes w/ bucketing
    GenericRow row2_2 = new GenericRow();
    row2_2.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochMinutesBucket(timestamp, 15)", Lists.newArrayList("timestamp"), row2_2, 1754094L});

    // toEpochHours
    GenericRow row3_0 = new GenericRow();
    row3_0.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochHours(timestamp)", Lists.newArrayList("timestamp"), row3_0, 438523L});

    // toEpochHours w/ rounding
    GenericRow row3_1 = new GenericRow();
    row3_1.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochHoursRounded(timestamp, 2)", Lists.newArrayList("timestamp"), row3_1, 438522L});

    // toEpochHours w/ bucketing
    GenericRow row3_2 = new GenericRow();
    row3_2.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochHoursBucket(timestamp, 2)", Lists.newArrayList("timestamp"), row3_2, 219261L});

    // toEpochDays
    GenericRow row4_0 = new GenericRow();
    row4_0.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochDays(timestamp)", Lists.newArrayList("timestamp"), row4_0, 18271L});

    // toEpochDays w/ rounding
    GenericRow row4_1 = new GenericRow();
    row4_1.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochDaysRounded(timestamp, 7)", Lists.newArrayList("timestamp"), row4_1, 18270L});

    // toEpochDays w/ bucketing
    GenericRow row4_2 = new GenericRow();
    row4_2.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochDaysBucket(timestamp, 7)", Lists.newArrayList("timestamp"), row4_2, 2610L});

    // fromEpochDays
    GenericRow row5_0 = new GenericRow();
    row5_0.putValue("daysSinceEpoch", 14000);
    inputs.add(
        new Object[]{"fromEpochDays(daysSinceEpoch)", Lists.newArrayList("daysSinceEpoch"), row5_0, 1209600000000L});

    // fromEpochDays w/ bucketing
    GenericRow row5_1 = new GenericRow();
    row5_1.putValue("sevenDaysSinceEpoch", 2000);
    inputs.add(new Object[]{"fromEpochDaysBucket(sevenDaysSinceEpoch, 7)", Lists.newArrayList(
        "sevenDaysSinceEpoch"), row5_1, 1209600000000L});

    // fromEpochHours
    GenericRow row6_0 = new GenericRow();
    row6_0.putValue("hoursSinceEpoch", 336000);
    inputs.add(
        new Object[]{"fromEpochHours(hoursSinceEpoch)", Lists.newArrayList("hoursSinceEpoch"), row6_0, 1209600000000L});

    // fromEpochHours w/ bucketing
    GenericRow row6_1 = new GenericRow();
    row6_1.putValue("twoHoursSinceEpoch", 168000);
    inputs.add(new Object[]{"fromEpochHoursBucket(twoHoursSinceEpoch, 2)", Lists.newArrayList(
        "twoHoursSinceEpoch"), row6_1, 1209600000000L});

    // fromEpochMinutes
    GenericRow row7_0 = new GenericRow();
    row7_0.putValue("minutesSinceEpoch", 20160000);
    inputs.add(new Object[]{"fromEpochMinutes(minutesSinceEpoch)", Lists.newArrayList(
        "minutesSinceEpoch"), row7_0, 1209600000000L});

    // fromEpochMinutes w/ bucketing
    GenericRow row7_1 = new GenericRow();
    row7_1.putValue("fifteenMinutesSinceEpoch", 1344000);
    inputs.add(new Object[]{"fromEpochMinutesBucket(fifteenMinutesSinceEpoch, 15)", Lists.newArrayList(
        "fifteenMinutesSinceEpoch"), row7_1, 1209600000000L});

    // fromEpochSeconds
    GenericRow row8_0 = new GenericRow();
    row8_0.putValue("secondsSinceEpoch", 1209600000L);
    inputs.add(new Object[]{"fromEpochSeconds(secondsSinceEpoch)", Lists.newArrayList(
        "secondsSinceEpoch"), row8_0, 1209600000000L});

    // fromEpochSeconds w/ bucketing
    GenericRow row8_1 = new GenericRow();
    row8_1.putValue("tenSecondsSinceEpoch", 120960000L);
    inputs.add(new Object[]{"fromEpochSecondsBucket(tenSecondsSinceEpoch, 10)", Lists.newArrayList(
        "tenSecondsSinceEpoch"), row8_1, 1209600000000L});

    // nested
    GenericRow row9_0 = new GenericRow();
    row9_0.putValue("hoursSinceEpoch", 336000);
    inputs.add(new Object[]{"toEpochDays(fromEpochHours(hoursSinceEpoch))", Lists.newArrayList(
        "hoursSinceEpoch"), row9_0, 14000L});

    GenericRow row9_1 = new GenericRow();
    row9_1.putValue("fifteenSecondsSinceEpoch", 80640000L);
    inputs.add(
        new Object[]{"toEpochMinutesBucket(fromEpochSecondsBucket(fifteenSecondsSinceEpoch, 15), 10)", Lists.newArrayList(
            "fifteenSecondsSinceEpoch"), row9_1, 2016000L});

    // toDateTime simple
    GenericRow row10_0 = new GenericRow();
    row10_0.putValue("dateTime", 98697600000L);
    inputs.add(new Object[]{"toDateTime(dateTime, 'yyyyMMdd')", Lists.newArrayList("dateTime"), row10_0, "19730216"});

    // toDateTime complex
    GenericRow row10_1 = new GenericRow();
    row10_1.putValue("dateTime", 1234567890000L);
    inputs.add(new Object[]{"toDateTime(dateTime, 'MM/yyyy/dd HH:mm:ss')", Lists.newArrayList(
        "dateTime"), row10_1, "02/2009/13 23:31:30"});

    // toDateTime with timezone
    GenericRow row10_2 = new GenericRow();
    row10_2.putValue("dateTime", 7897897890000L);
    inputs.add(new Object[]{"toDateTime(dateTime, 'EEE MMM dd HH:mm:ss ZZZ yyyy')", Lists.newArrayList(
        "dateTime"), row10_2, "Mon Apr 10 20:31:30 UTC 2220"});

    // fromDateTime simple
    GenericRow row11_0 = new GenericRow();
    row11_0.putValue("dateTime", "19730216");
    inputs
        .add(new Object[]{"fromDateTime(dateTime, 'yyyyMMdd')", Lists.newArrayList("dateTime"), row11_0, 98668800000L});

    // fromDateTime complex
    GenericRow row11_1 = new GenericRow();
    row11_1.putValue("dateTime", "02/2009/13 15:31:30");
    inputs.add(new Object[]{"fromDateTime(dateTime, 'MM/yyyy/dd HH:mm:ss')", Lists.newArrayList(
        "dateTime"), row11_1, 1234539090000L});

    // fromDateTime with timezone
    GenericRow row11_2 = new GenericRow();
    row11_2.putValue("dateTime", "Mon Aug 24 12:36:46 America/Los_Angeles 2009");
    inputs.add(new Object[]{"fromDateTime(dateTime, \"EEE MMM dd HH:mm:ss ZZZ yyyy\")", Lists.newArrayList(
        "dateTime"), row11_2, 1251142606000L});

    // timezone_hour and timezone_minute
    List<String> expectedArguments = Collections.singletonList("tz");
    GenericRow row12_0 = new GenericRow();
    row12_0.putValue("tz", "UTC");
    inputs.add(new Object[]{"timezone_hour(tz)", expectedArguments, row12_0, 0});
    inputs.add(new Object[]{"timezone_minute(tz)", expectedArguments, row12_0, 0});

    GenericRow row12_1 = new GenericRow();
    row12_1.putValue("tz", "Asia/Shanghai");
    inputs.add(new Object[]{"timezone_hour(tz)", expectedArguments, row12_1, 8});
    inputs.add(new Object[]{"timezone_minute(tz)", expectedArguments, row12_1, 0});

    GenericRow row12_2 = new GenericRow();
    row12_2.putValue("tz", "Pacific/Marquesas");
    inputs.add(new Object[]{"timezone_hour(tz)", expectedArguments, row12_2, 14});
    inputs.add(new Object[]{"timezone_minute(tz)", expectedArguments, row12_2, 30});

    GenericRow row12_3 = new GenericRow();
    row12_3.putValue("tz", "Etc/GMT+12");
    inputs.add(new Object[]{"timezone_hour(tz)", expectedArguments, row12_3, 12});
    inputs.add(new Object[]{"timezone_minute(tz)", expectedArguments, row12_3, 0});

    GenericRow row12_4 = new GenericRow();
    row12_4.putValue("tz", "Etc/GMT+1");
    inputs.add(new Object[]{"timezone_hour(tz)", expectedArguments, row12_4, 23});
    inputs.add(new Object[]{"timezone_minute(tz)", expectedArguments, row12_4, 0});

    // Convenience extraction functions
    expectedArguments = Collections.singletonList("millis");
    GenericRow row13_0 = new GenericRow();
    // Sat May 23 2020 22:23:13.123 UTC
    row13_0.putValue("millis", 1590272593123L);

    inputs.add(new Object[]{"year(millis)", expectedArguments, row13_0, 2020});
    inputs.add(new Object[]{"year_of_week(millis)", expectedArguments, row13_0, 2020});
    inputs.add(new Object[]{"yow(millis)", expectedArguments, row13_0, 2020});
    inputs.add(new Object[]{"quarter(millis)", expectedArguments, row13_0, 2});
    inputs.add(new Object[]{"month(millis)", expectedArguments, row13_0, 5});
    inputs.add(new Object[]{"week(millis)", expectedArguments, row13_0, 21});
    inputs.add(new Object[]{"week_of_year(millis)", expectedArguments, row13_0, 21});
    inputs.add(new Object[]{"day_of_year(millis)", expectedArguments, row13_0, 144});
    inputs.add(new Object[]{"doy(millis)", expectedArguments, row13_0, 144});
    inputs.add(new Object[]{"day(millis)", expectedArguments, row13_0, 23});
    inputs.add(new Object[]{"day_of_month(millis)", expectedArguments, row13_0, 23});
    inputs.add(new Object[]{"day_of_week(millis)", expectedArguments, row13_0, 6});
    inputs.add(new Object[]{"dow(millis)", expectedArguments, row13_0, 6});
    inputs.add(new Object[]{"hour(millis)", expectedArguments, row13_0, 22});
    inputs.add(new Object[]{"minute(millis)", expectedArguments, row13_0, 23});
    inputs.add(new Object[]{"second(millis)", expectedArguments, row13_0, 13});
    inputs.add(new Object[]{"millisecond(millis)", expectedArguments, row13_0, 123});

    expectedArguments = Arrays.asList("millis", "tz");
    GenericRow row13_1 = new GenericRow();
    // Sat May 23 2020 15:23:13.123 America/Los_Angeles
    row13_1.putValue("millis", 1590272593123L);
    row13_1.putValue("tz", "America/Los_Angeles");

    inputs.add(new Object[]{"year(millis, tz)", expectedArguments, row13_1, 2020});
    inputs.add(new Object[]{"year_of_week(millis, tz)", expectedArguments, row13_1, 2020});
    inputs.add(new Object[]{"yow(millis, tz)", expectedArguments, row13_1, 2020});
    inputs.add(new Object[]{"quarter(millis, tz)", expectedArguments, row13_1, 2});
    inputs.add(new Object[]{"month(millis, tz)", expectedArguments, row13_1, 5});
    inputs.add(new Object[]{"week(millis, tz)", expectedArguments, row13_1, 21});
    inputs.add(new Object[]{"week_of_year(millis, tz)", expectedArguments, row13_1, 21});
    inputs.add(new Object[]{"day_of_year(millis, tz)", expectedArguments, row13_1, 144});
    inputs.add(new Object[]{"doy(millis, tz)", expectedArguments, row13_1, 144});
    inputs.add(new Object[]{"day(millis, tz)", expectedArguments, row13_1, 23});
    inputs.add(new Object[]{"day_of_month(millis, tz)", expectedArguments, row13_1, 23});
    inputs.add(new Object[]{"day_of_week(millis, tz)", expectedArguments, row13_1, 6});
    inputs.add(new Object[]{"dow(millis, tz)", expectedArguments, row13_1, 6});
    inputs.add(new Object[]{"hour(millis, tz)", expectedArguments, row13_1, 15});
    inputs.add(new Object[]{"minute(millis, tz)", expectedArguments, row13_1, 23});
    inputs.add(new Object[]{"second(millis, tz)", expectedArguments, row13_1, 13});
    inputs.add(new Object[]{"millisecond(millis, tz)", expectedArguments, row13_1, 123});

    return inputs.toArray(new Object[0][]);
  }

  @Test(dataProvider = "jsonFunctionsDataProvider")
  public void testJsonFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "jsonFunctionsDataProvider")
  public Object[][] jsonFunctionsDataProvider()
      throws IOException {
    List<Object[]> inputs = new ArrayList<>();

    // toJsonMapStr
    GenericRow row0 = new GenericRow();
    String jsonStr = "{\"k1\":\"foo\",\"k2\":\"bar\"}";
    row0.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"toJsonMapStr(jsonMap)", Lists.newArrayList("jsonMap"), row0, jsonStr});

    GenericRow row1 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row1.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"toJsonMapStr(jsonMap)", Lists.newArrayList("jsonMap"), row1, jsonStr});

    GenericRow row2 = new GenericRow();
    jsonStr = "{\"k1\":\"foo\",\"k2\":\"bar\"}";
    row2.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row2, jsonStr});

    GenericRow row3 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row3.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row3, jsonStr});

    GenericRow row4 = new GenericRow();
    jsonStr = "[{\"one\":1,\"two\":\"too\"},{\"one\":11,\"two\":\"roo\"}]";
    row4.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row4, jsonStr});

    GenericRow row5 = new GenericRow();
    jsonStr =
        "[{\"one\":1,\"two\":{\"sub1\":1.1,\"sub2\":1.2},\"three\":[\"a\",\"b\"]},{\"one\":11,\"two\":{\"sub1\":11.1,\"sub2\":11.2},\"three\":[\"aa\",\"bb\"]}]";
    row5.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row5, jsonStr});

    GenericRow row6 = new GenericRow();
    jsonStr =
        "[{\"one\":1,\"two\":{\"sub1\":1.1,\"sub2\":1.2},\"three\":[\"a\",\"b\"]},{\"one\":11,\"two\":{\"sub1\":11.1,\"sub2\":11.2},\"three\":[\"aa\",\"bb\"]}]";
    row6.putValue("jsonPathArray", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{"json_path_array(jsonPathArray, '$.[*].one')", Lists.newArrayList(
        "jsonPathArray"), row6, new Object[]{1, 11}});

    GenericRow row7 = new GenericRow();
    jsonStr =
        "[{\"one\":1,\"two\":{\"sub1\":1.1,\"sub2\":1.2},\"three\":[\"a\",\"b\"]},{\"one\":11,\"two\":{\"sub1\":11.1,\"sub2\":11.2},\"three\":[\"aa\",\"bb\"]}]";
    row7.putValue("jsonPathArray", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{"json_path_array(jsonPathArray, '$.[*].three')", Lists.newArrayList(
        "jsonPathArray"), row7, new Object[]{Arrays.asList("a", "b"), Arrays.asList("aa", "bb")}});

    GenericRow row8 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row8.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_path_string(jsonPathString, '$.k3')", Lists.newArrayList(
        "jsonPathString"), row8, "{\"sub1\":10,\"sub2\":1.0}"});

    GenericRow row9 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row9.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_path_string(jsonPathString, '$.k4')", Lists.newArrayList(
        "jsonPathString"), row9, "baz"});

    GenericRow row10 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row10.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_path_long(jsonPathString, '$.k3.sub1')", Lists.newArrayList(
        "jsonPathString"), row10, 10L});

    GenericRow row11 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row11.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_path_double(jsonPathString, '$.k3.sub2')", Lists.newArrayList(
        "jsonPathString"), row11, 1.0});
    return inputs.toArray(new Object[0][]);
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

    inputs.add(new Object[]{"array_reverse_int(intArray)", Collections.singletonList(
        "intArray"), row, new int[]{12, 1, 6, 10, 2, 3}});
    inputs.add(new Object[]{"array_reverse_int(integerArray)", Collections.singletonList(
        "integerArray"), row, new int[]{12, 1, 6, 10, 2, 3}});
    inputs.add(new Object[]{"array_reverse_int(stringArray)", Collections.singletonList(
        "stringArray"), row, new int[]{12, 1, 6, 10, 2, 3}});

    inputs.add(new Object[]{"array_reverse_string(intArray)", Collections.singletonList(
        "intArray"), row, new String[]{"12", "1", "6", "10", "2", "3"}});
    inputs.add(new Object[]{"array_reverse_string(integerArray)", Collections.singletonList(
        "integerArray"), row, new String[]{"12", "1", "6", "10", "2", "3"}});
    inputs.add(new Object[]{"array_reverse_string(stringArray)", Collections.singletonList(
        "stringArray"), row, new String[]{"12", "1", "6", "10", "2", "3"}});

    inputs.add(new Object[]{"array_sort_int(intArray)", Collections.singletonList(
        "intArray"), row, new int[]{1, 2, 3, 6, 10, 12}});
    inputs.add(new Object[]{"array_sort_int(integerArray)", Collections.singletonList(
        "integerArray"), row, new int[]{1, 2, 3, 6, 10, 12}});
    inputs.add(new Object[]{"array_sort_int(stringArray)", Collections.singletonList(
        "stringArray"), row, new int[]{1, 2, 3, 6, 10, 12}});

    inputs.add(new Object[]{"array_sort_string(intArray)", Collections.singletonList(
        "intArray"), row, new String[]{"1", "10", "12", "2", "3", "6"}});
    inputs.add(new Object[]{"array_sort_string(integerArray)", Collections.singletonList(
        "integerArray"), row, new String[]{"1", "10", "12", "2", "3", "6"}});
    inputs.add(new Object[]{"array_sort_string(stringArray)", Collections.singletonList(
        "stringArray"), row, new String[]{"1", "10", "12", "2", "3", "6"}});

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
    inputs.add(new Object[]{"array_slice_int(integerArray, 1, 2)", Collections.singletonList(
        "integerArray"), row, new int[]{2}});
    inputs.add(new Object[]{"array_slice_string(stringArray, 1, 2)", Collections.singletonList(
        "stringArray"), row, new String[]{"2"}});

    inputs.add(new Object[]{"array_distinct_int(intArray)", Collections.singletonList(
        "intArray"), row, new int[]{3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_distinct_int(integerArray)", Collections.singletonList(
        "integerArray"), row, new int[]{3, 2, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_distinct_string(stringArray)", Collections.singletonList(
        "stringArray"), row, new String[]{"3", "2", "10", "6", "1", "12"}});

    inputs.add(new Object[]{"array_remove_int(intArray, 2)", Collections.singletonList(
        "intArray"), row, new int[]{3, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_remove_int(integerArray, 2)", Collections.singletonList(
        "integerArray"), row, new int[]{3, 10, 6, 1, 12}});
    inputs.add(new Object[]{"array_remove_string(stringArray, 2)", Collections.singletonList(
        "stringArray"), row, new String[]{"3", "10", "6", "1", "12"}});

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
