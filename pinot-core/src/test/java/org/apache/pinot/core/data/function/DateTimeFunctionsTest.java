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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.local.function.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the Pinot inbuilt transform functions
 */
public class DateTimeFunctionsTest {
  private static final ZoneOffset WEIRD_ZONE = ZoneOffset.ofHoursMinutes(7, 9);
  private static final DateTimeZone WEIRD_DATE_TIME_ZONE = DateTimeZone.forID(WEIRD_ZONE.getId());
  private static final DateTime WEIRD_TIMESTAMP = new DateTime(2021, 2, 1, 20, 12, 12, 123, WEIRD_DATE_TIME_ZONE);
  private static final String WEIRD_TIMESTAMP_ISO8601_STRING = "2021-02-01T20:12:12.123+07:09";

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
    inputs.add(new Object[]{"round(\"timestamp\", 900000)", Lists.newArrayList("timestamp"), row0_0, 1578684600000L});

    // toEpochSeconds (with type conversion)
    GenericRow row1_0 = new GenericRow();
    row1_0.putValue("timestamp", 1578685189000.0);
    inputs.add(new Object[]{"toEpochSeconds(\"timestamp\")", Lists.newArrayList("timestamp"), row1_0, 1578685189L});

    // toEpochSeconds w/ rounding (with type conversion)
    GenericRow row1_1 = new GenericRow();
    row1_1.putValue("timestamp", "1578685189000");
    inputs.add(
        new Object[]{"toEpochSecondsRounded(\"timestamp\", 10)", Lists.newArrayList("timestamp"), row1_1, 1578685180L});

    // toEpochSeconds w/ bucketing (with underscore in function name)
    GenericRow row1_2 = new GenericRow();
    row1_2.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"to_epoch_seconds_bucket(\"timestamp\", 10)", Lists.newArrayList(
        "timestamp"), row1_2, 157868518L});

    // toEpochMinutes
    GenericRow row2_0 = new GenericRow();
    row2_0.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochMinutes(\"timestamp\")", Lists.newArrayList("timestamp"), row2_0, 26311419L});

    // toEpochMinutes w/ rounding
    GenericRow row2_1 = new GenericRow();
    row2_1.putValue("timestamp", 1578685189000L);
    inputs.add(
        new Object[]{"toEpochMinutesRounded(\"timestamp\", 15)", Lists.newArrayList("timestamp"), row2_1, 26311410L});

    // toEpochMinutes w/ bucketing
    GenericRow row2_2 = new GenericRow();
    row2_2.putValue("timestamp", 1578685189000L);
    inputs.add(
        new Object[]{"toEpochMinutesBucket(\"timestamp\", 15)", Lists.newArrayList("timestamp"), row2_2, 1754094L});

    // toEpochHours
    GenericRow row3_0 = new GenericRow();
    row3_0.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochHours(\"timestamp\")", Lists.newArrayList("timestamp"), row3_0, 438523L});

    // toEpochHours w/ rounding
    GenericRow row3_1 = new GenericRow();
    row3_1.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochHoursRounded(\"timestamp\", 2)", Lists.newArrayList("timestamp"), row3_1, 438522L});

    // toEpochHours w/ bucketing
    GenericRow row3_2 = new GenericRow();
    row3_2.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochHoursBucket(\"timestamp\", 2)", Lists.newArrayList("timestamp"), row3_2, 219261L});

    // toEpochDays
    GenericRow row4_0 = new GenericRow();
    row4_0.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochDays(\"timestamp\")", Lists.newArrayList("timestamp"), row4_0, 18271L});

    // toEpochDays w/ rounding
    GenericRow row4_1 = new GenericRow();
    row4_1.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochDaysRounded(\"timestamp\", 7)", Lists.newArrayList("timestamp"), row4_1, 18270L});

    // toEpochDays w/ bucketing
    GenericRow row4_2 = new GenericRow();
    row4_2.putValue("timestamp", 1578685189000L);
    inputs.add(new Object[]{"toEpochDaysBucket(\"timestamp\", 7)", Lists.newArrayList("timestamp"), row4_2, 2610L});

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
    inputs.add(new Object[]{"fromDateTime(dateTime, 'EEE MMM dd HH:mm:ss ZZZ yyyy')", Lists.newArrayList(
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

  @Test
  public void testDateTrunc() {
    GenericRow row = new GenericRow();
    row.putValue("epochMillis", 1612296732123L);
    List<String> arguments = Lists.newArrayList("epochMillis");

    // name variations
    testFunction("datetrunc('millisecond', epochMillis, 'MILLISECONDS')", arguments, row, 1612296732123L);
    testFunction("date_trunc('MILLISECOND', epochMillis, 'MILLISECONDS')", arguments, row, 1612296732123L);
    testFunction("dateTrunc('millisecond', epochMillis, 'MILLISECONDS')", arguments, row, 1612296732123L);
    testFunction("DATE_TRUNC('SECOND', epochMillis, 'MILLISECONDS')", arguments, row, 1612296732000L);

    // MILLISECONDS to various
    testFunction("datetrunc('millisecond', epochMillis, 'MILLISECONDS')", arguments, row, 1612296732123L);
    testFunction("datetrunc('second', epochMillis, 'MILLISECONDS')", arguments, row, 1612296732000L);
    testFunction("datetrunc('minute', epochMillis, 'MILLISECONDS')", arguments, row, 1612296720000L);
    testFunction("datetrunc('hour', epochMillis, 'MILLISECONDS')", arguments, row, 1612296000000L);
    testFunction("datetrunc('day', epochMillis, 'MILLISECONDS')", arguments, row, 1612224000000L);
    testFunction("datetrunc('week', epochMillis, 'MILLISECONDS')", arguments, row, 1612137600000L);
    testFunction("datetrunc('month', epochMillis, 'MILLISECONDS')", arguments, row, 1612137600000L);
    testFunction("datetrunc('quarter', epochMillis, 'MILLISECONDS')", arguments, row, 1609459200000L);
    testFunction("datetrunc('year', epochMillis, 'MILLISECONDS')", arguments, row, 1609459200000L);

    // SECONDS to various
    row.clear();
    row.putValue("epochSeconds", 1612296732);
    arguments = Lists.newArrayList("epochSeconds");
    testFunction("datetrunc('millisecond', epochSeconds, 'SECONDS')", arguments, row, 1612296732L);
    testFunction("datetrunc('second', epochSeconds, 'SECONDS')", arguments, row, 1612296732L);
    testFunction("datetrunc('minute', epochSeconds, 'SECONDS')", arguments, row, 1612296720L);
    testFunction("datetrunc('hour', epochSeconds, 'SECONDS')", arguments, row, 1612296000L);
    testFunction("datetrunc('day', epochSeconds, 'SECONDS')", arguments, row, 1612224000L);
    testFunction("datetrunc('week', epochSeconds, 'SECONDS')", arguments, row, 1612137600L);
    testFunction("datetrunc('month', epochSeconds, 'SECONDS')", arguments, row, 1612137600L);
    testFunction("datetrunc('quarter', epochSeconds, 'SECONDS')", arguments, row, 1609459200L);
    testFunction("datetrunc('year', epochSeconds, 'SECONDS')", arguments, row, 1609459200L);

    // MINUTES to various
    row.clear();
    row.putValue("epochMinutes", 26871612);
    arguments = Lists.newArrayList("epochMinutes");
    testFunction("datetrunc('millisecond', epochMinutes, 'MINUTES')", arguments, row, 26871612L);
    testFunction("datetrunc('second', epochMinutes, 'MINUTES')", arguments, row, 26871612L);
    testFunction("datetrunc('minute', epochMinutes, 'MINUTES')", arguments, row, 26871612L);
    testFunction("datetrunc('hour', epochMinutes, 'MINUTES')", arguments, row, 26871600L);
    testFunction("datetrunc('day', epochMinutes, 'MINUTES')", arguments, row, 26870400L);
    testFunction("datetrunc('week', epochMinutes, 'MINUTES')", arguments, row, 26868960L);
    testFunction("datetrunc('month', epochMinutes, 'MINUTES')", arguments, row, 26868960L);
    testFunction("datetrunc('quarter', epochMinutes, 'MINUTES')", arguments, row, 26824320L);
    testFunction("datetrunc('year', epochMinutes, 'MINUTES')", arguments, row, 26824320L);

    // MILLISECONDS to various with timezone
    row.clear();
    row.putValue("epochMillis", iso8601ToUtcEpochMillis(WEIRD_TIMESTAMP_ISO8601_STRING));
    arguments = Lists.newArrayList("epochMillis");
    String weirdDateTimeZoneid = WEIRD_DATE_TIME_ZONE.getID();
    DateTime result = WEIRD_TIMESTAMP;
    testFunction("datetrunc('millisecond', epochMillis, 'MILLISECONDS', '" + weirdDateTimeZoneid + "')", arguments, row,
        result.getMillis());
    result = result.withMillisOfSecond(0);
    testFunction("datetrunc('second', epochMillis, 'MILLISECONDS', '" + weirdDateTimeZoneid + "')", arguments, row,
        result.getMillis());
    result = result.withSecondOfMinute(0);
    testFunction("datetrunc('minute', epochMillis, 'MILLISECONDS', '" + weirdDateTimeZoneid + "')", arguments, row,
        result.getMillis());
    result = result.withMinuteOfHour(0);
    testFunction("datetrunc('hour', epochMillis, 'MILLISECONDS', '" + weirdDateTimeZoneid + "')", arguments, row,
        result.getMillis());
    result = result.withHourOfDay(0);
    testFunction("datetrunc('day', epochMillis, 'MILLISECONDS', '" + weirdDateTimeZoneid + "')", arguments, row,
        result.getMillis());
  }

  private static long iso8601ToUtcEpochMillis(String iso8601) {
    DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser().withOffsetParsed();
    return formatter.parseDateTime(iso8601).getMillis();
  }
}
