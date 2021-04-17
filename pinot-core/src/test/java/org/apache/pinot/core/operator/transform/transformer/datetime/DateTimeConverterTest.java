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
package org.apache.pinot.core.operator.transform.transformer.datetime;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DateTimeConverterTest {

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "testDateTimeConversion")
  public void testDateTimeConversion(String inputFormat, String outputFormat, String outputGranularity, Object input,
      Object expected) {
    BaseDateTimeTransformer converter =
        DateTimeTransformerFactory.getDateTimeTransformer(inputFormat, outputFormat, outputGranularity);
    int length;
    Object output;
    if (expected instanceof long[]) {
      length = ((long[]) expected).length;
      output = new long[length];
    } else {
      length = ((String[]) expected).length;
      output = new String[length];
    }
    converter.transform(input, output, length);
    Assert.assertEquals(output, expected);
  }

  @DataProvider(name = "testDateTimeConversion")
  public Object[][] testDateTimeConversion() {
    List<Object[]> entries = new ArrayList<>();

    /*************** Epoch to Epoch ***************/
    {
      // Test bucketing to 15 minutes
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505898300000L /* 20170920T02:05:00 */, 1505898960000L /* 20170920T02:16:00 */ };
      long[] expected =
          {1505898000000L /* 20170920T02:00:00 */, 1505898000000L /* 20170920T02:00:00 */, 1505898900000L /* 20170920T02:15:00 */ };
      entries.add(new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "15:MINUTES", input, expected});
    }
    {
      // Test input which should create no change
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505898300000L /* 20170920T02:05:00 */, 1505898960000L /* 20170920T02:16:00 */ };
      long[] expected =
          {1505898000000L /* 20170920T02:00:00 */, 1505898300000L /* 20170920T02:05:00 */, 1505898960000L /* 20170920T02:16:00 */ };
      entries.add(new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", input, expected});
    }
    {
      // Test conversion from millis to hours
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505898300000L /* 20170920T02:05:00 */, 1505902560000L /* 20170920T03:16:00 */ };
      long[] expected =
          {418305L /* 20170920T02:00:00 */, 418305L /* 20170920T02:00:00 */, 418306L /* 20170920T03:00:00 */ };
      entries.add(new Object[]{"1:MILLISECONDS:EPOCH", "1:HOURS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion from 5 minutes to hours
      long[] input =
          {5019660L /* 20170920T02:00:00 */, 5019661L /* 20170920T02:05:00 */, 5019675L /* 20170920T03:15:00 */ };
      long[] expected =
          {418305L /* 20170920T02:00:00 */, 418305L /* 20170920T02:00:00 */, 418306L /* 20170920T03:00:00 */ };
      entries.add(new Object[]{"5:MINUTES:EPOCH", "1:HOURS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion from 5 minutes to millis and bucketing to hours
      long[] input =
          {5019660L /* 20170920T02:00:00 */, 5019661L /* 20170920T02:05:00 */, 5019675L /* 20170920T03:15:00 */ };
      long[] expected =
          {1505898000000L /* 20170920T02:00:00 */, 1505898000000L /* 20170920T02:00:00 */, 1505901600000L /* 20170920T03:00:00 */ };
      entries.add(new Object[]{"5:MINUTES:EPOCH", "1:MILLISECONDS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion to non-java time unit WEEKS
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505199600000L /* 20170912T00:00:00 */, 1504257300000L /* 20170901T00:20:00 */ };
      long[] expected = {2489L, 2488L, 2487L};
      entries.add(new Object[]{"1:MILLISECONDS:EPOCH", "1:WEEKS:EPOCH", "1:MILLISECONDS", input, expected});
    }

    /*************** Epoch to SDF ***************/
    {
      // Test conversion from millis since epoch to simple date format (UTC)
      long[] input =
          {1505890800000L /* 20170920T00:00:00 */, 1505962800000L /* 20170920T20:00:00 */, 1505985360000L /* 20170921T02:16:00 */ };
      String[] expected = {"20170920", "20170921", "20170921"};
      entries
          .add(new Object[]{"1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (Pacific timezone)
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505952000000L /* 20170920T17:00:00 */, 1505962800000L /* 20170920T20:00:00 */ };
      String[] expected = {"20170920", "20170920", "20170920"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Los_Angeles)", "1:DAYS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (IST)
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505941200000L /* 20170920T14:00:00 */, 1505962800000L /* 20170920T20:00:00 */ };
      String[] expected = {"20170920", "20170921", "20170921"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(IST)", "1:DAYS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (Pacific timezone)
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505952000000L /* 20170920T17:00:00 */, 1505962800000L /* 20170920T20:00:00 */ };
      String[] expected = {"2017092002", "2017092017", "2017092020"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH tz(America/Los_Angeles)", "1:HOURS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (East Coast timezone)
      long[] input =
          {1505898000000L /* 20170920T02:00:00 */, 1505941200000L /* 20170920T14:00:00 */, 1505970000000L /* 20170920T22:00:00 */ };
      String[] expected = {"2017092005", "2017092017", "2017092101"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH tz(America/New_York)", "1:HOURS", input, expected});
    }

    // additional granularity tests
    {
      // Test conversion from millis since epoch to simple date format (America/Denver timezone with 15 second granualrity)
      long[] input =
          {1523560598000L /* 20180412T19:16:38 */, 1523560589000L /* 20180412T19:16:29 */, 1523560632000L /* 20180412T19:17:12 */ };
      String[] expected = {"2018-04-12 13:16:30.000", "2018-04-12 13:16:15.000", "2018-04-12 13:17:00.000"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Denver)", "15:SECONDS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (America/Denver timezone with 3 minute granualrity)
      long[] input =
          {1523560598000L /* 20180412T19:16:38 */, 1523560708000L /* 20180412T19:18:28 */, 1523561708000L /* 20180412T19:35:08 */ };
      String[] expected = {"2018-04-12 13:15:00.000", "2018-04-12 13:18:00.000", "2018-04-12 13:33:00.000"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Denver)", "3:MINUTES", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (America/Denver timezone with 12 hour granualrity)
      long[] input =
          {1523560598000L /* 20180412T19:16:38 */, 1523460502000L /* 20180411T15:28:22 */, 1523430205000L /* 20180411T07:03:25 */ };
      String[] expected = {"2018-04-12 12:00:00.000", "2018-04-11 00:00:00.000", "2018-04-11 00:00:00.000"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Denver)", "12:HOURS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (America/Denver timezone with 5 day granualrity)
      long[] input =
          {1523560598000L /* 20180412T19:16:38 */, 1524160502000L /* 20180419T17:55:02 */, 1522230205000L /* 20180328T09:43:25 */ };
      String[] expected = {"2018-04-10 00:00:00.000", "2018-04-15 00:00:00.000", "2018-03-25 00:00:00.000"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Denver)", "5:DAYS", input, expected});
    }
    {
      // Test conversion from millis since epoch to simple date format (America/Los_Angeles timezone with 1 day granualrity)
      long[] input = {1524045600000L /* 20180418T10:00:00 */, 1524013200000L /* 20180418T01:00:00 */ };
      String[] expected = {"2018-04-18 00:00:00.000", "2018-04-17 00:00:00.000"};
      entries.add(
          new Object[]{"1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Los_Angeles)", "1:DAYS", input, expected});
    }

    /*************** SDF to Epoch ***************/
    {
      // Test conversion from simple date format to millis since epoch
      String[] input =
          {"20170920" /* 20170920T00:00:00 */, "20170601" /* 20170601T00:00:00 */, "20170921" /* 20170921T00:00:00 */ };
      long[] expected =
          {1505865600000L /* 20170920T00:00:00 */, 1496275200000L /* 20170601T00:00:00 */, 1505952000000L /* 20170921T00:00:00 */ };
      entries
          .add(new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS:EPOCH", "1:DAYS", input, expected});
    }
    {
      // Test conversion from simple date format (East Coast timezone) to millis since epoch
      // Converted to
      String[] input =
          {"20170920" /* 20170920T00:00:00 */, "20170601" /* 20170601T00:00:00 */, "20170921" /* 20170921T00:00:00 */ };
      long[] expected =
          {1505865600000L /* 20170920T00:00:00 */, 1496275200000L /* 20170601T00:00:00 */, 1505952000000L /* 20170921T00:00:00 */ };
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/New_York)", "1:MILLISECONDS:EPOCH", "1:DAYS", input, expected});
    }
    {
      // Test conversion from simple date format (East Coast timezone) to millis since epoch
      // Converted to
      String[] input =
          {"2017092013" /* 20170920T00:00:00 */, "2017092001" /* 20170601T00:00:00 */, "2017092000" /* 20170921T00:00:00 */ };
      long[] expected =
          {1505926800000L /* 20170920T13:00:00 Eastern */, 1505883600000L /* 20170920T01:00:00 Eastern */, 1505880000000L /* 20170920T00:00:00 Eastern */ };
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH tz(America/New_York)", "1:MILLISECONDS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion from simple date format with special characters to millis since epoch
      String[] input = {"2017092013 America/New_York", "2017092004 Asia/Kolkata", "2017092000 America/Los_Angeles"};
      long[] expected =
          {1505926800000L /* 20170920T10:00:00 UTC */, 1505858400000L /* 20170919T22:00:00 UTC */, 1505890800000L /* 20170920T00:00:00 UTC */ };
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH ZZZ", "1:MILLISECONDS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion from simple date format with special characters to millis since epoch
      String[] input = {"8/7/2017 1 AM", "12/27/2016 11 PM", "8/7/2017 12 AM", "8/7/2017 12 PM"};
      long[] expected = {1502067600000L, 1482879600000L, 1502064000000L, 1502107200000L};
      entries.add(
          new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h a", "1:MILLISECONDS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion from simple date format with special characters to millis since epoch, with bucketing
      String[] input =
          {"8/7/2017 1:00:00 AM", "12/27/2016 11:20:00 PM", "8/7/2017 12:45:50 AM", "8/7/2017 12:00:01 PM"};
      long[] expected = {1502067600000L, 1482879600000L, 1502064000000L, 1502107200000L};
      entries.add(
          new Object[]{"1:SECONDS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:HOURS", input, expected});
    }
    {
      // Test conversion from simple date format with special characters to millis since epoch, without bucketing
      String[] input =
          {"8/7/2017 1:00:00 AM", "12/27/2016 11:20:00 PM", "8/7/2017 12:45:50 AM", "8/7/2017 12:00:01 PM"};
      long[] expected = {1502067600000L, 1482880800000L, 1502066750000L, 1502107201000L};
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", input, expected});
    }

    /*************** SDF to SDF ***************/
    {
      // Test conversion from simple date format to another simple date format
      String[] input = {"8/7/2017 1:00:00 AM", "12/27/2016 11:20:00 PM", "8/7/2017 12:45:50 AM"};
      String[] expected = {"20170807", "20161227", "20170807"};
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS", input, expected});
    }
    {
      // Test conversion from simple date format with timezone to another simple date format
      String[] input = {"20170920 America/Chicago", "20170919 America/Los_Angeles", "20170921 Asia/Kolkata"};
      String[] expected = {"20170920", "20170919", "20170920"};
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd ZZZ", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS", input, expected});
    }
    {
      // Test conversion from simple date format with timezone to another simple date format with timezone
      String[] input = {"20170920 America/New_York", "20170919 America/Los_Angeles", "20170921 Asia/Kolkata"};
      String[] expected = {"20170919", "20170919", "20170920"};
      entries.add(
          new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd ZZZ", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Chicago)", "1:MILLISECONDS", input, expected});
    }

    // additional granularity tests
    {
      // Test conversion from simple date format to another simple date format (America/Denver timezone with 15 second granualrity)
      String[] input = {"20180412T19:16:38", "20180412T19:16:29", "20180412T19:17:12"};
      String[] expected = {"2018-04-12 13:16:30.000", "2018-04-12 13:16:15.000", "2018-04-12 13:17:00.000"};
      entries.add(
          new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd'T'HH:mm:ss", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Denver)", "15:SECONDS", input, expected});
    }
    {
      // Test conversion from simple date format to another simple date format (America/Denver timezone with 5 day granualrity)
      String[] input = {"20180412T19:16:38", "20180419T17:55:02", "20180328T09:43:25"};
      String[] expected = {"2018-04-10 00:00:00.000", "2018-04-15 00:00:00.000", "2018-03-25 00:00:00.000"};
      entries.add(
          new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd'T'HH:mm:ss", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Denver)", "5:DAYS", input, expected});
    }
    {
      // Test conversion from simple date format to another simple date format (America/Los_Angeles timezone with 1 day granualrity)
      String[] input = {"20180418T10:00:00", "20180418T01:00:00"};
      String[] expected = {"2018-04-18 00:00:00.000", "2018-04-17 00:00:00.000"};
      entries.add(
          new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd'T'HH:mm:ss", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS tz(America/Los_Angeles)", "1:DAYS", input, expected});
    }

    return entries.toArray(new Object[entries.size()][]);
  }
}
