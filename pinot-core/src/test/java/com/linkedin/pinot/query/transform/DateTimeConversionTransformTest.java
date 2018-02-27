/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.datetime.convertor.DateTimeConvertor;
import com.linkedin.pinot.common.datetime.convertor.DateTimeConvertorFactory;
import com.linkedin.pinot.core.operator.docvalsets.ConstantBlockValSet;
import com.linkedin.pinot.core.operator.transform.function.DateTimeConversionTransform;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link DateTimeConversionTransform}.
 * Checks if the bucketing is right, and the final format is right
 */
public class DateTimeConversionTransformTest {


  // Test conversion of a dateTimeColumn value from a format to millis
  @Test(dataProvider = "testDateTimeConversionTransformDataProvider")
  public void testDateTimeConversionTransform(String inputFormat, String outputFormat,
      String outputGranularity, Object input, int numRows, DataType inputDataType, long[] expected) {

    TransformTestUtils.TestBlockValSet inputBlockSet =
        new TransformTestUtils.TestBlockValSet(input, numRows, inputDataType);
    ConstantBlockValSet inputFormatBlockSet = new ConstantBlockValSet(inputFormat, numRows);
    ConstantBlockValSet outputFormatBlockSet = new ConstantBlockValSet(outputFormat, numRows);
    ConstantBlockValSet outputGranularityBlockSet = new ConstantBlockValSet(outputGranularity, numRows);

    DateTimeConversionTransform function = new DateTimeConversionTransform();
    long[] actual =
        function.transform(numRows, inputBlockSet, inputFormatBlockSet, outputFormatBlockSet, outputGranularityBlockSet);

    for (int i = 0; i < numRows; i++) {
      Assert.assertEquals(actual[i], expected[i]);
    }
  }

  @DataProvider(name = "testDateTimeConversionTransformDataProvider")
  public Object[][] provideTestDateTimeConversionTransformData() {
    long[] input = null;
    long[] expected = null;
    String[] stringInput = null;

    List<Object[]> entries = new ArrayList<>();

    /****** Epoch to Epoch ***************/

    // Testing bucketing to 15 minutes
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505898300000L /* 20170920T02:05:00 */;
    input[2] = 1505898960000L /* 20170920T02:16:00 */;
    expected[0] = 1505898000000L /* 20170920T02:00:00 */;
    expected[1] = 1505898000000L; /* 20170920T02:00:00 */
    expected[2] = 1505898900000L; /* 20170920T02:15:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "15:MINUTES", input, 3, DataType.LONG, expected
    });

    // testing inputs which should create no change
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505898300000L /* 20170920T02:05:00 */;
    input[2] = 1505898960000L /* 20170920T02:16:00 */;
    expected[0] = 1505898000000L /* 20170920T02:00:00 */;
    expected[1] = 1505898300000L; /* 20170920T02:05:00 */
    expected[2] = 1505898960000L; /* 20170920T02:16:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", input, 3, DataType.LONG, expected
    });

    // testing conversion from millis to hours and bucketing
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505898300000L /* 20170920T02:05:00 */;
    input[2] = 1505902560000L /* 20170920T03:16:00 */;
    expected[0] = 418305L /* 20170920T02:00:00 */;
    expected[1] = 418305L; /* 20170920T02:00:00 */
    expected[2] = 418306L; /* 20170920T03:00:00 */
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:HOURS:EPOCH", "1:HOURS", input, 3, DataType.LONG, expected
    });

    // testing conversion from n minutes epoch to millis and bucketing to hours
    input = new long[3];
    expected = new long[3];
    input[0] = 5019660L /* 20170920T02:00:00 */;
    input[1] = 5019661L /* 20170920T02:05:00 */;
    input[2] = 5019675L /* 20170920T03:15:00 */;
    expected[0] = 1505898000000L /* 20170920T02:00:00 */;
    expected[1] = 1505898000000L; /* 20170920T02:00:00 */
    expected[2] = 1505901600000L; /* 20170920T03:00:00 */
    entries.add(new Object[] {
        "5:MINUTES:EPOCH", "1:MILLISECONDS:EPOCH", "1:HOURS", input, 3, DataType.LONG, expected
    });

    input = new long[3];
    expected = new long[3];
    input[0] = 5019660L /* 20170920T02:00:00 */;
    input[1] = 5019661L /* 20170920T02:05:00 */;
    input[2] = 5019675L /* 20170920T03:15:00 */;
    expected[0] = 418305L /* 20170920T02:00:00 */;
    expected[1] = 418305L; /* 20170920T02:00:00 */
    expected[2] = 418306L; /* 20170920T03:00:00 */
    entries.add(new Object[] {
        "5:MINUTES:EPOCH", "1:HOURS:EPOCH", "1:HOURS", input, 3, DataType.LONG, expected
    });

    // testing conversion to non-java timeunit WEEKS
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505199600000L /* 20170912T00:00:00 */;
    input[2] = 1504257300000L /* 20170901T00:20:00 */;
    expected[0] = 2489L;
    expected[1] = 2488L;
    expected[2] = 2487L;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:WEEKS:EPOCH", "1:MILLISECONDS", input, 3, DataType.LONG, expected
    });

    /****** Epoch to SDF ***************/
    // Testing conversion from epoch millis to simple date format
    // Converted according to UTC buckets
    input = new long[3];
    expected = new long[3];
    input[0] = 1505890800000L; /* 20170920T00:00:00 */
    input[1] = 1505962800000L; /* 20170920T20:00:00 */
    input[2] = 1505985360000L; /* 20170921T02:16:00 */
    expected[0] = 20170920L;
    expected[1] = 20170921L;
    expected[2] = 20170921L;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from epoch millis to simple date format
    // Converted according to Pacific timezone
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505952000000L /* 20170920T17:00:00 */;
    input[2] = 1505962800000L /* 20170920T20:00:00 */;
    expected[0] = 20170920;
    expected[1] = 20170920;
    expected[2] = 20170920;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Los_Angeles)", "1:DAYS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from epoch millis to simple date format
    // Converted according to IST
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505941200000L /* 20170920T14:00:00 */;
    input[2] = 1505962800000L /* 20170920T20:00:00 */;
    expected[0] = 20170920;
    expected[1] = 20170921;
    expected[2] = 20170921;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(IST)", "1:DAYS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from epoch millis to simple date format
    // Converted according to Pacific timezone
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505952000000L /* 20170920T17:00:00 */;
    input[2] = 1505962800000L /* 20170920T20:00:00 */;
    expected[0] = 2017092002;
    expected[1] = 2017092017;
    expected[2] = 2017092020;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH tz(America/Los_Angeles)", "1:DAYS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from epoch millis to simple date format
    // Converted according to East Coast timezone
    input = new long[3];
    expected = new long[3];
    input[0] = 1505898000000L /* 20170920T02:00:00 */;
    input[1] = 1505941200000L /* 20170920T14:00:00 */;
    input[2] = 1505970000000L /* 20170920T22:00:00 */;
    expected[0] = 2017092005;
    expected[1] = 2017092017;
    expected[2] = 2017092101;
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH tz(America/New_York)", "1:DAYS", input, 3, DataType.LONG, expected
    });


    /****** SDF to Epoch ***************/
    // Testing conversion from sdf to epoch millis
    input = new long[3];
    expected = new long[3];
    expected[0] = 1505865600000L; /* 20170920T00:00:00 */
    expected[1] = 1496275200000L; /* 20170601T00:00:00 */
    expected[2] = 1505952000000L; /* 20170921T00:00:00 */
    input[0] = 20170920L; /* 20170920T00:00:00 */
    input[1] = 20170601L; /* 20170601T00:00:00 */
    input[2] = 20170921L; /* 20170921T00:00:00 */
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS:EPOCH", "1:DAYS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from sdf to epoch millis
    // Converted to East Coast timezone
    input = new long[3];
    expected = new long[3];
    expected[0] = 1505865600000L; /* 20170920T00:00:00 */
    expected[1] = 1496275200000L; /* 20170601T00:00:00 */
    expected[2] = 1505952000000L; /* 20170921T00:00:00 */
    input[0] = 20170920L; /* 20170920T00:00:00 */
    input[1] = 20170601L; /* 20170601T00:00:00 */
    input[2] = 20170921L; /* 20170921T00:00:00 */
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/New_York)", "1:MILLISECONDS:EPOCH", "1:DAYS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from sdf to epoch millis
    // Converted to East Coast timezone
    input = new long[3];
    expected = new long[3];
    expected[0] = 1505926800000L; /* 20170920T13:00:00 Eastern*/
    expected[1] = 1505883600000L; /* 20170920T01:00:00 Eastern*/
    expected[2] = 1505880000000L; /* 20170920T00:00:00 Eastern */
    input[0] = 2017092013L;
    input[1] = 2017092001L;
    input[2] = 2017092000L;
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH tz(America/New_York)", "1:MILLISECONDS:EPOCH", "1:HOURS", input, 3, DataType.LONG, expected
    });

    // Testing conversion from sdf to epoch millis
    // Converted to East Coast timezone
    stringInput = new String[3];
    expected = new long[3];
    expected[0] = 1505926800000L; /* 20170920T10:00:00 UTC*/
    expected[1] = 1505858400000L; /* 20170919T22:00:00 UTC*/
    expected[2] = 1505890800000L; /* 20170920T00:00:00 UTC */
    stringInput[0] = "2017092013 America/New_York";
    stringInput[1] = "2017092004 Asia/Kolkata";
    stringInput[2] = "2017092000 America/Los_Angeles";
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMddHH ZZZ", "1:MILLISECONDS:EPOCH", "1:HOURS", stringInput, 3, DataType.STRING, expected
    });

    // Testing conversion from sdf with special characters to epoch millis
    stringInput = new String[4];
    expected = new long[4];
    stringInput[0] = "8/7/2017 1 AM";
    stringInput[1] = "12/27/2016 11 PM";
    stringInput[2] = "8/7/2017 12 AM";
    stringInput[3] = "8/7/2017 12 PM";
    expected[0] = 1502067600000L;
    expected[1] = 1482879600000L;
    expected[2] = 1502064000000L;
    expected[3] = 1502107200000L;
    entries.add(new Object[] {
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h a", "1:MILLISECONDS:EPOCH", "1:HOURS", stringInput, 4, DataType.STRING, expected
    });

    // Testing conversion from sdf with special characters to epoch millis, with bucketing
    stringInput = new String[4];
    expected = new long[4];
    stringInput[0] = "8/7/2017 1:00:00 AM";
    stringInput[1] = "12/27/2016 11:20:00 PM";
    stringInput[2] = "8/7/2017 12:45:50 AM";
    stringInput[3] = "8/7/2017 12:00:01 PM";
    expected[0] = 1502067600000L;
    expected[1] = 1482879600000L;
    expected[2] = 1502064000000L;
    expected[3] = 1502107200000L;
    entries.add(new Object[] {
        "1:SECONDS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:HOURS", stringInput, 4, DataType.STRING, expected
    });

    // Testing conversion from sdf to epoch millis, with no bucketing
    stringInput = new String[4];
    expected = new long[4];
    stringInput[0] = "8/7/2017 1:00:00 AM";
    stringInput[1] = "12/27/2016 11:20:00 PM";
    stringInput[2] = "8/7/2017 12:45:50 AM";
    stringInput[3] = "8/7/2017 12:00:01 PM";
    expected[0] = 1502067600000L;
    expected[1] = 1482880800000L;
    expected[2] = 1502066750000L;
    expected[3] = 1502107201000L;
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", stringInput, 4, DataType.STRING, expected
    });


    /****** SDF to SDF ***************/
    // Testing sdf to another sdf
    stringInput = new String[3];
    expected = new long[3];
    stringInput[0] = "8/7/2017 1:00:00 AM";
    stringInput[1] = "12/27/2016 11:20:00 PM";
    stringInput[2] = "8/7/2017 12:45:50 AM";
    expected[0] = 20170807;
    expected[1] = 20161227;
    expected[2] = 20170807;
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS", stringInput, 3, DataType.STRING, expected
    });

    // Testing sdf with timezone to another sdf
    stringInput = new String[3];
    expected = new long[3];
    stringInput[0] = "20170920 America/Chicago";
    stringInput[1] = "20170919 America/Los_Angeles";
    stringInput[2] = "20170921 Asia/Kolkata";
    expected[0] = 20170920;
    expected[1] = 20170919;
    expected[2] = 20170920;
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd ZZZ", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS", stringInput, 3, DataType.STRING, expected
    });

    // Testing sdf with timezone in format, to another sdf with timezone input
    stringInput = new String[3];
    expected = new long[3];
    stringInput[0] = "20170920 America/New_York";
    stringInput[1] = "20170919 America/Los_Angeles";
    stringInput[2] = "20170921 Asia/Kolkata";
    expected[0] = 20170919;
    expected[1] = 20170919;
    expected[2] = 20170920;
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd ZZZ", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Chicago)", "1:MILLISECONDS", stringInput, 3, DataType.STRING, expected
    });

    return entries.toArray(new Object[entries.size()][]);
  }



  // Test conversion of a dateTimeColumn value from a format to another
  @Test(dataProvider = "testEvaluateDataProvider")
  public void testDateTimeConversionEvaluation(String inputFormat, String outputFormat,
      String outputGranularity, Object inputValue, Object expectedValue) {
    DateTimeConvertor convertor = DateTimeConvertorFactory.getDateTimeConvertorFromFormats(inputFormat, outputFormat, outputGranularity);
    long actual = convertor.convert(inputValue);
    Assert.assertEquals(actual, expectedValue);
  }

  @DataProvider(name = "testEvaluateDataProvider")
  public Object[][] provideTestEvaluateData() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "15:MINUTES", 1505898300000L, 1505898000000L
    });

    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", 1505898000000L, 1505898000000L
    });

    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:HOURS:EPOCH", "1:HOURS", 1505902560000L, 418306L
    });

    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS", 1505898300000L, 20170920L
    });

    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:MILLISECONDS:EPOCH", "1:DAYS", 20170601, 1496275200000L
    });

    entries.add(new Object[] {
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h a", "1:MILLISECONDS:EPOCH", "1:HOURS", "8/7/2017 1 AM", 1502067600000L
    });

    entries.add(new Object[] {
        "1:SECONDS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:HOURS", "12/27/2016 11:20:00 PM", 1482879600000L
    });

    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", "8/7/2017 12:45:50 AM", 1502066750000L
    });

    entries.add(new Object[] {
        "5:MINUTES:EPOCH", "1:MILLISECONDS:EPOCH", "1:HOURS", 5019675L, 1505901600000L
    });

    entries.add(new Object[] {
        "5:MINUTES:EPOCH", "1:HOURS:EPOCH", "1:HOURS", 5019661L, 418305L
    });

    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", "1:WEEKS:EPOCH", "1:MILLISECONDS", 1505898000000L, 2489L
    });

    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", null, 0L
    });

    return entries.toArray(new Object[entries.size()][]);
  }

}
