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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatPatternSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DateTimeConversionTransformFunctionTest extends BaseTransformFunctionTest {

  private final MutableDateTime _expBuffer = new MutableDateTime(DateTimeZone.UTC);
  private final MutableDateTime _actualBuffer = new MutableDateTime(DateTimeZone.UTC);
  private final DateTimeFormatter _formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSSZ");
  private final TestSVLongTransformFunction _firstArg = new TestSVLongTransformFunction();

  @BeforeTest
  public void setUp()
  throws Exception {
    super.setUp();
    _expBuffer.hourOfDay().roundFloor();
  }

  // tests with static data
  // convert with no bucketing time zone
  @Test
  public void testConvertEpochToEpochWith1HourBucketNoTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/20 01:20:15.002+0000",
        "2024/10/20 02:40:15.003+0000",
        "2024/10/20 03:35:45.004+0000",
        "2024/10/20 04:05:45.005+0000",
        "2024/10/20 05:12:01.006+0000",
        "2024/10/20 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 02:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000",
        "2024/10/20 05:00:00.000+0000",
        "2024/10/20 06:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:HOURS", null);

    assertLongInLongOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToEpochWith3HoursBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/20 01:20:15.002+0000",
        "2024/10/20 02:40:15.003+0000",
        "2024/10/20 03:35:45.004+0000",
        "2024/10/20 04:05:45.005+0000",
        "2024/10/20 05:12:01.006+0000",
        "2024/10/20 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 06:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "3:HOURS", null);

    assertLongInLongOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToEpochWith1DaysBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/21 00:00:00.000+0000",
        "2024/10/22 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/24 00:00:00.000+0000",
        "2024/10/25 00:00:00.000+0000",
        "2024/10/26 00:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:DAYS", null);
    assertLongInLongOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToEpochWith3DaysBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/26 00:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "3:DAYS", null);

    assertLongInLongOut(input, expected, function);
  }

  //convert with cet bucket time zone
  @Test
  public void testConvertEpochToEpochWith3HoursBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/20 01:20:15.002+0000",
        "2024/10/20 02:40:15.003+0000",
        "2024/10/20 03:35:45.004+0000",
        "2024/10/20 04:05:45.005+0000",
        "2024/10/20 05:12:01.006+0000",
        "2024/10/20 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/19 22:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "3:HOURS", "CET");

    assertLongInLongOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToEpochWith1DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/19 22:00:00.000+0000",
        "2024/10/20 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/22 22:00:00.000+0000",
        "2024/10/23 22:00:00.000+0000",
        "2024/10/24 22:00:00.000+0000",
        "2024/10/25 22:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "1:DAYS", "CET");
    assertLongInLongOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToEpochWith3DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/18 22:00:00.000+0000",
        "2024/10/18 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/24 22:00:00.000+0000",
        "2024/10/24 22:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:EPOCH", "3:DAYS", "CET");

    assertLongInLongOut(input, expected, function);
  }

  //convert to string with cet bucket time zone
  @Test
  public void testConvertEpochToStringWith3HoursBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/20 01:20:15.002+0000",
        "2024/10/20 02:40:15.003+0000",
        "2024/10/20 03:35:45.004+0000",
        "2024/10/20 04:05:45.005+0000",
        "2024/10/20 05:12:01.006+0000",
        "2024/10/20 06:15:01.007+0000",
        "2024/11/25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-20 00:00:00.000+0200",
        "2024-10-20 03:00:00.000+0200",
        "2024-10-20 03:00:00.000+0200",
        "2024-10-20 03:00:00.000+0200",
        "2024-10-20 06:00:00.000+0200",
        "2024-10-20 06:00:00.000+0200",
        "2024-10-20 06:00:00.000+0200",
        "2024-11-25 06:00:00.000+0100",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(CET)",
            "3:HOURS", "CET");

    assertLongInStrOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToStringWith1DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000",
        "2024/11/25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-20 00:00:00.000+0200",
        "2024-10-21 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-23 00:00:00.000+0200",
        "2024-10-24 00:00:00.000+0200",
        "2024-10-25 00:00:00.000+0200",
        "2024-10-26 00:00:00.000+0200",
        "2024-11-25 00:00:00.000+0100"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(CET)",
            "1:DAYS", "CET");
    assertLongInStrOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToStringWith3DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000",
        "2024/11/20 07:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-19 00:00:00.000+0200",
        "2024-10-19 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-25 00:00:00.000+0200",
        "2024-10-25 00:00:00.000+0200",
        "2024-11-19 00:00:00.000+0100",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(CET)",
            "3:DAYS", "CET");

    assertLongInStrOut(input, expected, function);
  }

  @Test
  public void testConvertEpochToStringWith3DaysBucketWithEetTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000",
        "2024/11/20 07:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-19 01:00:00.000+0300",
        "2024-10-19 01:00:00.000+0300",
        "2024-10-22 01:00:00.000+0300",
        "2024-10-22 01:00:00.000+0300",
        "2024-10-22 01:00:00.000+0300",
        "2024-10-25 01:00:00.000+0300",
        "2024-10-25 01:00:00.000+0300",
        "2024-11-19 01:00:00.000+0200",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:EPOCH", "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(EET)",
            "3:DAYS", "CET");

    assertLongInStrOut(input, expected, function);
  }

  // convert from utc string to cet string without bucket time zone
  @Test
  public void testConvertStringToStringWith1HourBucketNoTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/20 01:20:15.002+0000",
        "2024/10/20 02:40:15.003+0000",
        "2024/10/20 03:35:45.004+0000",
        "2024/10/20 04:05:45.005+0000",
        "2024/10/20 05:12:01.006+0000",
        "2024/10/20 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 02:00:00.000+0200",
        "2024/10/20 03:00:00.000+0200",
        "2024/10/20 04:00:00.000+0200",
        "2024/10/20 05:00:00.000+0200",
        "2024/10/20 06:00:00.000+0200",
        "2024/10/20 07:00:00.000+0200",
        "2024/10/20 08:00:00.000+0200"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ tz(CET)", "1:HOURS", null);

    assertStrInStrOut(input, expected, function);
  }

  @Test
  public void testConvertStringToStringWith3HoursBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/20 01:20:15.002+0000",
        "2024/10/20 02:40:15.003+0000",
        "2024/10/20 03:35:45.004+0000",
        "2024/10/20 04:05:45.005+0000",
        "2024/10/20 05:12:01.006+0000",
        "2024/10/20 06:15:01.007+0000",
        "2024/11/20 06:15:01.007+0000",
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0200",
        "2024/10/20 03:00:00.000+0200",
        "2024/10/20 03:00:00.000+0200",
        "2024/10/20 03:00:00.000+0200",
        "2024/10/20 06:00:00.000+0200",
        "2024/10/20 06:00:00.000+0200",
        "2024/10/20 06:00:00.000+0200",
        "2024/11/20 06:00:00.000+0100",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ tz(CET)", "3:HOURS", null);

    assertStrInStrOut(input, expected, function);
  }

  @Test
  public void testConvertStringToStringWith1DaysBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0200",
        "2024/10/21 00:00:00.000+0200",
        "2024/10/22 00:00:00.000+0200",
        "2024/10/23 00:00:00.000+0200",
        "2024/10/24 00:00:00.000+0200",
        "2024/10/25 00:00:00.000+0200",
        "2024/10/26 00:00:00.000+0200"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ tz(CET)", "1:DAYS", null);
    assertStrInStrOut(input, expected, function);
  }

  @Test
  public void testConvertStringToStringWith3DaysBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024/10/20 00:12:15.001+0000",
        "2024/10/21 01:20:15.002+0000",
        "2024/10/22 02:40:15.003+0000",
        "2024/10/23 03:35:45.004+0000",
        "2024/10/24 04:05:45.005+0000",
        "2024/10/25 05:12:01.006+0000",
        "2024/10/26 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/19 00:00:00.000+0000",
        "2024/10/19 00:00:00.000+0000",
        "2024/10/22 00:00:00.000+0000",
        "2024/10/22 00:00:00.000+0000",
        "2024/10/22 00:00:00.000+0000",
        "2024/10/25 00:00:00.000+0000",
        "2024/10/25 00:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy/MM/dd HH:mm:ss.SSSZ tz(UTC)", "3:DAYS", null);

    assertStrInStrOut(input, expected, function);
  }

  // convert from utc string to string with bucket time zone
  @Test
  public void testConvertStringToStringWith3HoursBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-20 01:20:15.002+0000",
        "2024-10-20 02:40:15.003+0000",
        "2024-10-20 03:35:45.004+0000",
        "2024-10-20 04:05:45.005+0000",
        "2024-10-20 05:12:01.006+0000",
        "2024-10-20 06:15:01.007+0000",
        "2024-11-25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-20 00:00:00.000+0200",
        "2024-10-20 03:00:00.000+0200",
        "2024-10-20 03:00:00.000+0200",
        "2024-10-20 03:00:00.000+0200",
        "2024-10-20 06:00:00.000+0200",
        "2024-10-20 06:00:00.000+0200",
        "2024-10-20 06:00:00.000+0200",
        "2024-11-25 06:00:00.000+0100",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(CET)",
            "3:HOURS", "CET");

    assertStrInStrOut(input, expected, function);
  }

  @Test
  public void testConvertStringToStringWith1DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-20 00:00:00.000+0200",
        "2024-10-21 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-23 00:00:00.000+0200",
        "2024-10-24 00:00:00.000+0200",
        "2024-10-25 00:00:00.000+0200",
        "2024-10-26 00:00:00.000+0200",
        "2024-11-25 00:00:00.000+0100"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(CET)",
            "1:DAYS", "CET");
    assertStrInStrOut(input, expected, function);
  }

  @Test
  public void testConvertStringToStringWith3DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-20 07:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-19 00:00:00.000+0200",
        "2024-10-19 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-22 00:00:00.000+0200",
        "2024-10-25 00:00:00.000+0200",
        "2024-10-25 00:00:00.000+0200",
        "2024-11-19 00:00:00.000+0100",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(CET)",
            "3:DAYS", "CET");

    assertStrInStrOut(input, expected, function);
  }

  @Test
  public void testConvertStringToStringWith3DaysBucketWithEetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-20 07:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024-10-19 01:00:00.000+0300",
        "2024-10-19 01:00:00.000+0300",
        "2024-10-22 01:00:00.000+0300",
        "2024-10-22 01:00:00.000+0300",
        "2024-10-22 01:00:00.000+0300",
        "2024-10-25 01:00:00.000+0300",
        "2024-10-25 01:00:00.000+0300",
        "2024-11-19 01:00:00.000+0200",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ tz(EET)",
            "3:DAYS", "CET");

    assertStrInStrOut(input, expected, function);
  }

  // convert from utc string to millis without bucket time zone
  @Test
  public void testConvertStringToEpochWith3HoursBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-20 01:20:15.002+0000",
        "2024-10-20 02:40:15.003+0000",
        "2024-10-20 03:35:45.004+0000",
        "2024-10-20 04:05:45.005+0000",
        "2024-10-20 05:12:01.006+0000",
        "2024-10-20 06:15:01.007+0000",
        "2024-11-25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 03:00:00.000+0000",
        "2024/10/20 06:00:00.000+0000",
        "2024/11/25 06:00:00.000+0000",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:EPOCH",
            "3:HOURS", null);

    assertStrInLongOut(input, expected, function);
  }

  @Test
  public void testConvertStringToEpochWith1DaysBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/21 00:00:00.000+0000",
        "2024/10/22 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/24 00:00:00.000+0000",
        "2024/10/25 00:00:00.000+0000",
        "2024/10/26 00:00:00.000+0000",
        "2024/11/25 00:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:EPOCH",
            "1:DAYS", null);
    assertStrInLongOut(input, expected, function);
  }

  @Test
  public void testConvertStringToEpochWith3DaysBucketWithoutTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-20 07:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/20 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/23 00:00:00.000+0000",
        "2024/10/26 00:00:00.000+0000",
        "2024/11/19 00:00:00.000+0000",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:EPOCH",
            "3:DAYS", null);

    assertStrInLongOut(input, expected, function);
  }

  // convert from utc string to millis with bucket time zone
  @Test
  public void testConvertStringToEpochWith3HoursBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-20 01:20:15.002+0000",
        "2024-10-20 02:40:15.003+0000",
        "2024-10-20 03:35:45.004+0000",
        "2024-10-20 04:05:45.005+0000",
        "2024-10-20 05:12:01.006+0000",
        "2024-10-20 06:15:01.007+0000",
        "2024-11-25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/19 22:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 01:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000",
        "2024/10/20 04:00:00.000+0000",
        "2024/11/25 05:00:00.000+0000",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:EPOCH",
            "3:HOURS", "CET");

    assertStrInLongOut(input, expected, function);
  }

  @Test
  public void testConvertStringToEpochWith1DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-25 06:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/19 22:00:00.000+0000",
        "2024/10/20 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/22 22:00:00.000+0000",
        "2024/10/23 22:00:00.000+0000",
        "2024/10/24 22:00:00.000+0000",
        "2024/10/25 22:00:00.000+0000",
        "2024/11/24 23:00:00.000+0000"
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:EPOCH",
            "1:DAYS", "CET");
    assertStrInLongOut(input, expected, function);
  }

  @Test
  public void testConvertStringToEpochWith3DaysBucketWithCetTimeZone() {
    String[] input = new String[]{
        "2024-10-20 00:12:15.001+0000",
        "2024-10-21 01:20:15.002+0000",
        "2024-10-22 02:40:15.003+0000",
        "2024-10-23 03:35:45.004+0000",
        "2024-10-24 04:05:45.005+0000",
        "2024-10-25 05:12:01.006+0000",
        "2024-10-26 06:15:01.007+0000",
        "2024-11-20 07:15:01.007+0000"
    };
    String[] expected = new String[]{
        "2024/10/18 22:00:00.000+0000",
        "2024/10/18 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/21 22:00:00.000+0000",
        "2024/10/24 22:00:00.000+0000",
        "2024/10/24 22:00:00.000+0000",
        "2024/11/18 23:00:00.000+0000",
    };

    DateTimeConversionTransformFunction function =
        prepareFunction("1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSSZ",
            "1:MILLISECONDS:EPOCH",
            "3:DAYS", "CET");

    assertStrInLongOut(input, expected, function);
  }

  private @NotNull DateTimeConversionTransformFunction prepareFunction(String inputFormat,
      String outputFormat, String bucketGranularity, String bucketingTimeZone) {

    DateTimeConversionTransformFunction function = new DateTimeConversionTransformFunction();
    ArrayList<TransformFunction> args = new ArrayList<>();
    args.add(_firstArg);
    args.add(strLiteral(inputFormat));
    args.add(strLiteral(outputFormat));
    args.add(strLiteral(bucketGranularity));

    if (bucketingTimeZone != null) {
      args.add(literal(DataType.STRING, bucketingTimeZone));
    }

    function.init(args, Collections.emptyMap());
    return function;
  }

  private void assertLongInLongOut(String[] input, String[] expected, DateTimeConversionTransformFunction function) {
    SingleValueTestBlock block = new SingleValueTestBlock();

    for (int i = 0; i < input.length; i++) {
      long inputMs = parse(input[i]);
      parse(expected[i]);

      _firstArg.setLongValue(inputMs);
      long[] result = function.transformToLongValuesSV(block);

      assertEquals(toDate(result), _expBuffer);
    }
  }

  private long parse(String inp) {
    int pos = _formatter.parseInto(_expBuffer, inp, 0);
    if (pos < 0) {
      throw new IllegalArgumentException("parsing of " + inp + " failed!");
    }
    return _expBuffer.getMillis();
  }

  private void assertLongInStrOut(String[] input, String[] expected, DateTimeConversionTransformFunction function) {
    SingleValueTestBlock block = new SingleValueTestBlock();

    for (int i = 0; i < input.length; i++) {
      long inputMs = parse(input[i]);

      _firstArg.setLongValue(inputMs);
      String[] result = function.transformToStringValuesSV(block);

      assertEquals(result[0], expected[i]);
    }
  }

  private void assertStrInStrOut(String[] input, String[] expected, DateTimeConversionTransformFunction function) {
    SingleValueTestBlock block = new SingleValueTestBlock();

    for (int i = 0; i < input.length; i++) {
      _firstArg.setStringValue(input[i]);
      String[] result = function.transformToStringValuesSV(block);

      assertEquals(result[0], expected[i], "position:" + i);
    }
  }

  private void assertStrInLongOut(String[] input, String[] expected, DateTimeConversionTransformFunction function) {
    SingleValueTestBlock block = new SingleValueTestBlock();

    for (int i = 0; i < input.length; i++) {
      _firstArg.setStringValue(input[i]);
      parse(expected[i]);

      long[] result = function.transformToLongValuesSV(block);

      assertEquals(toDate(result), _expBuffer, "position:" + i);
    }
  }

  private MutableDateTime toDate(long[] milliDates) {
    assertEquals(milliDates.length, 1);
    _actualBuffer.setMillis(milliDates[0]);
    _actualBuffer.setZone(DateTimeZone.UTC);
    return _actualBuffer;
  }

  private class TestSVLongTransformFunction implements TransformFunction {

    private long _longValue;
    private String _stringValue;
    private long[] _longbuffer = new long[1];
    private String[] _stringbuffer = new String[1];

    public TestSVLongTransformFunction() {
    }

    public TestSVLongTransformFunction(long value) {
      _longValue = value;
    }

    public void setLongValue(long value) {
      _longValue = value;
    }

    public void setStringValue(String value) {
      _stringValue = value;
    }

    @Override
    public String getName() {
      return "test";
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      //
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return new TransformResultMetadata(DataType.LONG, true, false);
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      return null;
    }

    @Override
    public int[] transformToDictIdsSV(ValueBlock valueBlock) {
      return new int[0];
    }

    @Override
    public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
      return new int[0][];
    }

    @Override
    public int[] transformToIntValuesSV(ValueBlock valueBlock) {
      return new int[]{(int) _longValue};
    }

    @Override
    public long[] transformToLongValuesSV(ValueBlock valueBlock) {
      return new long[]{_longValue};
    }

    @Override
    public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
      return new float[]{_longValue};
    }

    @Override
    public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
      return new double[]{_longValue};
    }

    @Override
    public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
      return new BigDecimal[0];
    }

    @Override
    public String[] transformToStringValuesSV(ValueBlock valueBlock) {
      _stringbuffer[0] = _stringValue;
      return _stringbuffer;
    }

    @Override
    public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
      return new byte[0][];
    }

    @Override
    public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
      return new int[0][];
    }

    @Override
    public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
      return new long[0][];
    }

    @Override
    public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
      return new float[0][];
    }

    @Override
    public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
      return new double[0][];
    }

    @Override
    public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
      return new String[0][];
    }

    @Override
    public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
      return new byte[0][][];
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock block) {
      return null;
    }
  }

  private class SingleValueTestBlock implements ValueBlock {

    @Override
    public int getNumDocs() {
      return 1;
    }

    @Nullable
    @Override
    public int[] getDocIds() {
      return new int[]{0};
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      return null;
    }
  }

  private static LiteralTransformFunction strLiteral(String value) {
    return new LiteralTransformFunction(new LiteralContext(DataType.STRING, value));
  }

  private static LiteralTransformFunction literal(DataType type, Object value) {
    return new LiteralTransformFunction(new LiteralContext(type, value));
  }

  // tests with random data

  interface Transformer {
    void transform(MutableDateTime dateTime);
  }

  @Test
  public void testConvertEpochToEpochWith1DayBucketingWithNoTimeZone() {
    testConvertEpochToEpochWithBucketingTimeZone("1:DAYS", null, (dateTime) -> dateTime.dayOfMonth().roundFloor());
  }

  @Test
  public void testConvertEpochToEpochWith1HoursBucketingWithNoTimeZone() {
    testConvertEpochToEpochWithBucketingTimeZone("1:HOURS", null, (dateTime) -> {
          dateTime.hourOfDay().roundFloor();
        }
    );
  }

  @Test
  public void testConvertEpochToEpochWith1DayBucketingInUTC() {
    testConvertEpochToEpochWithBucketingTimeZone("1:DAYS", "UTC", (dateTime) -> dateTime.dayOfMonth().roundFloor());
  }

  @Test
  public void testConvertEpochToEpochWith4HoursBucketingInUTC() {
    testConvertEpochToEpochWithBucketingTimeZone("4:HOURS", "UTC", (dateTime) -> {
      dateTime.setHourOfDay((dateTime.getHourOfDay() / 4) * 4);
      dateTime.hourOfDay().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToEpochWith7DaysBucketingInUTC() {
    testConvertEpochToEpochWithBucketingTimeZone("7:DAYS", "UTC", (dateTime) -> {
      dateTime.setDayOfMonth(((dateTime.getDayOfMonth() - 1) / 7) * 7 + 1);
      dateTime.dayOfMonth().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToEpochWith1DayBucketingInCET() {
    testConvertEpochToEpochWithBucketingTimeZone("1:DAYS", "CET", (dateTime) -> dateTime.dayOfMonth().roundFloor());
  }

  @Test
  public void testConvertEpochToEpochWith3HoursBucketingInCET() {
    testConvertEpochToEpochWithBucketingTimeZone("3:HOURS", "CET", (dateTime) -> {
      dateTime.setHourOfDay((dateTime.getHourOfDay() / 3) * 3);
      dateTime.hourOfDay().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToEpochWith5DaysBucketingInCET() {
    testConvertEpochToEpochWithBucketingTimeZone("5:DAYS", "CET", (dateTime) -> {
      dateTime.setDayOfMonth(((dateTime.getDayOfMonth() - 1) / 5) * 5 + 1);
      dateTime.dayOfMonth().roundFloor();
    });
  }

  private void testConvertEpochToEpochWithBucketingTimeZone(String granularity, String timeZone,
      Transformer transformer) {
    ExpressionContext expression = RequestContextUtils.getExpression(
        "dateTimeConvert(" + TIME_COLUMN + ",'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','" + granularity + "'"
            + (timeZone != null ? ", '" + timeZone + "'" : "") + ")"
    );
    TransformFunction function = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(function.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata meta = function.getResultMetadata();
    assertTrue(meta.isSingleValue());
    assertEquals(meta.getDataType(), DataType.LONG);

    MutableDateTime dateTime = new MutableDateTime();
    dateTime.setZone(timeZone != null ? DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZone)) : DateTimeZone.UTC);

    long[] expected = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      dateTime.setMillis(_timeValues[i]);
      transformer.transform(dateTime);
      expected[i] = dateTime.getMillis();
    }

    testTransformFunction(function, expected);
  }

  @Test
  public void testConvertEpochToStringWith3HoursBucketingInCETAndOutputInCET() {
    testConvertEpochToStringWithBucketingTimeZone("3:HOURS", "CET", "yyyy-MM-dd HH:mm:ss.mmm tz(CET)", (dateTime) -> {
      dateTime.setHourOfDay((dateTime.getHourOfDay() / 3) * 3);
      dateTime.hourOfDay().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToStringWith1DaysBucketingInCETAndOutputInCET() {
    testConvertEpochToStringWithBucketingTimeZone("1:DAYS", "CET", "yyyy-MM-dd HH:mm:ss.mmm tz(CET)", (dateTime) -> {
      dateTime.dayOfMonth().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToStringWith5DaysBucketingInCETAndOutputInCET() {
    testConvertEpochToStringWithBucketingTimeZone("5:DAYS", "CET", "yyyy-MM-dd HH:mm:ss.mmm tz(CET)", (dateTime) -> {
      dateTime.setDayOfMonth(((dateTime.getDayOfMonth() - 1) / 5) * 5 + 1);
      dateTime.dayOfMonth().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToStringWith3HoursBucketingInCETAndOutputInUTC() {
    testConvertEpochToStringWithBucketingTimeZone("3:HOURS", "CET", "yyyy-MM-dd HH:mm:ss.mmm tz(UTC)", (dateTime) -> {
      dateTime.setHourOfDay((dateTime.getHourOfDay() / 3) * 3);
      dateTime.hourOfDay().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToStringWith1DaysBucketingInCETAndOutputInUTC() {
    testConvertEpochToStringWithBucketingTimeZone("1:DAYS", "CET", "yyyy-MM-dd HH:mm:ss.mmm tz(UTC)", (dateTime) -> {
      dateTime.dayOfMonth().roundFloor();
    });
  }

  @Test
  public void testConvertEpochToStringWith5DaysBucketingInCETAndOutputInUTC() {
    testConvertEpochToStringWithBucketingTimeZone("5:DAYS", "CET", "yyyy-MM-dd HH:mm:ss.mmm tz(UTC)", (dateTime) -> {
      dateTime.setDayOfMonth(((dateTime.getDayOfMonth() - 1) / 5) * 5 + 1);
      dateTime.dayOfMonth().roundFloor();
    });
  }

  private void testConvertEpochToStringWithBucketingTimeZone(String granularity, String timeZone,
      String outputFormat, Transformer transformer) {
    ExpressionContext expression = RequestContextUtils.getExpression(
        "dateTimeConvert(" + TIME_COLUMN
            + ",'1:MILLISECONDS:EPOCH','1:DAYS:SIMPLE_DATE_FORMAT:" + outputFormat + "','" + granularity
            + "'"
            + (timeZone != null ? ", '" + timeZone + "'" : "") + ")"
    );
    TransformFunction function = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(function.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata meta = function.getResultMetadata();
    assertTrue(meta.isSingleValue());
    assertEquals(meta.getDataType(), DataType.STRING);

    MutableDateTime dateTime = new MutableDateTime();
    dateTime.setZone(timeZone != null ? DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZone)) : DateTimeZone.UTC);
    DateTimeFormatter formatter = new DateTimeFormatPatternSpec(DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        outputFormat).getDateTimeFormatter();
    StringBuilder buffer = new StringBuilder();

    String[] expected = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      dateTime.setMillis(_timeValues[i]);
      transformer.transform(dateTime);
      buffer.setLength(0);
      formatter.printTo(buffer, dateTime);
      expected[i] = buffer.toString();
    }

    testTransformFunction(function, expected);
  }

  @Test
  public void testDateTimeConversionTransformFunction() {
    // NOTE: functionality of DateTimeConverter is covered in DateTimeConverterTest
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')", TIME_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof DateTimeConversionTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertTrue(resultMetadata.isSingleValue());
    assertEquals(resultMetadata.getDataType(), DataType.LONG);
    long[] expectedValues = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{
            String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH')", TIME_COLUMN)
        }, new Object[]{"dateTimeConvert(5,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')"}, new Object[]{
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')", INT_MV_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','MINUTES:1')", TIME_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,%s,'1:MINUTES:EPOCH','1:MINUTES')", TIME_COLUMN, INT_SV_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,'1:MINUTES:EPOCH','1:MINUTES:EPOCH','1:MINUTES','aa')", TIME_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,'1:MINUTES:EPOCH','1:MINUTES:EPOCH','1:MINUTES','')", TIME_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,'1:MINUTES:EPOCH','1:MINUTES:EPOCH','1:MINUTES',null)", TIME_COLUMN)
    }
    };
  }

  @Test
  public void testConvertNullColumnWithBucketingInUTC() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','1:HOURS', 'UTC')",
            TIMESTAMP_COLUMN_NULL));
    TransformFunction function = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(function.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata meta = function.getResultMetadata();
    assertTrue(meta.isSingleValue());
    assertEquals(meta.getDataType(), DataType.LONG);

    MutableDateTime dateTime = new MutableDateTime();
    dateTime.setZone(DateTimeZone.UTC);

    long[] expected = new long[NUM_ROWS];
    RoaringBitmap expectedNulls = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNulls.add(i);
      } else {
        dateTime.setMillis(_timeValues[i]);
        dateTime.hourOfDay().roundFloor();
        expected[i] = dateTime.getMillis();
      }
    }
    testTransformFunctionWithNull(function, expected, expectedNulls);
  }

  @Test
  public void testDateTimeConversionTransformFunctionNullColumn() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')",
            TIMESTAMP_COLUMN_NULL));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof DateTimeConversionTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertTrue(resultMetadata.isSingleValue());
    assertEquals(resultMetadata.getDataType(), DataType.LONG);
    long[] expectedValues = new long[NUM_ROWS];
    RoaringBitmap expectedNulls = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNulls.add(i);
      } else {
        expectedValues[i] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, expectedNulls);
  }
}
