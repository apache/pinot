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
package com.linkedin.pinot.common.utils.time;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;

/**
 * Tests for helper methods in DateTimeFieldSpecUtils
 */
public class DateTimeFieldSpecUtilsTest {

  // Test conversion of a dateTimeColumn value from a format to millis
  @Test(dataProvider = "testFromFormatToMillisDataProvider")
  public void testFromFormatToMillis(String format, Object timeColumnValue, long millisExpected) {

    long millisActual = DateTimeFieldSpecUtils.fromFormatToMillis(timeColumnValue, format);
    Assert.assertEquals(millisActual, millisExpected);
  }

  @DataProvider(name = "testFromFormatToMillisDataProvider")
  public Object[][] provideTestFromFormatToMillisData() {

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "1:HOURS:EPOCH", 416359L, 1498892400000L
    });
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", 1498892400000L, 1498892400000L
    });
    entries.add(new Object[] {
        "1:HOURS:EPOCH", 0L, 0L
    });
    entries.add(new Object[] {
        "5:MINUTES:EPOCH", 4996308L, 1498892400000L
    });
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "20170701",
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().parseMillis("20170701")
    });
    entries.add(new Object[] {
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", "20170701 00",
        DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().parseMillis("20170701 00")
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Test the conversion of a millis value to date time column value in a format
  @Test(dataProvider = "testFromMillisToFormatDataProvider")
  public void testFromMillisToFormat(String format, long timeColumnValueMS, Class type,
      Object timeColumnValueExpected) {

    Object timeColumnValueActual =
        DateTimeFieldSpecUtils.fromMillisToFormat(timeColumnValueMS, format, type);
    Assert.assertEquals(timeColumnValueActual, timeColumnValueExpected);
  }

  @DataProvider(name = "testFromMillisToFormatDataProvider")
  public Object[][] provideTestFromMillisToFormatData() {

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        "1:HOURS:EPOCH", 1498892400000L, Long.class, 416359L
    });
    entries.add(new Object[] {
        "1:MILLISECONDS:EPOCH", 1498892400000L, Long.class, 1498892400000L
    });
    entries.add(new Object[] {
        "1:HOURS:EPOCH", 0L, Long.class, 0L
    });
    entries.add(new Object[] {
        "5:MINUTES:EPOCH", 1498892400000L, Long.class, 4996308L
    });
    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", 1498892400000L, String.class,
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[] {
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", 1498892400000L, String.class,
        DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().print(1498892400000L)
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Test fetching components of a format form a given format
  @Test(dataProvider = "testGetFromFormatDataProvider")
  public void testGetFromFormat(String format, int columnSizeFromFormatExpected,
      TimeUnit columnUnitFromFormatExpected, TimeFormat timeFormatFromFormatExpected,
      String sdfPatternFromFormatExpected) {

    int columnSizeFromFormat = DateTimeFieldSpecUtils.getColumnSizeFromFormat(format);
    Assert.assertEquals(columnSizeFromFormat, columnSizeFromFormatExpected);

    TimeUnit columnUnitFromFormat = DateTimeFieldSpecUtils.getColumnUnitFromFormat(format);
    Assert.assertEquals(columnUnitFromFormat, columnUnitFromFormatExpected);

    com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat timeFormatFromFormat =
        DateTimeFieldSpecUtils.getTimeFormatFromFormat(format);
    Assert.assertEquals(timeFormatFromFormat, timeFormatFromFormatExpected);

    String sdfPatternFromFormat = null;
    try {
      sdfPatternFromFormat = DateTimeFieldSpecUtils.getSDFPatternFromFormat(format);
    } catch (Exception e) {
      // No sdf pattern
    }
    Assert.assertEquals(sdfPatternFromFormat, sdfPatternFromFormatExpected);
  }

  @DataProvider(name = "testGetFromFormatDataProvider")
  public Object[][] provideTestGetFromFormatData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[] {
        "1:HOURS:EPOCH", 1, TimeUnit.HOURS,
        com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.EPOCH, null
    });

    entries.add(new Object[] {
        "5:MINUTES:EPOCH", 5, TimeUnit.MINUTES,
        com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.EPOCH, null
    });

    entries.add(new Object[] {
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", 1, TimeUnit.DAYS,
        com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd"
    });

    entries.add(new Object[] {
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", 1, TimeUnit.HOURS,
        com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        "yyyyMMdd HH"
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Test construct format given its components
  @Test(dataProvider = "testConstructFormatDataProvider")
  public void testConstructFormat(int columnSize, TimeUnit columnUnit, String columnTimeFormat,
      String pattern, String formatExpected1, String formatExpected2) {
    String formatActual1 = null;
    try {
      formatActual1 =
          DateTimeFieldSpecUtils.constructFormat(columnSize, columnUnit, columnTimeFormat);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(formatActual1, formatExpected1);

    String formatActual2 = null;
    try {
      formatActual2 =
          DateTimeFieldSpecUtils.constructFormat(columnSize, columnUnit, columnTimeFormat, pattern);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(formatActual2, formatExpected2);
  }

  @DataProvider(name = "testConstructFormatDataProvider")
  public Object[][] provideTestConstructFormatData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[] {
        1, TimeUnit.HOURS, "EPOCH", null, "1:HOURS:EPOCH", null
    });
    entries.add(new Object[] {
        1, TimeUnit.HOURS, "EPOCH", "yyyyMMdd", "1:HOURS:EPOCH", null
    });
    entries.add(new Object[] {
        5, TimeUnit.MINUTES, "EPOCH", null, "5:MINUTES:EPOCH", null
    });
    entries.add(new Object[] {
        0, TimeUnit.HOURS, "EPOCH", null, null, null
    });
    entries.add(new Object[] {
        1, null, "EPOCH", null, null, null
    });
    entries.add(new Object[] {
        1, TimeUnit.HOURS, null, null, null, null
    });
    entries.add(new Object[] {
        1, TimeUnit.HOURS, "DUMMY", "yyyyMMdd", null, null
    });
    entries.add(new Object[] {
        1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", "yyyyMMdd", null,
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd"
    });
    entries.add(new Object[] {
        1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", null, null, null
    });
    entries.add(new Object[] {
        -1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", "yyyyMMDD", null, null
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  // Test construct granularity from components
  @Test(dataProvider = "testConstructGranularityDataProvider")
  public void testConstructGranularity(int size, TimeUnit unit, String granularityExpected) {
    String granularityActual = null;
    try {
      granularityActual = DateTimeFieldSpecUtils.constructGranularity(size, unit);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(granularityActual, granularityExpected);
  }

  @DataProvider(name = "testConstructGranularityDataProvider")
  public Object[][] provideTestConstructGranularityData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[] {
        1, TimeUnit.HOURS, "1:HOURS"
    });
    entries.add(new Object[] {
        5, TimeUnit.MINUTES, "5:MINUTES"
    });
    entries.add(new Object[] {
        0, TimeUnit.HOURS, null
    });
    entries.add(new Object[] {
        -1, TimeUnit.HOURS, null
    });
    entries.add(new Object[] {
        1, null, null
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  // Test granularity to millis
  @Test(dataProvider = "testGranularityToMillisDataProvider")
  public void testGranularityToMillis(String granularity, Long millisExpected) {
    Long millisActual = null;
    try {
      millisActual = DateTimeFieldSpecUtils.granularityToMillis(granularity);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(millisActual, millisExpected);
  }

  @DataProvider(name = "testGranularityToMillisDataProvider")
  public Object[][] provideTestGranularityToMillisData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[] {
        "1:HOURS", 3600000L
    });
    entries.add(new Object[] {
        "1:MILLISECONDS", 1L
    });
    entries.add(new Object[] {
        "15:MINUTES", 900000L
    });
    entries.add(new Object[] {
        "0:HOURS", null
    });
    entries.add(new Object[] {
        null, null
    });
    entries.add(new Object[] {
        "1:DUMMY", null
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  // Test bucket millis to granularity
  @Test(dataProvider = "testBucketMillisDataProvider")
  public void testBucketMillis(Long dateTimeColumnValueMS, String outputGranularity,
      Long expectedBucketedMillis) {
    Long bucketedMillis = null;
    try {
      bucketedMillis =
          DateTimeFieldSpecUtils.bucketDateTimeValueMS(dateTimeColumnValueMS, outputGranularity);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(bucketedMillis, expectedBucketedMillis);
  }

  @DataProvider(name = "testBucketMillisDataProvider")
  public Object[][] provideTestBucketMillisData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[] {
        1498892400000L, "1:MILLISECONDS", 1498892400000L
    });
    entries.add(new Object[] {
        0L, "1:HOURS", 0L
    });
    entries.add(new Object[] {
        null, "1:HOURS", null
    });
    entries.add(new Object[] {
        1498919002080L /* 2017-07-01T07:23:22 080 */, "1:SECONDS", 1498919002000L
    /* Rounded to seconds 2017-07-01T07:23:22 000 */
    });
    entries.add(new Object[] {
        1498919002080L /* 2017-07-01T07:23:22 080 */, "1:MINUTES", 1498918980000L
    /* Rounded to minutes 2017-07-01T07:23:00 000 */
    });
    entries.add(new Object[] {
        1498919002080L /* 2017-07-01T07:23:22 080 */, "5:MINUTES", 1498918800000L
    /* Rounded to 5 minutes 2017-07-01T07:20:00 000 */
    });
    entries.add(new Object[] {
        1498919002080L /* 2017-07-01T07:23:22 080 */, "1:HOURS", 1498917600000L
    /* Rounded to 1 hours 2017-07-01T07:20:00 000 */
    });
    entries.add(new Object[] {
        1498892400000L, null, null
    });

    return entries.toArray(new Object[entries.size()][]);
  }

}
