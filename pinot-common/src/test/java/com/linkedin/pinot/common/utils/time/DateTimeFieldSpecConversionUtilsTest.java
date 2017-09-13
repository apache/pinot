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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.DateTimeFieldSpec.DateTimeType;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
public class DateTimeFieldSpecConversionUtilsTest {

  // Timespec toMillis
  @Test(dataProvider = "testTimespecToMillisDataProvider")
  public void testTimespecToMillis(String name, DataType dataType, TimeUnit timeType, int timeSize, String timeFormat,
      Object timeColumnValue, long millisExpected) {

    TimeGranularitySpec timeGranularitySpec = new TimeGranularitySpec(dataType, timeSize, timeType, timeFormat, name);
    long millisActual = timeGranularitySpec.toMillis(timeColumnValue);
    Assert.assertEquals(millisActual, millisExpected);
  }

  @DataProvider(name = "testTimespecToMillisDataProvider")
  public Object[][] provideTestTimespecToMillisData() {

    String name = "Date";

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.HOURS, 1, TimeFormat.EPOCH.toString(), 416359L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.MILLISECONDS, 1, TimeFormat.EPOCH.toString(), 1498892400000L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.HOURS, 1, TimeFormat.EPOCH.toString(), 0L, 0L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.MINUTES, 5, TimeFormat.EPOCH.toString(), 4996308L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.DAYS, 1, TimeFormat.SIMPLE_DATE_FORMAT.toString() + ":yyyyMMdd", "20170701", 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.STRING, TimeUnit.HOURS, 1, TimeFormat.SIMPLE_DATE_FORMAT.toString() + ":yyyyMMdd HH", "20170701 00", 1498892400000L
    });
    return entries.toArray(new Object[entries.size()][]);
  }


  // Timespec fromMillis
  @Test(dataProvider = "testTimespecFromMillisDataProvider")
  public void testTimespecFromMillis(String name, DataType dataType, TimeUnit timeType, int timeSize, String timeFormat,
      long timeColumnValueMS, Object timeColumnValueExpected) {

    TimeGranularitySpec timeGranularitySpec = new TimeGranularitySpec(dataType, timeSize, timeType, timeFormat, name);
    Object timeColumnValueActual = timeGranularitySpec.fromMillis(timeColumnValueMS);
    Assert.assertEquals(timeColumnValueActual, timeColumnValueExpected);
  }

  @DataProvider(name = "testTimespecFromMillisDataProvider")
  public Object[][] provideTestTimespecFromMillisData() {

    String name = "Date";

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.HOURS, 1, TimeFormat.EPOCH.toString(), 1498892400000L, 416359L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.MILLISECONDS, 1, TimeFormat.EPOCH.toString(), 1498892400000L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.HOURS, 1, TimeFormat.EPOCH.toString(), 0L, 0L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.MINUTES, 5, TimeFormat.EPOCH.toString(), 1498892400000L, 4996308L
    });
    entries.add(new Object[] {
        name, DataType.LONG, TimeUnit.DAYS, 1, TimeFormat.SIMPLE_DATE_FORMAT.toString() + ":yyyyMMdd", 1498892400000L, "20170701"
    });
    entries.add(new Object[] {
        name, DataType.STRING, TimeUnit.HOURS, 1, TimeFormat.SIMPLE_DATE_FORMAT.toString() + ":yyyyMMdd HH", 1498892400000L, "20170701 00"
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Datetimespec toMillis
  @Test(dataProvider = "testDateTimeSpecToMillisDataProvider")
  public void testDateTimeSpecToMillis(String name, DataType dataType, String format, String granularity, DateTimeType dateTimeType,
      Object timeColumnValue, long millisExpected) {

    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, dateTimeType);
    long millisActual = dateTimeFieldSpec.toMillis(timeColumnValue);
    Assert.assertEquals(millisActual, millisExpected);
  }

  @DataProvider(name = "testDateTimeSpecToMillisDataProvider")
  public Object[][] provideTestDateTimeSpecToMillisData() {

    String name = "Date";

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        name, DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 416359L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 1498892400000L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 0L, 0L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "5:MINUTES:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 4996308L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:HOURS", DateTimeType.PRIMARY, "20170701", 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", "1:HOURS", DateTimeType.PRIMARY, "20170701 00", 1498892400000L
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Datetimespec fromMillis
  @Test(dataProvider = "testDateTimeSpecFromMillisDataProvider")
  public void testDateTimeSpecFromMillis(String name, DataType dataType, String format, String granularity, DateTimeType dateTimeType,
      long timeColumnValueMS, Object timeColumnValueExpected) {

    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, dateTimeType);
    Object timeColumnValueActual = dateTimeFieldSpec.fromMillis(timeColumnValueMS);
    Assert.assertEquals(timeColumnValueActual, timeColumnValueExpected);
  }

  @DataProvider(name = "testDateTimeSpecFromMillisDataProvider")
  public Object[][] provideTestDateTimeSpecFromMillisData() {

    String name = "Date";

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        name, DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 1498892400000L, 416359L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 1498892400000L, 1498892400000L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 0L, 0L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "5:MINUTES:EPOCH", "1:HOURS", DateTimeType.PRIMARY, 1498892400000L, 4996308L
    });
    entries.add(new Object[] {
        name, DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:HOURS", DateTimeType.PRIMARY, 1498892400000L, "20170701"
    });
    entries.add(new Object[] {
        name, DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", "1:HOURS", DateTimeType.PRIMARY, 1498892400000L, "20170701 00"
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Datetimespec fromMillis
  @Test(dataProvider = "testGetFromFormatDataProvider")
  public void testGetFromFormat(DateTimeFieldSpec dateTimeFieldSpec, int columnSizeFromFormatExpected,
      TimeUnit columnUnitFromFormatExpected,
      com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat timeFormatFromFormatExpected,
      String sdfPatternFromFormatExpected) {

    int columnSizeFromFormat = dateTimeFieldSpec.getColumnSizeFromFormat();
    Assert.assertEquals(columnSizeFromFormat, columnSizeFromFormatExpected);

    TimeUnit columnUnitFromFormat = dateTimeFieldSpec.getColumnUnitFromFormat();
    Assert.assertEquals(columnUnitFromFormat, columnUnitFromFormatExpected);

    com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat timeFormatFromFormat = dateTimeFieldSpec.getTimeFormatFromFormat();
    Assert.assertEquals(timeFormatFromFormat, timeFormatFromFormatExpected);

    String sdfPatternFromFormat = null;
    try {
      sdfPatternFromFormat = dateTimeFieldSpec.getSDFPatternFromFormat();
    } catch (Exception e) {
      // No sdf pattern
    }
    Assert.assertEquals(sdfPatternFromFormat, sdfPatternFromFormatExpected);
  }

  @DataProvider(name = "testGetFromFormatDataProvider")
  public Object[][] provideTestGetFromFormatData() {

    String name = "Date";
    DataType dataType = DataType.LONG;
    String granularity = "1:HOURS";
    DateTimeType dateTimeType = DateTimeType.PRIMARY;

    List<Object[]> entries = new ArrayList<>();

    String format = "1:HOURS:EPOCH";
    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, dateTimeType);
    entries.add(new Object[] {
        dateTimeFieldSpec, 1, TimeUnit.HOURS, com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.EPOCH, null
    });

    format = "5:MINUTES:EPOCH";
    dateTimeFieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, dateTimeType);
    entries.add(new Object[] {
        dateTimeFieldSpec, 5, TimeUnit.MINUTES, com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.EPOCH, null
    });

    format = "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd";
    dateTimeFieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, dateTimeType);
    entries.add(new Object[] {
        dateTimeFieldSpec, 1, TimeUnit.DAYS, com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd"
    });

    format = "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH";
    dateTimeFieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, dateTimeType);
    entries.add(new Object[] {
        dateTimeFieldSpec, 1, TimeUnit.HOURS, com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH"
    });


    return entries.toArray(new Object[entries.size()][]);
  }
}
