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
package org.apache.pinot.common.data;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for DateTimeFormatSpec helper methods
 */
public class DateTimeFormatSpecTest {

  // Test conversion of a dateTimeColumn value from a format to millis
  @Test(dataProvider = "testFromFormatToMillisDataProvider")
  public void testFromFormatToMillis(String format, String formattedValue, long expectedTimeMs) {
    Assert.assertEquals(new DateTimeFormatSpec(format).fromFormatToMillis(formattedValue), expectedTimeMs);
  }

  @DataProvider(name = "testFromFormatToMillisDataProvider")
  public Object[][] provideTestFromFormatToMillisData() {

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{"1:HOURS:EPOCH", "416359", 1498892400000L});
    entries.add(new Object[]{"1:MILLISECONDS:EPOCH", "1498892400000", 1498892400000L});
    entries.add(new Object[]{"1:HOURS:EPOCH", "0", 0L});
    entries.add(new Object[]{"5:MINUTES:EPOCH", "4996308", 1498892400000L});
    entries.add(new Object[]{
        "1:MILLISECONDS:TIMESTAMP", "2017-07-01 00:00:00", Timestamp.valueOf("2017-07-01 00:00:00").getTime()
    });
    entries.add(new Object[]{"1:MILLISECONDS:TIMESTAMP", "1498892400000", 1498892400000L});
    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "20170701",
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().parseMillis("20170701")
    });
    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Chicago)", "20170701", DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago"))).parseMillis("20170701")
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", "20170701 00",
        DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().parseMillis("20170701 00")
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH tz(GMT+0600)", "20170701 00", DateTimeFormat.forPattern("yyyyMMdd HH")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0600"))).parseMillis("20170701 00")
    });
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH Z", "20170701 00 -07:00", 1498892400000L});
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "8/7/2017 12:45:50 AM", 1502066750000L});
    entries.add(new Object[]{"EPOCH|HOURS|1", "416359", 1498892400000L});
    entries.add(new Object[]{"EPOCH|HOURS", "416359", 1498892400000L});
    entries.add(new Object[]{"EPOCH|MILLISECONDS|1", "1498892400000", 1498892400000L});
    entries.add(new Object[]{"EPOCH|MILLISECONDS", "1498892400000", 1498892400000L});
    entries.add(new Object[]{"EPOCH|HOURS|1", "0", 0L});
    entries.add(new Object[]{"EPOCH|HOURS", "0", 0L});
    entries.add(new Object[]{"EPOCH|MINUTES|5", "4996308", 1498892400000L});
    entries.add(new Object[]{
        "TIMESTAMP", "2017-07-01 00:00:00", Timestamp.valueOf("2017-07-01 00:00:00").getTime()
    });
    entries.add(new Object[]{"TIMESTAMP", "1498892400000", 1498892400000L});
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT", "2017-07-01",
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().parseMillis("20170701")
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT", "2017-07-01T12:45:50",
        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC().parseMillis("2017-07-01T12:45:50")
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT", "2017",
        DateTimeFormat.forPattern("yyyy").withZoneUTC().parseMillis("2017")
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd", "20170701",
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().parseMillis("20170701")
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd|America/Chicago", "20170701", DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago"))).parseMillis("20170701")
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH", "20170701 00",
        DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().parseMillis("20170701 00")
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH|GMT+0600", "20170701 00", DateTimeFormat.forPattern("yyyyMMdd HH")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0600"))).parseMillis("20170701 00")
    });
    entries.add(new Object[]{"SIMPLE_DATE_FORMAT|yyyyMMdd HH Z", "20170701 00 -07:00", 1498892400000L});
    entries.add(new Object[]{"SIMPLE_DATE_FORMAT|M/d/yyyy h:mm:ss a", "8/7/2017 12:45:50 AM", 1502066750000L});

    return entries.toArray(new Object[entries.size()][]);
  }

  // Test the conversion of a millis value to date time column value in a format
  @Test(dataProvider = "testFromMillisToFormatDataProvider")
  public void testFromMillisToFormat(String format, long timeMs, String expectedFormattedValue) {
    Assert.assertEquals(new DateTimeFormatSpec(format).fromMillisToFormat(timeMs), expectedFormattedValue);
  }

  @DataProvider(name = "testFromMillisToFormatDataProvider")
  public Object[][] provideTestFromMillisToFormatData() {

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{"1:HOURS:EPOCH", 1498892400000L, "416359"});
    entries.add(new Object[]{"1:MILLISECONDS:EPOCH", 1498892400000L, "1498892400000"});
    entries.add(new Object[]{"1:HOURS:EPOCH", 0L, "0"});
    entries.add(new Object[]{"5:MINUTES:EPOCH", 1498892400000L, "4996308"});
    entries.add(new Object[]{
        "1:MILLISECONDS:TIMESTAMP", Timestamp.valueOf("2017-07-01 00:00:00").getTime(), "2017-07-01 00:00:00.0"
    });
    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/New_York)", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/New_York"))).print(1498892400000L)
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH tz(IST)", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))).print(
            1498892400000L)
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH Z", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH Z").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH Z tz(GMT+0500)", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH Z")
            .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0500"))).print(1498892400000L)
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", 1498892400000L,
        DateTimeFormat.forPattern("M/d/yyyy h:mm:ss a").withZoneUTC().withLocale(Locale.ENGLISH).print(1498892400000L)
    });
    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h a", 1502066750000L,
        DateTimeFormat.forPattern("M/d/yyyy h a").withZoneUTC().withLocale(Locale.ENGLISH).print(1502066750000L)
    });
    entries.add(new Object[]{"EPOCH|HOURS|1", 1498892400000L, "416359"});
    entries.add(new Object[]{"EPOCH|MILLISECONDS|1", 1498892400000L, "1498892400000"});
    entries.add(new Object[]{"EPOCH|HOURS|1", 0L, "0"});
    entries.add(new Object[]{"EPOCH|MINUTES|5", 1498892400000L, "4996308"});
    entries.add(new Object[]{
        "TIMESTAMP", Timestamp.valueOf("2017-07-01 00:00:00").getTime(), "2017-07-01 00:00:00.0"
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd|America/New_York", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/New_York"))).print(1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH|IST", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))).print(
            1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH Z", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH Z").withZoneUTC().print(1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH Z|GMT+0500", 1498892400000L,
        DateTimeFormat.forPattern("yyyyMMdd HH Z")
            .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0500"))).print(1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|M/d/yyyy h:mm:ss a", 1498892400000L,
        DateTimeFormat.forPattern("M/d/yyyy h:mm:ss a").withZoneUTC().withLocale(Locale.ENGLISH).print(1498892400000L)
    });
    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|M/d/yyyy h a", 1502066750000L,
        DateTimeFormat.forPattern("M/d/yyyy h a").withZoneUTC().withLocale(Locale.ENGLISH).print(1502066750000L)
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT", 1502066750000L,
        ISODateTimeFormat.dateTimeNoMillis().withZoneUTC().withLocale(Locale.ENGLISH).print(1502066750000L)
    });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Test fetching components of a format form a given format
  @Test(dataProvider = "testGetFromFormatDataProvider")
  public void testGetFromFormat(String format, int columnSizeFromFormatExpected, TimeUnit columnUnitFromFormatExpected,
      TimeFormat timeFormatFromFormatExpected, String sdfPatternFromFormatExpected,
      DateTimeZone dateTimeZoneFromFormatExpected) {

    DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec(format);

    int columnSizeFromFormat = dateTimeFormatSpec.getColumnSize();
    Assert.assertEquals(columnSizeFromFormat, columnSizeFromFormatExpected);

    TimeUnit columnUnitFromFormat = dateTimeFormatSpec.getColumnUnit();
    Assert.assertEquals(columnUnitFromFormat, columnUnitFromFormatExpected);

    TimeFormat timeFormatFromFormat = dateTimeFormatSpec.getTimeFormat();
    Assert.assertEquals(timeFormatFromFormat, timeFormatFromFormatExpected);

    String sdfPatternFromFormat = null;
    DateTimeZone dateTimeZoneFromFormat = DateTimeZone.UTC;
    try {
      sdfPatternFromFormat = dateTimeFormatSpec.getSDFPattern();
      dateTimeZoneFromFormat = dateTimeFormatSpec.getDateTimezone();
    } catch (Exception e) {
      // No sdf pattern
    }
    Assert.assertEquals(sdfPatternFromFormat, sdfPatternFromFormatExpected);
    Assert.assertEquals(dateTimeZoneFromFormat, dateTimeZoneFromFormatExpected);
  }

  @DataProvider(name = "testGetFromFormatDataProvider")
  public Object[][] provideTestGetFromFormatData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(
        new Object[]{"1:MILLISECONDS:TIMESTAMP", 1, TimeUnit.MILLISECONDS, TimeFormat.TIMESTAMP, null,
            DateTimeZone.UTC});

    entries.add(
        new Object[]{"1:HOURS:EPOCH", 1, TimeUnit.HOURS, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC});

    entries.add(new Object[]{
        "5:MINUTES:EPOCH", 5, TimeUnit.MINUTES, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", 1, TimeUnit.MILLISECONDS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        "yyyyMMdd", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(IST)", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", DateTimeZone.forTimeZone(
        TimeZone.getTimeZone("IST"))
    });

    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd          tz(IST)", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd",
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))
    });

    entries.add(new Object[]{
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz  (   IST   )  ", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd",
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))
    });

    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH tz(dummy)", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "M/d/yyyy h:mm:ss a", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a tz(Asia/Tokyo)", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "M/d/yyyy h:mm:ss a",
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Tokyo"))
    });

    //test new format
    entries.add(
        new Object[]{"TIMESTAMP", 1, TimeUnit.MILLISECONDS, TimeFormat.TIMESTAMP, null,
            DateTimeZone.UTC});

    entries.add(
        new Object[]{"EPOCH", 1, TimeUnit.MILLISECONDS, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC});

    entries.add(
        new Object[]{"EPOCH|HOURS|1", 1, TimeUnit.HOURS, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC});

    entries.add(new Object[]{
        "EPOCH|MINUTES|5", 5, TimeUnit.MINUTES, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT", 1, TimeUnit.MILLISECONDS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        null, DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd", 1, TimeUnit.MILLISECONDS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        "yyyyMMdd", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd|IST", 1, TimeUnit.MILLISECONDS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        "yyyyMMdd", DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd|IST", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd",
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd|IST", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd",
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH", 1, TimeUnit.MILLISECONDS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
        "yyyyMMdd HH", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|yyyyMMdd HH|dummy", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|M/d/yyyy h:mm:ss a", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "M/d/yyyy h:mm:ss a", DateTimeZone.UTC
    });

    entries.add(new Object[]{
        "SIMPLE_DATE_FORMAT|M/d/yyyy h:mm:ss a|Asia/Tokyo", 1, TimeUnit.MILLISECONDS,
        DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "M/d/yyyy h:mm:ss a",
        DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Tokyo"))
    });
    return entries.toArray(new Object[entries.size()][]);
  }
}
