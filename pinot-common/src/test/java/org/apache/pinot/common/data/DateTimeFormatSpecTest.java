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
    entries.add(new Object[]{"1:MILLISECONDS:TIMESTAMP", "2017-07-01 00:00:00", Timestamp.valueOf("2017-07-01 00:00:00").getTime()
    });
    entries.add(new Object[]{"1:MILLISECONDS:TIMESTAMP", "1498892400000", 1498892400000L});
    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "20170701", DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().parseMillis(
            "20170701")
        });
    entries.add(new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Chicago)", "20170701", DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago"))).parseMillis("20170701")
    });
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", "20170701 00", DateTimeFormat.forPattern("yyyyMMdd HH")
        .withZoneUTC().parseMillis("20170701 00")
    });
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH tz(GMT+0600)", "20170701 00", DateTimeFormat.forPattern("yyyyMMdd HH")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0600"))).parseMillis("20170701 00")
    });
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH Z", "20170701 00 -07:00", 1498892400000L});
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", "8/7/2017 12:45:50 AM", 1502066750000L});
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
    entries.add(new Object[]{"1:MILLISECONDS:TIMESTAMP", Timestamp.valueOf("2017-07-01 00:00:00").getTime(), "2017-07-01 00:00:00.0"
    });
    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().print(
            1498892400000L)
        });
    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/New_York)", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd")
            .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/New_York"))).print(1498892400000L)
        });
    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd HH").withZoneUTC().print(
            1498892400000L)
        });
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH tz(IST)", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd HH")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"))).print(1498892400000L)
    });
    entries.add(new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH Z", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd HH Z")
        .withZoneUTC().print(1498892400000L)
    });
    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH Z tz(GMT+0500)", 1498892400000L, DateTimeFormat.forPattern("yyyyMMdd HH Z")
            .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+0500"))).print(1498892400000L)
        });
    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", 1498892400000L, DateTimeFormat.forPattern("M/d/yyyy h:mm:ss a")
            .withZoneUTC().withLocale(Locale.ENGLISH).print(1498892400000L)
        });
    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h a", 1502066750000L, DateTimeFormat.forPattern("M/d/yyyy h a").withZoneUTC()
            .withLocale(Locale.ENGLISH).print(1502066750000L)
        });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Test fetching components of a format form a given format
  @Test(dataProvider = "testGetFromFormatDataProvider")
  public void testGetFromFormat(String format, int columnSizeFromFormatExpected, TimeUnit columnUnitFromFormatExpected,
      TimeFormat timeFormatFromFormatExpected, String sdfPatternFromFormatExpected, DateTimeZone dateTimeZoneFromFormatExpected) {

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

    entries.add(new Object[]{"1:HOURS:EPOCH", 1, TimeUnit.HOURS, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC});

    entries.add(new Object[]{"5:MINUTES:EPOCH", 5, TimeUnit.MINUTES, DateTimeFieldSpec.TimeFormat.EPOCH, null, DateTimeZone.UTC});

    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", 1, TimeUnit.DAYS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd",
            DateTimeZone.UTC});

    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(IST)", 1, TimeUnit.DAYS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
            "yyyyMMdd", DateTimeZone.forTimeZone(
            TimeZone.getTimeZone("IST"))
        });

    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd          tz(IST)", 1, TimeUnit.DAYS,
            DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", DateTimeZone.forTimeZone(
            TimeZone.getTimeZone("IST"))
        });

    entries.add(
        new Object[]{"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd tz  (   IST   )  ", 1, TimeUnit.DAYS,
            DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd", DateTimeZone.forTimeZone(
            TimeZone.getTimeZone("IST"))
        });

    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH", 1, TimeUnit.HOURS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
            "yyyyMMdd HH", DateTimeZone.UTC});

    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd HH tz(dummy)", 1, TimeUnit.HOURS,
            DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "yyyyMMdd HH", DateTimeZone.UTC});

    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a", 1, TimeUnit.HOURS, DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT,
            "M/d/yyyy h:mm:ss a", DateTimeZone.UTC});

    entries.add(
        new Object[]{"1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a tz(Asia/Tokyo)", 1, TimeUnit.HOURS,
            DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT, "M/d/yyyy h:mm:ss a", DateTimeZone.forTimeZone(
            TimeZone.getTimeZone("Asia/Tokyo"))
        });
    return entries.toArray(new Object[entries.size()][]);
  }

  // Test construct format given its components
  @Test(dataProvider = "testConstructFormatDataProvider")
  public void testConstructFormat(int columnSize, TimeUnit columnUnit, String columnTimeFormat, String pattern,
      DateTimeFormatSpec formatExpected1, DateTimeFormatSpec formatExpected2) {
    DateTimeFormatSpec formatActual1 = null;
    try {
      formatActual1 = new DateTimeFormatSpec(columnSize, columnUnit.toString(), columnTimeFormat);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(formatActual1, formatExpected1);

    DateTimeFormatSpec formatActual2 = null;
    try {
      formatActual2 = new DateTimeFormatSpec(columnSize, columnUnit.toString(), columnTimeFormat, pattern);
    } catch (Exception e) {
      // invalid arguments
    }
    Assert.assertEquals(formatActual2, formatExpected2);
  }

  @DataProvider(name = "testConstructFormatDataProvider")
  public Object[][] provideTestConstructFormatData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[]{1, TimeUnit.HOURS, "EPOCH", null, new DateTimeFormatSpec("1:HOURS:EPOCH"), null});
    entries.add(new Object[]{1, TimeUnit.HOURS, "EPOCH", "yyyyMMdd", new DateTimeFormatSpec("1:HOURS:EPOCH"), null});
    entries.add(new Object[]{5, TimeUnit.MINUTES, "EPOCH", null, new DateTimeFormatSpec("5:MINUTES:EPOCH"), null});
    entries.add(new Object[]{0, TimeUnit.HOURS, "EPOCH", null, null, null});
    entries.add(new Object[]{1, null, "EPOCH", null, null, null});
    entries.add(new Object[]{1, TimeUnit.HOURS, null, null, null, null});
    entries.add(new Object[]{1, TimeUnit.HOURS, "DUMMY", "yyyyMMdd", null, null});
    entries.add(new Object[]{1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", "yyyyMMdd", null, new DateTimeFormatSpec(
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd")
    });
    entries.add(new Object[]{1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", "yyyyMMdd tz(America/Los_Angeles)", null, new DateTimeFormatSpec(
        "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMdd tz(America/Los_Angeles)")
    });
    entries.add(new Object[]{1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", null, null, null});
    entries.add(new Object[]{-1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", "yyyyMMDD", null, null});
    entries.add(new Object[]{1, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT", "M/d/yyyy h:mm:ss a", null, new DateTimeFormatSpec(
        "1:HOURS:SIMPLE_DATE_FORMAT:M/d/yyyy h:mm:ss a")
    });
    return entries.toArray(new Object[entries.size()][]);
  }
}
