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
package org.apache.pinot.common.function.scalar;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.*;


public class DateTimeFunctionsTest {

  private DateTimeZone _defaultDateTimeZone;

  @BeforeMethod
  public void set() {
    _defaultDateTimeZone = DateTimeZone.getDefault();
    DateTimeZone.setDefault(DateTimeZone.UTC);
  }

  @AfterMethod
  public void restore() {
    DateTimeFunctions.resetClock();
    DateTimeZone.setDefault(_defaultDateTimeZone);
  }

  @Test
  public void testToEpochSeconds() {
    Assert.assertEquals(DateTimeFunctions.toEpochSeconds(1609459200999L), 1609459200L);
    Assert.assertEquals(DateTimeFunctions.toEpochSeconds(1646092799123L), 1646092799L);
  }

  @Test
  public void testToEpochMinutes() {
    Assert.assertEquals(DateTimeFunctions.toEpochMinutes(1609459200999L), 26824320L);
    Assert.assertEquals(DateTimeFunctions.toEpochMinutes(1646092799123L), 27434879L);
  }

  @Test
  public void testToEpochHours() {
    Assert.assertEquals(DateTimeFunctions.toEpochHours(1609459200999L), 447072L);
    Assert.assertEquals(DateTimeFunctions.toEpochHours(1646092799123L), 457247L);
  }

  @Test
  public void testToEpochDays() {
    Assert.assertEquals(DateTimeFunctions.toEpochDays(1609459200999L), 18628L);
    Assert.assertEquals(DateTimeFunctions.toEpochDays(1646092799123L), 19051L);
  }

  @Test
  public void testToEpochSecondsRounded() {
    Assert.assertEquals(DateTimeFunctions.toEpochSecondsRounded(1609459200999L, 1000), 1609459000L);
    Assert.assertEquals(DateTimeFunctions.toEpochSecondsRounded(1646092799123L, 5), 1646092795L);
  }

  @Test
  public void testToEpochMinutesRounded() {
    Assert.assertEquals(DateTimeFunctions.toEpochMinutesRounded(1609459200999L, 50), 26824300L);
    Assert.assertEquals(DateTimeFunctions.toEpochMinutesRounded(1646092799123L, 2), 27434878L);
  }

  @Test
  public void testToEpochHoursRounded() {
    Assert.assertEquals(DateTimeFunctions.toEpochHoursRounded(1609459200999L, 2), 447072L);
    Assert.assertEquals(DateTimeFunctions.toEpochHoursRounded(1646092799123L, 4), 457244L);
  }

  @Test
  public void testToEpochDaysRounded() {
    Assert.assertEquals(DateTimeFunctions.toEpochDaysRounded(1609459200999L, 32), 18624L);
    Assert.assertEquals(DateTimeFunctions.toEpochDaysRounded(1646092799123L, 64), 19008L);
  }

  @Test
  public void testToEpochSecondsBucket() {
    Assert.assertEquals(DateTimeFunctions.toEpochSecondsBucket(1609459200999L, 1000), 1609459L);
    Assert.assertEquals(DateTimeFunctions.toEpochSecondsBucket(1646092799123L, 5), 329218559L);
  }

  @Test
  public void testToEpochMinutesBucket() {
    Assert.assertEquals(DateTimeFunctions.toEpochMinutesBucket(1609459200999L, 50), 536486L);
    Assert.assertEquals(DateTimeFunctions.toEpochMinutesBucket(1646092799123L, 2), 13717439L);
  }

  @Test
  public void testToEpochHoursBucket() {
    Assert.assertEquals(DateTimeFunctions.toEpochHoursBucket(1609459200999L, 2), 223536L);
    Assert.assertEquals(DateTimeFunctions.toEpochHoursBucket(1646092799123L, 4), 114311L);
  }

  @Test
  public void testToEpochDaysBucket() {
    Assert.assertEquals(DateTimeFunctions.toEpochDaysBucket(1609459200999L, 32), 582L);
    Assert.assertEquals(DateTimeFunctions.toEpochDaysBucket(1646092799123L, 64), 297L);
  }

  @Test
  public void testFromEpochSeconds() {
    Assert.assertEquals(DateTimeFunctions.fromEpochSeconds(1609459200L), 1609459200000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochSeconds(1646092799L), 1646092799000L);
  }

  @Test
  public void testFromEpochMinutes() {
    Assert.assertEquals(DateTimeFunctions.fromEpochMinutes(26824320L), 1609459200000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochMinutes(27434879L), 1646092740000L);
  }

  @Test
  public void testFromEpochHours() {
    Assert.assertEquals(DateTimeFunctions.fromEpochHours(447072L), 1609459200000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochHours(457247L), 1646089200000L);
  }

  @Test
  public void testFromEpochDays() {
    Assert.assertEquals(DateTimeFunctions.fromEpochDays(18628L), 1609459200000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochDays(19051L), 1646006400000L);
  }

  @Test
  public void testFromEpochSecondsBucket() {
    Assert.assertEquals(DateTimeFunctions.fromEpochSecondsBucket(1609459L, 1000), 1609459000000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochSecondsBucket(329218559L, 5), 1646092795000L);
  }

  @Test
  public void testFromEpochMinutesBucket() {
    Assert.assertEquals(DateTimeFunctions.fromEpochMinutesBucket(536486L, 50), 1609458000000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochMinutesBucket(13717439L, 2), 1646092680000L);
  }

  @Test
  public void testFromEpochHoursBucket() {
    Assert.assertEquals(DateTimeFunctions.fromEpochHoursBucket(223536L, 2), 1609459200000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochHoursBucket(114311L, 4), 1646078400000L);
  }

  @Test
  public void fromEpochDaysBucket() {
    Assert.assertEquals(DateTimeFunctions.fromEpochDaysBucket(582L, 32), 1609113600000L);
    Assert.assertEquals(DateTimeFunctions.fromEpochDaysBucket(297L, 64), 1642291200000L);
  }

  @Test
  public void testToTimestamp() {
    Assert.assertEquals(
        DateTimeFunctions.toTimestamp(1633375617536L), new Timestamp(1633375617536L));
  }

  @Test
  public void testFromTimestamp() {
    Assert.assertEquals(
        DateTimeFunctions.fromTimestamp(new Timestamp(1633375617536L)), 1633375617536L);
  }

  @Test
  public void toDateTime() {
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1633376098332L, "YYYY-MM-dd'T'HH:mm:ss.SSS z"),
        "2021-10-04T19:34:58.332 UTC");
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1633376098332L, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2021-10-04T19:34:58.332+0000");
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1633376098332L, "YYYY-MM-dd'T'HH:mm:ss.SSS z"),
        "2021-10-04T19:34:58.332 UTC");
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1325376000000L, "xxxx"), "2011"); // 2012-01-01 00:00:00
    Assert.assertEquals(DateTimeFunctions.toDateTime(1325376000000L, "yyyy"), "2012");
    Assert.assertEquals(DateTimeFunctions.toDateTime(1325376000000L, "YYYY"), "2012");
    Assert.assertEquals(DateTimeFunctions.toDateTime(1325376000000L, "C' century 'G"), "20 century AD");
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1633376098332L, "'day 'D' o'' year is 'E"), "day 277 o' year is Mon");
    Assert.assertEquals(DateTimeFunctions.toDateTime(1325376000000L, "e EEEEE"), "7 Sunday");
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1633376098332L, "MM MMM MMMM"), "10 Oct October");
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1634754964999L, "hh a").toUpperCase(), "06 PM"); // 2021-10-20 18:36:04.999
    Assert.assertEquals(
        DateTimeFunctions.toDateTime(1325376000000L, "KK hh"), "00 12");
    Assert.assertEquals(DateTimeFunctions.toDateTime(1633376098332L, "w ww"), "40 40");
    Assert.assertEquals(DateTimeFunctions.toDateTime(1325376000000L, "w"), "52");
    Assert.assertEquals(DateTimeFunctions.toDateTime(-102117536000000L, "y Y G"), "-1266 1267 BC");
  }

  @Test
  public void testFromDateTime() {
    Assert.assertEquals(
        DateTimeFunctions.fromDateTime("2021-10-04T19:34:58.332 UTC", "YYYY-MM-dd'T'HH:mm:ss.SSS z"),
        1633376098332L);
    Assert.assertEquals(
        DateTimeFunctions.fromDateTime("2021-10-04T19:34:58.332+0000", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        1633376098332L);
    Assert.assertEquals(
        DateTimeFunctions.fromDateTime("2021-10-04T19:34:58.332 UTC", "YYYY-MM-dd'T'HH:mm:ss.SSS z"),
        1633376098332L);
    Assert.assertEquals(
        DateTimeFunctions.fromDateTime("2011", "xxxx"), 1294012800000L); // strange case, inverse operation is not exact
    Assert.assertEquals(DateTimeFunctions.fromDateTime("2012", "yyyy"), 1325376000000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("2012", "YYYY"), 1325376000000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("20 AD", "CC G"), 946684800000L);
    Assert.assertEquals(
        DateTimeFunctions.fromDateTime("day 277 is Mon", "'day 'D' is 'E"), 970444800000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("7 Sunday", "e EEEEE"), 946771200000L);
    Assert.assertEquals(
        DateTimeFunctions.fromDateTime("10 Oct October", "MM MMM MMMM"), 970358400000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("06 pm", "hh a"), 64800000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("00 12", "KK hh"), 0L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("40 40", "w ww"), 970444800000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("52", "w"), 977702400000L);
    Assert.assertEquals(DateTimeFunctions.fromDateTime("1266 BC", "Y G"), -102086784000000L);
  }

  @Test
  public void testRound() {
    Assert.assertEquals(DateTimeFunctions.round(1643673599999L, 1000), 1643673599000L);
    Assert.assertEquals(DateTimeFunctions.round(1643673599999L, 550000), 1643673350000L);
  }

  @Test
  public void testNow() {
    long testClock = 1633034418051L;
    DateTimeFunctions.setClock(Clock.fixed(Instant.ofEpochMilli(testClock), ZoneOffset.UTC));

    Assert.assertEquals(DateTimeFunctions.now(), testClock);
  }

  @Test
  public void testAgo() {
    long testClock = 1633034418051L;
    DateTimeFunctions.setClock(Clock.fixed(Instant.ofEpochMilli(testClock), ZoneOffset.UTC));

    Assert.assertEquals(DateTimeFunctions.ago("PT20.345S"), testClock - 20_345);
    Assert.assertEquals(DateTimeFunctions.ago("PT15M"), testClock - 15 * 60 * 1000);
    Assert.assertEquals(DateTimeFunctions.ago("PT10H"), testClock - 10 * 60 * 60 * 1000);
    Assert.assertEquals(DateTimeFunctions.ago("P2D"), testClock - 2 * 24 * 60 * 60 * 1000);
    Assert.assertEquals(
        DateTimeFunctions.ago("P2DT3H4M"),
        testClock - 2 * 24 * 60 * 60 * 1000 - 3 * 60 * 60 * 1000 - 4 * 60 * 1000);
    // These cases are invalid even though Javadocs state them as examples
    //    Assert.assertEquals(DateTimeFunctions.ago("P-6H3M"), "-6 hours and +3 minutes");
    //    Assert.assertEquals(DateTimeFunctions.ago("-P6H3M" ), "-6 hours and -3 minutes");
    //    Assert.assertEquals(DateTimeFunctions.ago("-P-6H+3M"), "+6 hours and -3 minutes");
  }

  @Test
  public void testTimezoneHour() {
    Assert.assertEquals(DateTimeFunctions.timezoneHour("GMT"), 0);
    Assert.assertEquals(DateTimeFunctions.timezoneHour("Europe/Lisbon"), 0);
    Assert.assertEquals(DateTimeFunctions.timezoneHour("Australia/North"), 9);
    Assert.assertEquals(DateTimeFunctions.timezoneHour("Asia/Katmandu"), 5);
  }

  @Test
  public void testTimezoneMinute() {
    Assert.assertEquals(DateTimeFunctions.timezoneMinute("GMT"), 0);
    Assert.assertEquals(DateTimeFunctions.timezoneMinute("Europe/Paris"), 0);
    Assert.assertEquals(DateTimeFunctions.timezoneMinute("Australia/North"), 30);
    Assert.assertEquals(DateTimeFunctions.timezoneMinute("Asia/Katmandu"), 45);
  }

  @Test
  public void testYear() {
    Assert.assertEquals(DateTimeFunctions.year(0L), 1970);
    Assert.assertEquals(DateTimeFunctions.year(938726142000L), 1999);
    Assert.assertEquals(DateTimeFunctions.year(1633036573472L), 2021);
    Assert.assertEquals(DateTimeFunctions.year(6114086142321L), 2163);

    Assert.assertEquals(DateTimeFunctions.year(0L, "Europe/Skopje"), 1970);
    Assert.assertEquals(
        DateTimeFunctions.year(1325376000000L, "GMT"), 2012); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.year(1640991600000L, "GMT"), 2021);
    Assert.assertEquals(DateTimeFunctions.year(1640991600000L, "Asia/Tel_Aviv"), 2022);
    Assert.assertEquals(DateTimeFunctions.year(1640999700000L, "GMT"), 2022);
    Assert.assertEquals(DateTimeFunctions.year(1640999700000L, "US/Eastern"), 2021);
  }

  @Test
  public void testYearOfWeek() {
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(0L), 1970);
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(1641158173000L), 2021); // 2022-01-02 21:16:13
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(1641244573000L), 2022); // 2022-01-03 21:16:13

    Assert.assertEquals(DateTimeFunctions.yearOfWeek(0L, "Europe/Skopje"), 1970);
    Assert.assertEquals(
        DateTimeFunctions.yearOfWeek(1325376000000L, "GMT"), 2011); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(1640991600000L, "GMT"), 2021);
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(1640991600000L, "Asia/Tel_Aviv"), 2021);
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(1640999700000L, "GMT"), 2021);
    Assert.assertEquals(DateTimeFunctions.yearOfWeek(1640999700000L, "US/Eastern"), 2021);
  }

  @Test
  public void testYow() {
    Assert.assertEquals(DateTimeFunctions.yow(0L), 1970);
    Assert.assertEquals(DateTimeFunctions.yow(1641158173000L), 2021); // 2022-01-02 21:16:13
    Assert.assertEquals(DateTimeFunctions.yow(1641244573000L), 2022); // 2022-01-03 21:16:13

    Assert.assertEquals(DateTimeFunctions.yow(0L, "Europe/Skopje"), 1970);
    Assert.assertEquals(DateTimeFunctions.yow(1640991600000L, "GMT"), 2021);
    Assert.assertEquals(DateTimeFunctions.yow(1640991600000L, "Asia/Tel_Aviv"), 2021);
    Assert.assertEquals(DateTimeFunctions.yow(1640999700000L, "GMT"), 2021);
    Assert.assertEquals(DateTimeFunctions.yow(1640999700000L, "US/Eastern"), 2021);
  }

  @Test
  public void testQuarter() {
    Assert.assertEquals(DateTimeFunctions.quarter(1609459200000L), 1); // 2021-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.quarter(1617235199999L), 1); // 2021-03-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.quarter(1617235200000L), 2); // 2021-04-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.quarter(1625097599999L), 2); // 2021-06-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.quarter(1625097600000L), 3); // 2021-07-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.quarter(1633046399999L), 3); // 2021-09-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.quarter(1633046400000L), 4); // 2021-10-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.quarter(1640995199999L), 4); // 2021-12-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.quarter(1640995200000L), 1); // 2022-01-01 00:00:00.000

    Assert.assertEquals(
        DateTimeFunctions.quarter(1609459200999L, "Europe/Madrid"), 1); // 2021-01-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.quarter(1617227999999L, "Europe/Madrid"), 1); // 2021-03-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.quarter(1617228000000L, "Europe/Madrid"), 2); // 2021-04-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.quarter(1625090399999L, "Europe/Madrid"), 2); // 2021-06-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.quarter(1625090400000L, "Europe/Madrid"), 3); // 2021-07-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.quarter(1633039199999L, "Europe/Madrid"), 3); // 2021-09-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.quarter(1633039200000L, "Europe/Madrid"), 4); // 2021-10-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.quarter(1640991599999L, "Europe/Madrid"), 4); // 2021-12-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.quarter(1640991600000L, "Europe/Madrid"), 1); // 2022-01-01 00:00:00.000
  }

  @Test
  public void testMonth() {
    Assert.assertEquals(DateTimeFunctions.month(1640995200000L), 1); // 2022-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1643673599999L), 1); // 2022-01-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1643673600000L), 2); // 2022-02-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1646092799999L), 2); // 2022-02-28 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1646092800000L), 3); // 2022-03-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1648771199999L), 3); // 2022-03-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1648771200000L), 4); // 2022-04-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1651363199999L), 4); // 2022-04-30 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1651363200000L), 5); // 2022-05-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1654041599999L), 5); // 2022-05-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1654041600000L), 6); // 2022-06-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1656633599999L), 6); // 2022-06-30 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1656633600000L), 7); // 2022-07-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1659311999999L), 7); // 2022-07-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1659312000000L), 8); // 2022-08-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1661990399999L), 8); // 2022-08-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1661990400000L), 9); // 2022-09-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1664582399999L), 9); // 2022-09-30 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1664582400000L), 10); // 2022-10-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1667260799999L), 10); // 2022-10-31 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1667260800000L), 11); // 2022-11-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1669852799999L), 11); // 2022-11-30 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.month(1669852800000L), 12); // 2022-12-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.month(1672531199999L), 12); // 2022-12-31 23:59:59.999

    Assert.assertEquals(
        DateTimeFunctions.month(1640995200000L, "Europe/Rome"), 1); // 2022-01-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1643669999999L, "Europe/Rome"), 1); // 2022-01-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1643670000000L, "Europe/Rome"), 2); // 2022-02-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1646089199999L, "Europe/Rome"), 2); // 2022-02-28 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1646089200000L, "Europe/Rome"), 3); // 2022-03-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1648763999999L, "Europe/Rome"), 3); // 2022-03-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1648764000000L, "Europe/Rome"), 4); // 2022-04-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1651355999999L, "Europe/Rome"), 4); // 2022-04-30 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1651356000000L, "Europe/Rome"), 5); // 2022-05-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1654034399999L, "Europe/Rome"), 5); // 2022-05-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1654034400000L, "Europe/Rome"), 6); // 2022-06-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1656626399999L, "Europe/Rome"), 6); // 2022-06-30 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1656626400000L, "Europe/Rome"), 7); // 2022-07-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1659304799999L, "Europe/Rome"), 7); // 2022-07-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1659304800000L, "Europe/Rome"), 8); // 2022-08-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1661983199999L, "Europe/Rome"), 8); // 2022-08-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1661983200000L, "Europe/Rome"), 9); // 2022-09-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1664575199999L, "Europe/Rome"), 9); // 2022-09-30 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1664575200000L, "Europe/Rome"), 10); // 2022-10-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1667257199999L, "Europe/Rome"), 10); // 2022-10-31 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1667257200000L, "Europe/Rome"), 11); // 2022-11-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1669849199999L, "Europe/Rome"), 11); // 2022-11-30 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.month(1669849200000L, "Europe/Rome"), 12); // 2022-12-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.month(1672527599999L, "Europe/Rome"), 12); // 2022-12-31 23:59:59.999
  }

  @Test
  public void testWeek() {
    Assert.assertEquals(DateTimeFunctions.week(1325376000000L), 52); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.week(1325462400000L), 1); // 2012-01-02 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.week(1356825600000L), 52); // 2012-12-30 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.week(1356912000000L), 1); // 2012-12-31 00:00:00.000

    Assert.assertEquals(
        DateTimeFunctions.week(1325408400000L, "America/Vancouver"), 52); // 2012-01-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.week(1325491200000L, "America/Vancouver"), 1); // 2012-01-02 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.week(1356854400000L, "America/Vancouver"), 52); // 2012-12-30 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.week(1356940800000L, "America/Vancouver"), 1); // 2012-12-31 00:00:00.000
  }

  @Test
  public void testWeekOfYear() {
    Assert.assertEquals(
        DateTimeFunctions.weekOfYear(1325404800000L), 52); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.weekOfYear(1325462400000L), 1); // 2012-01-02 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.weekOfYear(1356825600000L), 52); // 2012-12-30 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.weekOfYear(1356912000000L), 1); // 2012-12-31 00:00:00.000

    Assert.assertEquals(
        DateTimeFunctions.weekOfYear(1325408400000L, "America/Vancouver"),
        52); // 2012-01-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.weekOfYear(1325491200000L, "America/Vancouver"),
        1); // 2012-01-02 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.weekOfYear(1356854400000L, "America/Vancouver"),
        52); // 2012-12-30 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.weekOfYear(1356940800000L, "America/Vancouver"),
        1); // 2012-12-31 00:00:00.000
  }

  @Test
  public void testDayOfYear() {
    Assert.assertEquals(DateTimeFunctions.dayOfYear(1325404800000L), 1); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.dayOfYear(1328054400000L), 32); // 2012-02-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.dayOfYear(1330387200000L), 59); // 2012-02-28 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.dayOfYear(1330473600000L), 60); // 2012-02-29 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfYear(1356912000000L), 366); // 2012-12-31 00:00:00.000

    Assert.assertEquals(
        DateTimeFunctions.dayOfYear(1325404800000L, "Europe/Berlin"), 1); // 2012-01-01 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfYear(1328054400000L, "Europe/Berlin"),
        32); // 2012-02-01 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfYear(1330387200000L, "Europe/Berlin"),
        59); // 2012-02-28 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfYear(1330473600000L, "Europe/Berlin"),
        60); // 2012-02-29 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfYear(1356912000000L, "Europe/Berlin"),
        366); // 2012-12-31 01:00:00.000
  }

  @Test
  public void testDoy() {
    Assert.assertEquals(DateTimeFunctions.doy(1325404800000L), 1); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.doy(1328054400000L), 32); // 2012-02-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.doy(1330387200000L), 59); // 2012-02-28 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.doy(1330473600000L), 60); // 2012-02-29 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.doy(1356912000000L), 366); // 2012-12-31 00:00:00.000

    Assert.assertEquals(
        DateTimeFunctions.doy(1325404800000L, "Europe/Berlin"), 1); // 2012-01-01 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.doy(1328054400000L, "Europe/Berlin"), 32); // 2012-02-01 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.doy(1330387200000L, "Europe/Berlin"), 59); // 2012-02-28 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.doy(1330473600000L, "Europe/Berlin"), 60); // 2012-02-29 01:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.doy(1356912000000L, "Europe/Berlin"), 366); // 2012-12-31 01:00:00.000
  }

  @Test
  public void testDay() {
    Assert.assertEquals(DateTimeFunctions.day(1325404800000L), 1); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.day(1328054400000L), 1); // 2012-02-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.day(1330387200000L), 28); // 2012-02-28 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.day(1330473600000L), 29); // 2012-02-29 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.day(1356912000000L), 31); // 2012-12-31 00:00:00.000

    Assert.assertEquals(DateTimeFunctions.day(1325404800000L, "Europe/Rome"), 1);
    Assert.assertEquals(DateTimeFunctions.day(1328054400000L, "Europe/Rome"), 1);
    Assert.assertEquals(DateTimeFunctions.day(1330387200000L, "Europe/Rome"), 28);
    Assert.assertEquals(DateTimeFunctions.day(1330473600000L, "Europe/Rome"), 29);
    Assert.assertEquals(DateTimeFunctions.day(1356912000000L, "Europe/Rome"), 31);
  }

  @Test
  public void testDayOfMonth() {
    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1325404800000L), 1); // 2012-01-01 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1328054400000L), 1); // 2012-02-01 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfMonth(1330387200000L), 28); // 2012-02-28 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfMonth(1330473600000L), 29); // 2012-02-29 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.dayOfMonth(1356912000000L), 31); // 2012-12-31 00:00:00.000

    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1325404800000L, "Europe/Moscow"), 1);
    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1328054400000L, "Europe/Moscow"), 1);
    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1330387200000L, "Europe/Moscow"), 28);
    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1330473600000L, "Europe/Moscow"), 29);
    Assert.assertEquals(DateTimeFunctions.dayOfMonth(1356912000000L, "Europe/Moscow"), 31);
  }

  @Test
  public void testDayOfWeek() {
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454371199999L), 1); // 2016-01-01 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454457599999L), 2); // 2016-01-02 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454543999999L), 3); // 2016-01-03 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454630399999L), 4); // 2016-01-04 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454716799999L), 5); // 2016-01-05 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454803199999L), 6); // 2016-01-06 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dayOfWeek(1454889599999L), 7); // 2016-01-07 23:59:59.999

    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454353199999L, "America/Jamaica"),
        1); // 2016-01-01 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454439599999L, "America/Jamaica"),
        2); // 2016-01-02 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454525999999L, "America/Jamaica"),
        3); // 2016-01-03 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454612399999L, "America/Jamaica"),
        4); // 2016-01-04 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454698799999L, "America/Jamaica"),
        5); // 2016-01-05 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454785199999L, "America/Jamaica"),
        6); // 2016-01-06 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dayOfWeek(1454871599999L, "America/Jamaica"),
        7); // 2016-01-07 23:59:59.999
  }

  @Test
  public void testDow() {
    Assert.assertEquals(DateTimeFunctions.dow(1454371199999L), 1); // 2016-01-01 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dow(1454457599999L), 2); // 2016-01-02 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dow(1454543999999L), 3); // 2016-01-03 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dow(1454630399999L), 4); // 2016-01-04 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dow(1454716799999L), 5); // 2016-01-05 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dow(1454803199999L), 6); // 2016-01-06 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dow(1454889599999L), 7); // 2016-01-07 23:59:59.999

    Assert.assertEquals(
        DateTimeFunctions.dow(1454353199999L, "America/Jamaica"), 1); // 2016-01-01 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dow(1454439599999L, "America/Jamaica"), 2); // 2016-01-02 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dow(1454525999999L, "America/Jamaica"), 3); // 2016-01-03 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dow(1454612399999L, "America/Jamaica"), 4); // 2016-01-04 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dow(1454698799999L, "America/Jamaica"), 5); // 2016-01-05 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dow(1454785199999L, "America/Jamaica"), 6); // 2016-01-06 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.dow(1454871599999L, "America/Jamaica"), 7); // 2016-01-07 23:59:59.999
  }

  @Test
  public void testHour() {
    Assert.assertEquals(DateTimeFunctions.hour(1557273599999L), 23); // 2019-05-07 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.hour(1557273600000L), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.hour(1560169800000L), 12); // 2019-06-10 12:30:00.000
    Assert.assertEquals(DateTimeFunctions.hour(1560172500000L), 13); // 2019-06-10 13:15:00.000

    Assert.assertEquals(
        DateTimeFunctions.hour(1557273599999L, "Etc/UTC"), 23); // 2019-05-07 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.hour(1557273600000L, "Etc/UTC"), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.hour(1560169800000L, "Etc/UTC"), 12); // 2019-06-10 12:30:00.000
    Assert.assertEquals(
        DateTimeFunctions.hour(1560172500000L, "Etc/UTC"), 13); // 2019-06-10 13:15:00.000
  }

  @Test
  public void testMinute() {
    Assert.assertEquals(DateTimeFunctions.minute(1557273599999L), 59); // 2019-05-07 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.minute(1557273600000L), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.minute(1559954531123L), 42); // 2019-05-08 00:42:11.123

    Assert.assertEquals(
        DateTimeFunctions.minute(1557262799999L, "Europe/Istanbul"), 59); // 2019-05-07 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.minute(1557262800000L, "Europe/Istanbul"), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.minute(1559943731123L, "Europe/Istanbul"), 42); // 2019-05-08 00:42:11.123
  }

  @Test
  public void testSecond() {
    Assert.assertEquals(DateTimeFunctions.second(1557273599999L), 59); // 2019-05-07 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.second(1557273600000L), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(DateTimeFunctions.second(1559954531123L), 11); // 2019-05-08 00:42:11.123

    Assert.assertEquals(
        DateTimeFunctions.second(1557262799999L, "Asia/Istanbul"), 59); // 2019-05-07 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.second(1557262800000L, "Asia/Istanbul"), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.second(1559943731123L, "Asia/Istanbul"), 11); // 2019-05-08 00:42:11.123
  }

  @Test
  public void testMillisecond() {
    Assert.assertEquals(
        DateTimeFunctions.millisecond(1557273599999L), 999); // 2019-05-07 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.millisecond(1557273600000L), 0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.millisecond(1559954531123L), 123); // 2019-05-08 00:42:11.123

    Assert.assertEquals(
        DateTimeFunctions.millisecond(1557262799999L, "Europe/Kirov"),
        999); // 2019-05-07 23:59:59.999
    Assert.assertEquals(
        DateTimeFunctions.millisecond(1557262800000L, "Europe/Kirov"),
        0); // 2019-05-08 00:00:00.000
    Assert.assertEquals(
        DateTimeFunctions.millisecond(1559943731123L, "Europe/Kirov"),
        123); // 2019-05-08 00:42:11.123
  }

  @Test
  public void testDateTrunc() {
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("millisecond", 1557273599999L),
        1557273599999L); // 2019-05-07 23:59:59.999
    Assert.assertEquals(DateTimeFunctions.dateTrunc("second", 1557273599999L), 1557273599000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("minute", 1557273599999L), 1557273540000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("hour", 1557273599999L), 1557270000000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("day", 1557273599999L), 1557187200000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("week", 1557273599999L), 1557100800000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("month", 1557273599999L), 1556668800000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("quarter", 1557273599999L), 1554076800000L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("year", 1557273599999L), 1546300800000L);

    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("millisecond", 15572759L, MINUTES.name()), 15572759L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("second", 15572759L, MINUTES.name()), 15572759L);
    Assert.assertEquals(DateTimeFunctions.dateTrunc("hour", 15572759L, MINUTES.name()), 15572700L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("millisecond", 1557273599L, SECONDS.name()), 1557273599L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("second", 1557273599L, SECONDS.name()), 1557273599L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("hour", 1557273599L, SECONDS.name()), 1557270000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("millisecond", 1557273599999999L, MICROSECONDS.name()),
        1557273599999000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("second", 1557273599999999L, MICROSECONDS.name()),
        1557273599000000L);

    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("millisecond", 15572759L, MINUTES.name(), "Africa/Abidjan"),
        15572759L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("second", 15572759L, MINUTES.name(), "Africa/Abidjan"),
        15572759L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("hour", 15572759L, MINUTES.name(), "Africa/Abidjan"),
        15572700L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("millisecond", 1557273599L, SECONDS.name(), "Africa/Abidjan"),
        1557273599L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("second", 1557273599L, SECONDS.name(), "Africa/Abidjan"),
        1557273599L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc("hour", 1557273599L, SECONDS.name(), "Africa/Abidjan"),
        1557270000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "millisecond", 1557273599999999L, MICROSECONDS.name(), "Africa/Abidjan"),
        1557273599999000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "second", 1557273599999999L, MICROSECONDS.name(), "Africa/Abidjan"),
        1557273599000000L);

    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "millisecond", 25572L, MINUTES.name(), "Africa/Abidjan", MILLISECONDS.name()),
        1534320000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "second", 25572L, MINUTES.name(), "Africa/Abidjan", MILLISECONDS.name()),
        1534320000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "hour", 25572L, MINUTES.name(), "Africa/Abidjan", MILLISECONDS.name()),
        1533600000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "millisecond", 1557273599L, SECONDS.name(), "Africa/Abidjan", MILLISECONDS.name()),
        1557273599000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "second", 1557273599L, SECONDS.name(), "Africa/Abidjan", MILLISECONDS.name()),
        1557273599000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "hour", 1557273599L, SECONDS.name(), "Africa/Abidjan", MILLISECONDS.name()),
        1557270000000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "millisecond",
            1557273599999999L,
            MICROSECONDS.name(),
            "Africa/Abidjan",
            NANOSECONDS.name()),
        1557273599999000000L);
    Assert.assertEquals(
        DateTimeFunctions.dateTrunc(
            "second", 1557273599999999L, MICROSECONDS.name(), "Africa/Abidjan", NANOSECONDS.name()),
        1557273599000000000L);
  }
}
